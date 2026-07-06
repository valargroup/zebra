use std::sync::Arc;

use zebra_chain::history_tree::HistoryTree;

use super::{error::*, events::*, scheduler::*, validation::*, wire::*, *};
use crate::zakura::{
    HeaderSyncServiceSummary, ServicePeerDirection, DEFAULT_LIVE_SERVICE_SUMMARY_TTL,
};

pub(super) const HEADER_SYNC_ADVISORY_BACKOFF_FAILURES: u32 = 2;
pub(super) const HEADER_SYNC_ADVISORY_BACKOFF: Duration = Duration::from_secs(60);
pub(super) const HEADER_SYNC_ADVISORY_TTL: Duration = DEFAULT_LIVE_SERVICE_SUMMARY_TTL;
pub(super) const HEADER_SYNC_STALE_LINK_FAILURES: u32 = 3;
pub(super) const HEADER_SYNC_STALE_LINK_DISTINCT_PEERS: usize = 2;
/// Production default for the dormant below-sync-start forward backfill.
///
/// When enabled (per-startup via [`HeaderSyncStartup::backfill_enabled`], which defaults to this
/// constant), a node whose trusted sync start is above genesis backfills the headers and verified
/// commitment roots below it with a second forward cursor: checkpoint-bounded finalized ranges
/// starting at genesis, root-verified against a dedicated backfill history tree, filling the
/// `commitment_roots_by_height` serving index. Restarts resume from genesis in v1 (identical
/// re-commits are idempotent and roots re-verify from the empty tree). Disabled until the node
/// wiring consumes backfilled data. See [`HeaderSyncCore::refresh_backfill_range`].
pub(super) const BELOW_SYNC_START_BACKFILL_ENABLED: bool = false;

#[derive(Clone, Debug)]
pub(super) struct HeaderSyncCore {
    pub(super) trusted_sync_start: (block::Height, block::Hash),
    pub(super) finalized_height: block::Height,
    pub(super) verified_block_tip: block::Height,
    pub(super) verified_block_hash: block::Hash,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    pub(super) best_header_parent_hash: Option<block::Hash>,
    /// History tree positioned at the parent of the next forward range.
    ///
    /// Seeded at startup from durable state and repositioned as ranges commit, so peer-supplied
    /// roots can be folded and authenticated against header commitments. The empty tree is the
    /// natural pre-Heartwood value.
    pub(super) best_header_history_tree: Arc<HistoryTree>,
    /// Highest backfilled header below the trusted sync start (its own root may be unconfirmed).
    ///
    /// The below-sync-start backfill is a second forward cursor: it starts at genesis and advances
    /// checkpoint bracket by checkpoint bracket until it reaches the sync start, then confirms the
    /// sync-start root with a final stitch range. See [`Self::refresh_backfill_range`].
    pub(super) backfill_tip: block::Height,
    pub(super) backfill_hash: block::Hash,
    pub(super) backfill_parent_hash: Option<block::Hash>,
    /// History tree positioned at the parent of the next backfill range.
    ///
    /// Seeded empty at genesis (the natural pre-Heartwood value) and repositioned as backfill
    /// ranges commit, mirroring [`Self::best_header_history_tree`] for the backfill cursor.
    pub(super) backfill_history_tree: Arc<HistoryTree>,
    /// True once the stitch range has confirmed and persisted the sync-start height's own root.
    pub(super) backfill_sync_start_root_confirmed: bool,
    pub(super) peers: HashMap<ZakuraPeerId, PeerHeaderState>,
    pub(super) parked_peers: HashSet<ZakuraPeerId>,
    pub(super) seen: HeaderHashDedup,
    pub(super) pending_new_blocks: HashSet<block::Hash>,
    pub(super) schedule: RangeScheduler,
    pub(super) pending_commits: HashMap<PendingCommitKey, PendingHeaderCommit>,
    pub(super) advisory: HashMap<ZakuraPeerId, HeaderSyncAdvisoryPeerState>,
    pub(super) stale_link: StaleLinkFailures,
    /// True while a `QueryBestHeaderHistoryTree` rebuild is outstanding, so a run of forward ranges
    /// that all find the tree stale dispatches only one reload.
    pub(super) rebuild_in_flight: bool,
}

impl HeaderSyncCore {
    pub(super) fn new(startup: &HeaderSyncStartup) -> Result<Self, HeaderSyncStartError> {
        validate_trusted_sync_start(&startup.network, startup.trusted_sync_start)?;
        let (best_header_tip, best_header_hash) = startup
            .best_header_tip
            .unwrap_or(startup.trusted_sync_start);
        let best_header_history_tree = startup.best_header_history_tree.clone();
        // v1 always reseeds the backfill cursor at genesis; a future startup read can resume it
        // from the highest contiguous backfilled frontier instead. This is the one insertion
        // point for that resume.
        let (backfill_tip, backfill_hash) = (block::Height(0), startup.network.genesis_hash());

        Ok(Self {
            trusted_sync_start: startup.trusted_sync_start,
            finalized_height: startup.frontiers.finalized_height,
            verified_block_tip: startup.frontiers.verified_block_tip,
            verified_block_hash: startup.frontiers.verified_block_hash,
            best_header_tip,
            best_header_hash,
            best_header_parent_hash: startup.best_header_parent_hash,
            best_header_history_tree,
            backfill_tip,
            backfill_hash,
            backfill_parent_hash: None,
            backfill_history_tree: Arc::new(HistoryTree::default()),
            backfill_sync_start_root_confirmed: false,
            peers: HashMap::new(),
            parked_peers: HashSet::new(),
            seen: HeaderHashDedup::default(),
            pending_new_blocks: HashSet::new(),
            schedule: RangeScheduler::new(),
            pending_commits: HashMap::new(),
            advisory: HashMap::new(),
            stale_link: StaleLinkFailures::default(),
            rebuild_in_flight: false,
        })
    }

    pub(super) fn refresh_forward_range(&mut self, startup: &HeaderSyncStartup) {
        let best_peer_tip = self
            .peers
            .values()
            .filter(|peer| peer.received_status)
            .map(|peer| peer.advertised_tip)
            .max()
            .unwrap_or(self.best_header_tip);
        if best_peer_tip <= self.best_header_tip {
            return;
        }

        let checkpoints = startup.network.checkpoint_list();
        // Commitment-root work only runs through the VCT fast-sync handoff boundary.
        // The root at `last_checkpoint` is confirmed by the next header, so root-carrying
        // ranges stop once the frontier reaches `last_checkpoint + 1`.
        let last_checkpoint = startup.last_checkpoint_height;
        let root_regime_end = next_height(last_checkpoint).unwrap_or(last_checkpoint);
        // Only persist roots for below-checkpoint heights this node forward-syncs but has
        // not committed yet.
        let root_region_floor = self.trusted_sync_start.0.max(self.finalized_height);
        let below_root_boundary =
            root_region_floor < last_checkpoint && self.best_header_tip < root_regime_end;
        let want_tree_aux_roots = below_root_boundary;

        // Root-carrying ranges redeliver the tip header so its root can be confirmed.
        let overlap_forward_range = below_root_boundary
            && self
                .best_header_parent_hash
                .is_some_and(|_| self.best_header_tip > block::Height(0));
        let Some(start) = (if overlap_forward_range {
            Some(self.best_header_tip)
        } else {
            next_height(self.best_header_tip)
        }) else {
            return;
        };
        let mut end = best_peer_tip;
        let mut finalized = false;
        if let Some(first_checkpoint) = checkpoints.min_height_in_range(block::Height(1)..) {
            if self.best_header_tip < first_checkpoint {
                if best_peer_tip < first_checkpoint {
                    return;
                }
                end = first_checkpoint;
                finalized = true;
            }
        }
        // Keep root-carrying ranges at or below the confirming header for the last checkpoint.
        if below_root_boundary {
            end = end.min(root_regime_end);
        }

        let count = count_between(start, end);
        if count == 0 {
            return;
        }
        self.schedule.ensure_forward(RangeRequest {
            start_height: start,
            count,
            link_hash: if overlap_forward_range {
                self.best_header_parent_hash
                    .expect("overlapped ranges have a parent hash")
            } else {
                self.best_header_hash
            },
            finalized,
            want_tree_aux_roots,
            priority: RangePriority::Forward,
        });
    }

    /// Schedules the next below-sync-start backfill range, if any.
    ///
    /// Backfill is a second forward cursor sweeping genesis → trusted sync start with its own
    /// history tree, reusing the forward code path: checkpoint-bounded finalized ranges with the
    /// same one-block overlap so each range's tip root is confirmed by its successor, roots
    /// verified against [`Self::backfill_history_tree`], and the confirmed prefix persisted. The
    /// sync-start height's own root has no confirming successor inside the backfill region (the
    /// forward path starts above it and never persists it), so a final non-finalized stitch range
    /// `[sync_start ..= sync_start + 1]` re-fetches the sync-start header plus its successor to
    /// confirm and persist that last root; the redelivered successor re-commits idempotently.
    pub(super) fn refresh_backfill_range(&mut self, startup: &HeaderSyncStartup) {
        if !startup.backfill_enabled {
            return;
        }
        let sync_start = self.trusted_sync_start;
        if sync_start.0 == block::Height(0) {
            return;
        }

        if self.backfill_tip < sync_start.0 {
            // Root-carrying backfill ranges redeliver the frontier header so its root can be
            // confirmed, exactly like the forward overlap.
            let overlap =
                self.backfill_parent_hash.is_some() && self.backfill_tip > block::Height(0);
            let Some(next) = next_height(self.backfill_tip) else {
                return;
            };
            let start = if overlap { self.backfill_tip } else { next };
            // End at the next checkpoint so the range end is checkpoint-authenticated; the sync
            // start is itself a checkpoint, so the cap below never produces a non-checkpoint end.
            let end = startup
                .network
                .checkpoint_list()
                .min_height_in_range(next..)
                .map_or(sync_start.0, |checkpoint| checkpoint.min(sync_start.0));
            let count = count_between(start, end);
            if count == 0 {
                return;
            }
            self.schedule.ensure_backfill(RangeRequest {
                start_height: start,
                count,
                link_hash: if overlap {
                    self.backfill_parent_hash
                        .expect("overlapped ranges have a parent hash")
                } else {
                    self.backfill_hash
                },
                finalized: true,
                want_tree_aux_roots: true,
                priority: RangePriority::Backfill,
            });
        } else if self.backfill_tip == sync_start.0 && !self.backfill_sync_start_root_confirmed {
            // Stitch: confirm the sync-start root with its successor header. Non-finalized (the
            // end is not a checkpoint); the sync-start header is authenticated in-span against
            // the trusted hash instead.
            let Some(parent) = self.backfill_parent_hash else {
                return;
            };
            if next_height(sync_start.0).is_none() {
                return;
            }
            self.schedule.ensure_backfill(RangeRequest {
                start_height: sync_start.0,
                count: 2,
                link_hash: parent,
                finalized: false,
                want_tree_aux_roots: true,
                priority: RangePriority::Backfill,
            });
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PendingHeaderCommit {
    /// The full range requested from the peer.
    ///
    /// A peer may legally return a short prefix of the requested range. Keep the
    /// requested range so success, local commit failure, and rebase cleanup can
    /// clear or retry the scheduler assignment that was created for the original
    /// `GetHeaders` request.
    pub(super) requested_range: RangeRequest,
    /// The range actually delivered by the peer and handed to the commit path.
    ///
    /// Successful commit events are reported for this delivered range, so coverage
    /// and verified frontier-tree lookup must use this range rather than the
    /// original request.
    pub(super) delivered_range: RangeRequest,
    /// `Some` for every root-carrying range — below-checkpoint forward and below-sync-start
    /// backfill alike (both persist the confirmed prefix and install their cursor's frontier
    /// tree). `None` only for plain above-checkpoint forward ranges (past the VCT handoff
    /// boundary), which request no roots and persist none.
    pub(super) verified_roots:
        Option<zebra_chain::parallel::commitment_aux_verify::VerifiedHeaderCommitmentRoots>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct StaleLinkFailures {
    pub(super) count: u32,
    pub(super) peers: HashSet<ZakuraPeerId>,
}

impl StaleLinkFailures {
    pub(super) fn record(&mut self, peer: ZakuraPeerId) {
        self.count = self.count.saturating_add(1);
        self.peers.insert(peer);
    }

    pub(super) fn should_rebase(&self) -> bool {
        self.count >= HEADER_SYNC_STALE_LINK_FAILURES
            && self.peers.len() >= HEADER_SYNC_STALE_LINK_DISTINCT_PEERS
    }

    pub(super) fn reset(&mut self) {
        self.count = 0;
        self.peers.clear();
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) struct HeaderSyncAdvisoryPeerState {
    pub(super) summary: HeaderSyncServiceSummary,
    pub(super) observed_at: Instant,
    pub(super) failure_count: u32,
    pub(super) backoff_until: Option<Instant>,
}

impl HeaderSyncAdvisoryPeerState {
    pub(super) fn new(summary: HeaderSyncServiceSummary, observed_at: Instant) -> Self {
        Self {
            summary,
            observed_at,
            failure_count: 0,
            backoff_until: None,
        }
    }

    pub(super) fn refresh_summary(
        &mut self,
        summary: HeaderSyncServiceSummary,
        observed_at: Instant,
    ) {
        self.summary = summary;
        self.observed_at = observed_at;
    }

    pub(super) fn is_expired(&self, now: Instant) -> bool {
        now.duration_since(self.observed_at) >= HEADER_SYNC_ADVISORY_TTL
    }

    pub(super) fn is_backed_off(&self, now: Instant) -> bool {
        self.backoff_until.is_some_and(|until| until > now)
    }

    pub(super) fn record_confirmed(&mut self) {
        self.failure_count = 0;
        self.backoff_until = None;
    }

    pub(super) fn record_unconfirmed(&mut self, now: Instant) {
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_count >= HEADER_SYNC_ADVISORY_BACKOFF_FAILURES {
            self.backoff_until = Some(now + HEADER_SYNC_ADVISORY_BACKOFF);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PeerHeaderState {
    pub(super) session: HeaderSyncPeerSession,
    pub(super) direction: ServicePeerDirection,
    pub(super) advertised_tip: block::Height,
    pub(super) advertised_hash: block::Hash,
    pub(super) sync_start_height: block::Height,
    pub(super) max_headers_per_response: u32,
    pub(super) max_inflight_requests: u16,
    pub(super) received_status: bool,
    /// The most recent status sent to this peer over its current session, if
    /// any. Used to suppress re-sending an identical, non-tip-advancing status,
    /// which the peer's inbound rate limiter would otherwise treat as spam.
    pub(super) last_sent_status: Option<HeaderSyncStatus>,
    pub(super) outstanding: Vec<OutstandingRange>,
    pub(super) late_covered_responses: usize,
    pub(super) meters: HeaderSyncPeerMeters,
    pub(super) served_headers_inflight: u16,
}

impl PeerHeaderState {
    pub(super) fn new(
        session: HeaderSyncPeerSession,
        trusted_sync_start: (block::Height, block::Hash),
        local_range: u32,
        local_inflight: u16,
        status_refresh_interval: Duration,
        inbound_status_min_interval: Duration,
        inbound_new_block_min_interval: Duration,
    ) -> Self {
        Self {
            direction: session.direction(),
            session,
            advertised_tip: trusted_sync_start.0,
            advertised_hash: trusted_sync_start.1,
            sync_start_height: trusted_sync_start.0,
            max_headers_per_response: clamp_advertised_range(local_range),
            max_inflight_requests: local_inflight.clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER),
            received_status: false,
            last_sent_status: None,
            outstanding: Vec::new(),
            late_covered_responses: 0,
            meters: HeaderSyncPeerMeters::new(
                status_refresh_interval,
                inbound_status_min_interval,
                inbound_new_block_min_interval,
            ),
            served_headers_inflight: 0,
        }
    }

    pub(super) fn available_slots(&self) -> usize {
        usize::from(self.max_inflight_requests)
            .min(EFFECTIVE_HS_OUTBOUND_INFLIGHT_PER_PEER)
            .saturating_sub(self.outstanding.len())
    }

    pub(super) fn pop_oldest_outstanding(&mut self) -> Option<OutstandingRange> {
        (!self.outstanding.is_empty()).then(|| self.outstanding.remove(0))
    }

    pub(super) fn restore_oldest_outstanding(&mut self, outstanding: OutstandingRange) {
        self.outstanding.insert(0, outstanding);
    }

    pub(super) fn take_late_covered_response(&mut self) -> bool {
        if self.late_covered_responses == 0 {
            return false;
        }
        self.late_covered_responses -= 1;
        true
    }

    /// Whether `status` differs from the most recent status sent to this peer
    /// over its current session. A status identical to the last one we sent is
    /// redundant — the peer cannot learn anything from it and its inbound status
    /// rate limiter would treat it as spam — so callers suppress it.
    pub(super) fn status_differs_from_last_sent(&self, status: HeaderSyncStatus) -> bool {
        self.last_sent_status != Some(status)
    }

    /// Records `status` as the most recent status sent to this peer, so a later
    /// identical status can be suppressed by [`Self::status_differs_from_last_sent`].
    pub(super) fn record_sent_status(&mut self, status: HeaderSyncStatus) {
        self.last_sent_status = Some(status);
    }

    /// Forgets the last status sent to this peer so the next one is always sent.
    /// Called when a fresh session replaces the peer's transport: the new
    /// channel's remote has received no status yet and gates serving us on it,
    /// so the initial status must go out regardless of its contents.
    pub(super) fn reset_sent_status(&mut self) {
        self.last_sent_status = None;
    }

    pub(super) fn try_start_serving_headers(&mut self, local_inflight_cap: u16) -> bool {
        if self.served_headers_inflight >= local_inflight_cap {
            return false;
        }
        self.served_headers_inflight = self.served_headers_inflight.saturating_add(1);
        true
    }

    pub(super) fn finish_serving_headers(&mut self) {
        self.served_headers_inflight = self.served_headers_inflight.saturating_sub(1);
    }
}

#[derive(Clone, Debug)]
pub(super) struct HeaderSyncPeerMeters {
    pub(super) unsolicited: RateMeter,
    pub(super) inbound_status: RateMeter,
    pub(super) inbound_new_block: RateMeter,
    /// Gates redundant keepalive status sends.
    ///
    /// Floored above the remote's inbound status minimum interval so a
    /// keepalive can never be classified as status spam even when
    /// `status_refresh_interval` is configured below that minimum, and starts
    /// one full interval out so it never lands right after the initial
    /// connect status (which may already have consumed the remote's
    /// non-advancing status token).
    pub(super) keepalive: RateMeter,
}

impl HeaderSyncPeerMeters {
    pub(super) fn new(
        status_refresh_interval: Duration,
        inbound_status_min_interval: Duration,
        inbound_new_block_min_interval: Duration,
    ) -> Self {
        let keepalive_interval =
            status_refresh_interval.max(inbound_status_min_interval.saturating_mul(2));
        let mut keepalive = RateMeter::new(keepalive_interval);
        keepalive.mark_taken(Instant::now());
        Self {
            unsolicited: RateMeter::new(status_refresh_interval),
            inbound_status: RateMeter::new(inbound_status_min_interval),
            inbound_new_block: RateMeter::new(inbound_new_block_min_interval),
            keepalive,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) struct OutstandingRange {
    pub(super) range: RangeRequest,
    pub(super) deadline: Instant,
    pub(super) expected_max_count: u32,
    pub(super) clear_assignment_on_timeout: bool,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct RangeRequest {
    pub(super) start_height: block::Height,
    pub(super) count: u32,
    pub(super) link_hash: block::Hash,
    pub(super) finalized: bool,
    pub(super) want_tree_aux_roots: bool,
    pub(super) priority: RangePriority,
}

impl RangeRequest {
    pub(super) fn end_height(self) -> block::Height {
        height_after_count(self.start_height, self.count)
            .and_then(previous_height)
            .expect("range request count is non-zero")
    }

    pub(super) fn is_within(self, start: block::Height, end: block::Height) -> bool {
        self.start_height >= start && self.end_height() <= end
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum RangePriority {
    Forward,
    Backfill,
}

impl RangePriority {
    pub(super) fn label(self) -> &'static str {
        match self {
            RangePriority::Forward => "forward",
            RangePriority::Backfill => "backfill",
        }
    }
}
