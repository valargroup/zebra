//! The `tree_aux` peer-source driver and serving port (verified-commitment-trees POC).
//!
//! Serving side: [`StateTreeAuxPort`] answers inbound `GetRoots` from local state via
//! [`ReadRequest::BlockRoots`]. Client side: [`run_tree_aux_driver`] fetches the per-block
//! commitment roots for the verified-tip→checkpoint range from peers, then publishes the
//! complete range into the committer's cache ([`TreeAuxRootsWriter`]) *ahead of* body commit.
//! The handoff frontier is embedded in the binary, so only roots travel over the wire.
//!
//! Runs when the state committer exposes a `tree_aux` roots writer. On Mainnet this
//! is the default fast path under checkpoint sync; `consensus.checkpoint_sync = false`
//! or `consensus.disable_vct_fast_sync = true` selects the legacy recompute path.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::{Duration, Instant},
};

use tower::{Service, ServiceExt};
use zebra_chain::{block, parallel::commitment_aux::BlockCommitmentRoots, parameters::Network};
use zebra_network::zakura::{
    fetch_roots_with_peer, BoxRunFuture, PeerFetchEvent, PeerFetchStatus, PeerPreference,
    TreeAuxStatePort, ZakuraPeerId, ZakuraSupervisorHandle, MAX_TA_ROOTS_PER_REQUEST,
};
use zebra_state::{BoxError, ReadRequest, ReadResponse, ReadStateService, TreeAuxRootsWriter};

use super::frontier::verified_block_tip_from_state;

/// Delay between driver attempts while waiting for a peer, or after a fetch error.
const TREE_AUX_DRIVER_RETRY: Duration = Duration::from_secs(5);

/// Number of peer-root request batches the driver may keep ahead of committed state.
const TREE_AUX_FETCH_AHEAD_BATCHES: u32 = 16;

/// Maximum speculative peer roots held in the committer cache ahead of finalized progress.
const TREE_AUX_FETCH_AHEAD_ROOTS: u32 = MAX_TA_ROOTS_PER_REQUEST * TREE_AUX_FETCH_AHEAD_BATCHES;

/// How long a peer that supplied a verification-failing root is skipped by `tree_aux`.
const TREE_AUX_HARD_FAILURE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// How long a peer with a soft fetch failure is moved behind normal peers.
const TREE_AUX_SOFT_FAILURE_DEMOTION: Duration = Duration::from_secs(60);

/// Hard failures in one decay window before the driver disconnects the Zakura peer.
///
/// Three offenses means a peer was cooled down, became selectable again, and supplied
/// bad roots repeatedly. This catches slow-drip liars without dropping the whole Zakura
/// connection for a one-off corrupt root index.
const TREE_AUX_DISCONNECT_AFTER_FAILURES: u32 = 3;

/// How long repeat hard failures count toward disconnect escalation.
///
/// Must exceed the time needed to accumulate repeat offenses through multiple cooldowns,
/// otherwise a persistent liar would decay before reaching the disconnect threshold.
const TREE_AUX_OFFENSE_DECAY: Duration = Duration::from_secs(30 * 60);

/// How long soft fetch failures keep a peer's demotion record alive.
const TREE_AUX_SOFT_FAILURE_DECAY: Duration = Duration::from_secs(5 * 60);

/// Maximum remembered peer-root provenance entries.
///
/// `TREE_AUX_FETCH_AHEAD_ROOTS + MAX_TA_ROOTS_PER_REQUEST` fits in `usize` because Zebra's
/// supported platforms have pointer widths that can represent any `u32`.
const TREE_AUX_PROVENANCE_CAPACITY: usize =
    (TREE_AUX_FETCH_AHEAD_ROOTS + MAX_TA_ROOTS_PER_REQUEST) as usize;

/// Maximum peers kept in the local hard-failure table.
const TREE_AUX_FAILURE_CAPACITY: usize = 1024;

/// Maximum peers kept in the local soft-failure table.
const TREE_AUX_SOFT_FAILURE_CAPACITY: usize = 1024;

/// Serves inbound `tree_aux` `GetRoots` from local finalized state, through the read
/// service ([`ReadRequest::BlockRoots`]). An archive/produced node serves the roots it
/// derives from its per-height trees; a fast-synced node holds no per-height trees and so
/// returns an empty (unavailable) range, which the wire layer reports as `RangeUnavailable`.
///
/// Generic over the read service so the mapping is unit-testable with a mock; production
/// uses the default [`ReadStateService`].
pub(crate) struct StateTreeAuxPort<S = ReadStateService> {
    read_state: S,
}

impl<S> StateTreeAuxPort<S> {
    pub(crate) fn new(read_state: S) -> Self {
        Self { read_state }
    }
}

impl<S> TreeAuxStatePort for StateTreeAuxPort<S>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    fn read_block_roots(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> BoxRunFuture<'static, Vec<BlockCommitmentRoots>> {
        // Clone the buffered read service before the await (the Tower clone-before-call
        // pattern); the trait read is `&self`, so each request drives its own clone.
        let mut read_state = self.read_state.clone();
        Box::pin(async move {
            let ready = match read_state.ready().await {
                Ok(ready) => ready,
                Err(error) => {
                    tracing::debug!(?error, "tree_aux serve: read service not ready");
                    return Vec::new();
                }
            };
            match ready
                .call(ReadRequest::BlockRoots {
                    start_height,
                    count,
                })
                .await
            {
                Ok(ReadResponse::BlockRoots(roots)) => roots,
                // An unavailable range (fast-synced node, pruned, or wrong response) is an
                // empty serve, never wrong data — the client treats it as unavailable.
                Ok(_) => Vec::new(),
                Err(error) => {
                    tracing::debug!(?error, "tree_aux serve: BlockRoots read failed");
                    Vec::new()
                }
            }
        })
    }
}

/// Fetch and refetch verified-tip→checkpoint per-block roots from peers into the committer cache.
///
/// Once an outbound peer is available, fetch the roots the committer still needs — the range from
/// this node's verified tip up to the checkpoint — in bounded windows tied to committed progress.
/// Each window is published to `writer` only after that window has been fetched completely. The
/// driver then stays alive for targeted root-refetch requests from the committer: a frozen-frontier
/// root miss parks the checkpoint block, asks this driver to refill that height from peers, and
/// retries the same commit without resetting the block queue.
///
/// The fetch starts at `verified_tip + 1`, not genesis: heights at or below the verified tip are
/// already committed, so their roots are never looked up. Fetching from genesis on a node that
/// starts above it (e.g. a snapshot) would spend the whole fetch on already-committed heights and
/// never cache the window roots before the committer reaches them — every block would then fall
/// back to legacy recompute.
pub(crate) async fn run_tree_aux_driver(
    supervisor: ZakuraSupervisorHandle,
    writer: TreeAuxRootsWriter,
    network: Network,
    read_state: ReadStateService,
    shutdown: impl std::future::Future<Output = ()>,
) {
    let last_checkpoint_height = network.checkpoint_list().max_height();

    // The first root the committer still needs: one above the current verified tip. Read once at
    // startup; the fetched range stays a superset of what the committer will commit even if it
    // advances meanwhile (extra cached roots below its position are harmless).
    let finalized_tip = read_state_tip(&read_state, ReadRequest::FinalizedTip).await;
    let tip = read_state_tip(&read_state, ReadRequest::Tip).await;
    let from = root_fetch_start(finalized_tip, tip, &network);

    let mut refetch_rx = Some(writer.subscribe_refetch());

    let driver = async {
        let mut initial_fetch_complete = false;
        let mut next_fetch = from;
        let mut peer_policy = TreeAuxPeerPolicy::default();

        loop {
            // Wait for an outbound peer before issuing requests (fetch_roots needs one).
            if supervisor.outbound_peer_handles().await.is_empty() {
                tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                continue;
            }

            if !initial_fetch_complete {
                // We reached the last checkpoint, roots aren't needed anymore
                if next_fetch > last_checkpoint_height {
                    tracing::info!(
                        from_height = from.0,
                        handoff_height = last_checkpoint_height.0,
                        "tree_aux: fetched verified-tip→checkpoint roots from peer into the committer cache"
                    );
                    initial_fetch_complete = true;
                    continue;
                }

                let committed_through = writer.committed_through();
                if let Some(committed_height) = committed_through {
                    if next_fetch <= committed_height {
                        let Some(next_height) = committed_height.0.checked_add(1) else {
                            initial_fetch_complete = true;
                            continue;
                        };
                        next_fetch = block::Height(next_height);
                        if next_fetch > last_checkpoint_height {
                            continue;
                        }
                    }
                }

                let Some((window_from, window_to)) = next_fetch_window(
                    from,
                    next_fetch,
                    last_checkpoint_height,
                    committed_through,
                    TREE_AUX_FETCH_AHEAD_ROOTS,
                ) else {
                    if let Some(rx) = &mut refetch_rx {
                        let mut refetch_closed = false;
                        tokio::select! {
                            _ = tokio::time::sleep(TREE_AUX_DRIVER_RETRY) => {}
                            request = rx.recv() => {
                                refetch_closed = handle_refetch_request(
                                    request,
                                    &supervisor,
                                    &writer,
                                    &mut peer_policy,
                                )
                                    .await
                                    .is_err();
                            }
                        }
                        if refetch_closed {
                            refetch_rx = None;
                        }
                    } else {
                        tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                    }
                    continue;
                };

                let result = fetch_roots_into_writer(
                    &supervisor,
                    &writer,
                    &mut peer_policy,
                    window_from,
                    window_to,
                )
                .await;

                match result {
                    Ok(()) => {
                        if let Some(rx) = &mut refetch_rx {
                            let processed = process_queued_refetch_requests(
                                rx,
                                &supervisor,
                                &writer,
                                &mut peer_policy,
                            )
                            .await;
                            if processed > 0 {
                                tracing::debug!(
                                    processed,
                                    from_height = window_from.0,
                                    to_height = window_to.0,
                                    "tree_aux: processed queued refetch requests after initial root-window fetch"
                                );
                            }
                        }
                        tracing::debug!(
                            from_height = window_from.0,
                            to_height = window_to.0,
                            committed_through = ?writer.committed_through(),
                            "tree_aux: fetched bounded peer-root window into the committer cache"
                        );
                        if window_to >= last_checkpoint_height {
                            initial_fetch_complete = true;
                            tracing::info!(
                                from_height = from.0,
                                handoff_height = last_checkpoint_height.0,
                                "tree_aux: fetched verified-tip→checkpoint roots from peer into the committer cache"
                            );
                        } else {
                            next_fetch = block::Height(
                                window_to
                                    .0
                                    .checked_add(1)
                                    .expect("window end is below handoff, so next height exists"),
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(?error, "tree_aux: root fetch failed, retrying");
                        tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                    }
                }

                continue;
            }

            let Some(rx) = &mut refetch_rx else {
                tokio::time::sleep(TREE_AUX_DRIVER_RETRY).await;
                continue;
            };

            if handle_refetch_request(rx.recv().await, &supervisor, &writer, &mut peer_policy)
                .await
                .is_err()
            {
                break;
            }
        }
    };

    tokio::pin!(shutdown);
    tokio::select! {
        _ = driver => {}
        _ = &mut shutdown => {
            tracing::info!("tree_aux driver shutting down");
        }
    }
}

/// Return the next bounded fetch window, if committed progress has opened one.
fn next_fetch_window(
    from: block::Height,
    next_fetch: block::Height,
    last_checkpoint_height: block::Height,
    committed_through: Option<block::Height>,
    fetch_ahead_roots: u32,
) -> Option<(block::Height, block::Height)> {
    if next_fetch > last_checkpoint_height {
        return None;
    }

    let window_end = fetch_window_end(from, last_checkpoint_height, committed_through, fetch_ahead_roots);
    (next_fetch <= window_end).then_some((next_fetch, window_end))
}

/// Highest root the driver may fetch while staying within the fetch-ahead cap.
fn fetch_window_end(
    from: block::Height,
    last_checkpoint_height: block::Height,
    committed_through: Option<block::Height>,
    fetch_ahead_roots: u32,
) -> block::Height {
    let committed_floor = committed_through
        .map(|height| height.0)
        .unwrap_or_else(|| from.0.saturating_sub(1));

    block::Height(
        committed_floor
            .saturating_add(fetch_ahead_roots)
            .min(last_checkpoint_height.0),
    )
}

/// Fetch `[from, to]` roots and insert the complete range into the committer's peer cache.
///
/// This is the write side of the `tree_aux` source: `fetch_roots` validates the transport
/// response shape while staging each batch locally. Only a full-range success makes those roots
/// visible to the finalized committer. The roots are still untrusted here; the committer verifies
/// them against checkpoint headers.
async fn fetch_roots_into_writer(
    supervisor: &ZakuraSupervisorHandle,
    writer: &TreeAuxRootsWriter,
    peer_policy: &mut TreeAuxPeerPolicy,
    from: block::Height,
    to: block::Height,
) -> Result<(), BoxError> {
    let mut staged = Vec::new();
    let mut provenance = Vec::new();
    peer_policy.prune_committed(writer.committed_through());
    // The fetch API needs two synchronous callbacks with mutable policy access. This lock is
    // local and uncontended; it only keeps the spawned driver future `Send`.
    let peer_policy = Mutex::new(peer_policy);
    // Keep partial batches private: if the fetch errors or this future is cancelled by
    // `select!`, the staged prefix drops here without reaching `PeerSource`.
    let result = fetch_roots_with_peer(
        supervisor,
        from,
        to,
        |peer_id| {
            peer_policy
                .lock()
                .expect("tree_aux peer policy mutex is not poisoned")
                .peer_preference(peer_id, Instant::now())
        },
        |event| {
            peer_policy
                .lock()
                .expect("tree_aux peer policy mutex is not poisoned")
                .record_fetch_event(event, Instant::now());
        },
        |batch| {
            provenance.extend(
                batch
                    .roots
                    .iter()
                    .map(|root| (root.height, batch.peer_id.clone())),
            );
            staged.extend(batch.roots);
        },
    )
    .await;
    insert_staged_roots_after_success(staged, provenance, result, |roots, provenance| {
        peer_policy
            .lock()
            .expect("tree_aux peer policy mutex is not poisoned")
            .record_inserted_roots(provenance, writer.committed_through(), Instant::now());
        writer.insert_roots(roots);
    })
}

/// Publish staged roots and their provenance only if the whole requested range succeeded.
///
/// The caller records provenance before exposing roots to `TreeAuxRootsWriter`, so a fast
/// committer cannot observe a root before the driver knows which peer supplied it.
fn insert_staged_roots_after_success<F>(
    staged: Vec<BlockCommitmentRoots>,
    provenance: Vec<(block::Height, ZakuraPeerId)>,
    result: Result<(), BoxError>,
    insert_roots: F,
) -> Result<(), BoxError>
where
    F: FnOnce(Vec<BlockCommitmentRoots>, Vec<(block::Height, ZakuraPeerId)>),
{
    result?;
    insert_roots(staged, provenance);
    Ok(())
}

/// Handle one targeted root-refetch request from the finalized committer.
///
/// A concrete height requests an immediate one-root fetch into `writer`. Lagged requests are
/// recoverable because later retry iterations can send the height again; a closed channel means
/// the peer-source committer is gone, so the driver can stop waiting for refetches.
async fn handle_refetch_request(
    request: Result<block::Height, tokio::sync::broadcast::error::RecvError>,
    supervisor: &ZakuraSupervisorHandle,
    writer: &TreeAuxRootsWriter,
    peer_policy: &mut TreeAuxPeerPolicy,
) -> Result<(), ()> {
    match request {
        Ok(height) => {
            let now = Instant::now();
            if let Some(rejected) = peer_policy.mark_rejected_supplier(height, now) {
                writer.invalidate_roots(rejected.heights.iter().copied());
                // `tree_aux` roots are verified by state after fetch, outside Zakura's
                // block-sync scoring path. Keep a stream-local cooldown for selection,
                // then escalate to a whole-peer disconnect only after repeat offenses.
                metrics::counter!("tree_aux.peer.hard_failure.count").increment(1);
                if rejected.should_disconnect {
                    let disconnected = supervisor.disconnect_peer(&rejected.peer_id).await;
                    metrics::counter!("tree_aux.peer.disconnect.count").increment(1);
                    tracing::warn!(
                        ?height,
                        peer_id = ?rejected.peer_id,
                        evicted_roots = rejected.heights.len(),
                        offenses = rejected.offenses,
                        disconnected,
                        "tree_aux: disconnecting repeat-offender root supplier"
                    );
                } else {
                    tracing::warn!(
                        ?height,
                        peer_id = ?rejected.peer_id,
                        evicted_roots = rejected.heights.len(),
                        offenses = rejected.offenses,
                        "tree_aux: cooling down root supplier after verification failure"
                    );
                }
            }

            let result =
                fetch_roots_into_writer(supervisor, writer, peer_policy, height, height).await;
            match result {
                Ok(()) => tracing::info!(
                    ?height,
                    "tree_aux: fetched retryable missing root into the committer cache"
                ),
                Err(error) => tracing::warn!(
                    ?height,
                    ?error,
                    "tree_aux: retryable missing root fetch failed"
                ),
            }
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
            tracing::warn!(
                skipped,
                "tree_aux: missed root refetch requests, continuing with latest requests"
            );
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => Err(()),
    }
}

async fn process_queued_refetch_requests(
    rx: &mut tokio::sync::broadcast::Receiver<block::Height>,
    supervisor: &ZakuraSupervisorHandle,
    writer: &TreeAuxRootsWriter,
    peer_policy: &mut TreeAuxPeerPolicy,
) -> u64 {
    let mut processed = 0u64;
    loop {
        match rx.try_recv() {
            Ok(height) => {
                let _ = handle_refetch_request(Ok(height), supervisor, writer, peer_policy).await;
                processed = processed.saturating_add(1);
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                tracing::warn!(
                    "tree_aux: missed refetch requests while draining, latest will retry"
                );
            }
            Err(
                tokio::sync::broadcast::error::TryRecvError::Empty
                | tokio::sync::broadcast::error::TryRecvError::Closed,
            ) => return processed,
        }
    }
}

#[cfg(test)]
fn drain_queued_refetch_requests_for_test(
    rx: &mut tokio::sync::broadcast::Receiver<block::Height>,
) -> u64 {
    let mut drained = 0u64;
    loop {
        match rx.try_recv() {
            Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                drained = drained.saturating_add(1);
            }
            Err(
                tokio::sync::broadcast::error::TryRecvError::Empty
                | tokio::sync::broadcast::error::TryRecvError::Closed,
            ) => return drained,
        }
    }
}

/// Testable core of [`handle_refetch_request`], with the peer fetch supplied by the caller.
#[cfg(test)]
async fn handle_refetch_request_with_fetch<F, Fut>(
    request: Result<block::Height, tokio::sync::broadcast::error::RecvError>,
    peer_policy: &mut TreeAuxPeerPolicy,
    now: Instant,
    fetch_height: F,
) -> Result<(), ()>
where
    F: FnOnce(block::Height, Vec<ZakuraPeerId>) -> Fut,
    Fut: std::future::Future<Output = Result<(), BoxError>>,
{
    match request {
        Ok(height) => {
            let _ = peer_policy.mark_rejected_supplier(height, now);
            let result = fetch_height(height, peer_policy.excluded_peers(now)).await;
            match result {
                Ok(()) => tracing::info!(
                    ?height,
                    "tree_aux: fetched retryable missing root into the committer cache"
                ),
                Err(error) => tracing::warn!(
                    ?height,
                    ?error,
                    "tree_aux: retryable missing root fetch failed"
                ),
            }
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
            tracing::warn!(
                skipped,
                "tree_aux: missed root refetch requests, continuing with latest requests"
            );
            Ok(())
        }
        Err(tokio::sync::broadcast::error::RecvError::Closed) => Err(()),
    }
}

/// Driver-owned peer provenance and exclusion state for `tree_aux` roots.
///
/// `zebra-state` deliberately stores only heights and roots. This side table lives in
/// `zebrad`, where both the state writer and Zakura peer ids are visible without adding
/// a `zebra-state` → `zebra-network` dependency.
#[derive(Debug, Default)]
struct TreeAuxPeerPolicy {
    suppliers_by_height: BTreeMap<u32, ZakuraPeerId>,
    hard_failed: HashMap<ZakuraPeerId, PeerFailure>,
    soft_failed: HashMap<ZakuraPeerId, PeerSoftFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PeerFailure {
    /// Excluded from `tree_aux` selection until this instant.
    cooldown_until: Instant,
    /// Hard failures in the current decay window.
    offenses: u32,
    /// Most recent hard-failure time; resets the streak after [`TREE_AUX_OFFENSE_DECAY`].
    last_offense: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PeerSoftFailure {
    /// Demoted behind normal peers until this instant.
    demote_until: Instant,
    /// Most recent soft-failure time; clears the record after [`TREE_AUX_SOFT_FAILURE_DECAY`].
    last_failure: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RejectedSupplier {
    peer_id: ZakuraPeerId,
    heights: Vec<block::Height>,
    offenses: u32,
    should_disconnect: bool,
}

impl TreeAuxPeerPolicy {
    /// Return the selection preference for `peer_id` under the current peer policy.
    ///
    /// Hard failures exclude a peer from `tree_aux` selection. Soft failures only move a peer
    /// behind normal peers, so they remain available if every normal peer is also unusable.
    fn peer_preference(&mut self, peer_id: &ZakuraPeerId, now: Instant) -> PeerPreference {
        if self.is_excluded(peer_id, now) {
            return PeerPreference::Excluded;
        }

        self.prune_soft_failure_records(now);
        if self
            .soft_failed
            .get(peer_id)
            .is_some_and(|failure| now < failure.demote_until)
        {
            PeerPreference::Demoted
        } else {
            PeerPreference::Normal
        }
    }

    /// Returns true when `peer_id` is in a live hard-failure cooldown.
    ///
    /// Expired offense records are pruned before the check, but cooldown-expired records are
    /// retained until the longer offense-decay window elapses so repeat failures can escalate.
    fn is_excluded(&mut self, peer_id: &ZakuraPeerId, now: Instant) -> bool {
        self.prune_offense_records(now);
        self.hard_failed
            .get(peer_id)
            .is_some_and(|failure| now < failure.cooldown_until)
    }

    /// Return the currently excluded peers for test-injected refetches.
    ///
    /// Production fetches call [`Self::is_excluded`] lazily for each peer handle; tests use
    /// this snapshot to assert a height-only refetch turned into the expected peer exclusion.
    #[cfg(test)]
    fn excluded_peers(&mut self, now: Instant) -> Vec<ZakuraPeerId> {
        self.prune_offense_records(now);
        self.hard_failed
            .iter()
            .filter_map(|(peer_id, failure)| (now < failure.cooldown_until).then_some(peer_id))
            .cloned()
            .collect()
    }

    /// Record the outcome of one peer request.
    fn record_fetch_event(&mut self, event: PeerFetchEvent, now: Instant) {
        match event.status {
            PeerFetchStatus::Success => self.clear_soft_failure(&event.peer_id, now),
            PeerFetchStatus::SoftFailure { error } => {
                metrics::counter!("tree_aux.peer.soft_failure.count").increment(1);
                tracing::debug!(
                    peer_id = ?event.peer_id,
                    height = ?event.height,
                    event.count,
                    error,
                    "tree_aux: demoting peer after soft fetch failure"
                );
                self.record_soft_failure(event.peer_id, now);
            }
        }
    }

    /// Move a soft-failing peer behind normal peers for a short window.
    fn record_soft_failure(&mut self, peer_id: ZakuraPeerId, now: Instant) {
        self.prune_soft_failure_records(now);
        if self.is_excluded(&peer_id, now) {
            return;
        }

        let failure = self.soft_failed.entry(peer_id).or_insert(PeerSoftFailure {
            demote_until: now,
            last_failure: now,
        });
        failure.last_failure = now;
        failure.demote_until = now
            .checked_add(TREE_AUX_SOFT_FAILURE_DEMOTION)
            .expect("tree_aux soft demotion duration is representable");
        self.bound_soft_failures();
        self.set_active_soft_demotion_gauge(now);
    }

    fn clear_soft_failure(&mut self, peer_id: &ZakuraPeerId, now: Instant) {
        self.soft_failed.remove(peer_id);
        self.set_active_soft_demotion_gauge(now);
    }

    /// Record peer provenance for roots that are about to be inserted into the state cache.
    ///
    /// This must be called only after the full requested range succeeds and the roots are
    /// about to be published to `TreeAuxRootsWriter`; otherwise a failed staged prefix
    /// could falsely blame a peer for roots the committer never observed. Entries at
    /// already-committed heights are ignored, and the oldest remaining heights are dropped
    /// when the bounded side table reaches [`TREE_AUX_PROVENANCE_CAPACITY`].
    fn record_inserted_roots(
        &mut self,
        provenance: impl IntoIterator<Item = (block::Height, ZakuraPeerId)>,
        committed_through: Option<block::Height>,
        now: Instant,
    ) {
        self.prune_committed(committed_through);

        for (height, peer_id) in provenance {
            if committed_through.is_some_and(|committed| height <= committed) {
                continue;
            }
            self.suppliers_by_height.insert(height.0, peer_id);
        }

        while self.suppliers_by_height.len() > TREE_AUX_PROVENANCE_CAPACITY {
            self.suppliers_by_height.pop_first();
        }
        self.set_active_cooldown_gauge(now);
    }

    /// Move the last supplier for `height` into the hard-failure table.
    ///
    /// A state refetch request is height-only by crate-boundary design. If this side table
    /// remembers who supplied that height, the driver treats the supplier as the peer whose
    /// root just failed verification. All still-cached heights from that same supplier are
    /// returned so the state cache can bulk-evict the poisoned window instead of grinding
    /// through one rejected height per commit attempt. A peer is disconnected only after
    /// repeat offenses in the decay window; a first offense is cooldown-only.
    // The `expect` below is on `Instant + minute-scale Duration`, which is always representable;
    // it is an invariant assertion, not a fallible result this `Option` fn should propagate.
    #[allow(clippy::unwrap_in_result)]
    fn mark_rejected_supplier(
        &mut self,
        height: block::Height,
        now: Instant,
    ) -> Option<RejectedSupplier> {
        self.prune_offense_records(now);
        let peer_id = self.suppliers_by_height.get(&height.0)?.clone();
        let heights = self.remove_supplier_heights(&peer_id);
        self.soft_failed.remove(&peer_id);
        let failure = self
            .hard_failed
            .entry(peer_id.clone())
            .or_insert(PeerFailure {
                cooldown_until: now,
                offenses: 0,
                last_offense: now,
            });
        failure.offenses = failure.offenses.saturating_add(1);
        failure.last_offense = now;
        failure.cooldown_until = now
            .checked_add(TREE_AUX_HARD_FAILURE_COOLDOWN)
            .expect("tree_aux cooldown duration is representable");
        let offenses = failure.offenses;
        let should_disconnect = offenses >= TREE_AUX_DISCONNECT_AFTER_FAILURES;
        self.bound_cooldowns();
        self.set_active_cooldown_gauge(now);
        self.set_active_soft_demotion_gauge(now);
        Some(RejectedSupplier {
            peer_id,
            heights,
            offenses,
            should_disconnect,
        })
    }

    /// Drop provenance at or below the committed-root eviction watermark.
    ///
    /// Once state has committed and evicted a root, a later refetch for that height is stale
    /// from this driver's perspective and must not downscore the old supplier.
    fn prune_committed(&mut self, committed_through: Option<block::Height>) {
        let Some(committed_through) = committed_through else {
            return;
        };

        if let Some(first_uncommitted) = committed_through.0.checked_add(1) {
            let retained = self.suppliers_by_height.split_off(&first_uncommitted);
            self.suppliers_by_height = retained;
        } else {
            self.suppliers_by_height.clear();
        }
    }

    /// Remove hard-failure records whose offense streak fully decayed.
    fn prune_offense_records(&mut self, now: Instant) {
        self.hard_failed.retain(|_peer_id, failure| {
            failure
                .last_offense
                .checked_add(TREE_AUX_OFFENSE_DECAY)
                .is_some_and(|expires_at| now < expires_at)
        });
        self.set_active_cooldown_gauge(now);
    }

    /// Remove soft-failure records whose streak fully decayed.
    fn prune_soft_failure_records(&mut self, now: Instant) {
        self.soft_failed.retain(|_peer_id, failure| {
            failure
                .last_failure
                .checked_add(TREE_AUX_SOFT_FAILURE_DECAY)
                .is_some_and(|expires_at| now < expires_at)
        });
        self.set_active_soft_demotion_gauge(now);
    }

    /// Keep the failure table bounded by dropping records closest to full decay.
    fn bound_cooldowns(&mut self) {
        if self.hard_failed.len() <= TREE_AUX_FAILURE_CAPACITY {
            return;
        }

        let mut expirations: Vec<_> = self
            .hard_failed
            .iter()
            .map(|(peer_id, failure)| {
                (
                    peer_id.clone(),
                    failure
                        .last_offense
                        .checked_add(TREE_AUX_OFFENSE_DECAY)
                        .expect("tree_aux offense decay duration is representable"),
                )
            })
            .collect();
        expirations.sort_by_key(|(_peer_id, expires_at)| *expires_at);
        for (peer_id, _) in expirations
            .into_iter()
            .take(self.hard_failed.len() - TREE_AUX_FAILURE_CAPACITY)
        {
            self.hard_failed.remove(&peer_id);
        }
    }

    /// Keep the soft-failure table bounded by dropping records closest to full decay.
    fn bound_soft_failures(&mut self) {
        if self.soft_failed.len() <= TREE_AUX_SOFT_FAILURE_CAPACITY {
            return;
        }

        let mut expirations: Vec<_> = self
            .soft_failed
            .iter()
            .map(|(peer_id, failure)| {
                (
                    peer_id.clone(),
                    failure
                        .last_failure
                        .checked_add(TREE_AUX_SOFT_FAILURE_DECAY)
                        .expect("tree_aux soft-failure decay duration is representable"),
                )
            })
            .collect();
        expirations.sort_by_key(|(_peer_id, expires_at)| *expires_at);
        for (peer_id, _) in expirations
            .into_iter()
            .take(self.soft_failed.len() - TREE_AUX_SOFT_FAILURE_CAPACITY)
        {
            self.soft_failed.remove(&peer_id);
        }
    }

    fn remove_supplier_heights(&mut self, peer_id: &ZakuraPeerId) -> Vec<block::Height> {
        let heights: Vec<_> = self
            .suppliers_by_height
            .iter()
            .filter_map(|(height, supplier)| {
                (supplier == peer_id).then_some(block::Height(*height))
            })
            .collect();

        for height in &heights {
            self.suppliers_by_height.remove(&height.0);
        }

        heights
    }

    fn set_active_cooldown_gauge(&self, now: Instant) {
        let active = self
            .hard_failed
            .values()
            .filter(|failure| now < failure.cooldown_until)
            .count();
        // The failure table is bounded by `TREE_AUX_FAILURE_CAPACITY`, far below f64's
        // exact integer range.
        metrics::gauge!("tree_aux.peer.cooldown.active").set(active as f64);
    }

    fn set_active_soft_demotion_gauge(&self, now: Instant) {
        let active = self
            .soft_failed
            .values()
            .filter(|failure| now < failure.demote_until)
            .count();
        // The soft-failure table is bounded by `TREE_AUX_SOFT_FAILURE_CAPACITY`, far below
        // f64's exact integer range.
        metrics::gauge!("tree_aux.peer.soft_demoted.active").set(active as f64);
    }
}

/// The first height the committer still needs roots for: one above the node's verified tip
/// (`max(finalized_tip, best_tip)`). Heights at or below the verified tip are already committed, so
/// their roots are never looked up — re-fetching them is wasted and, on a node that starts well
/// above genesis, starves the roots it actually needs. Returns `Height(1)` only for a genesis-empty
/// node, i.e. "fetch from genesis" falls out exactly when the node really is at genesis.
fn root_fetch_start(
    finalized_tip: Option<(block::Height, block::Hash)>,
    tip: Option<(block::Height, block::Hash)>,
    network: &Network,
) -> block::Height {
    let empty_state_tip = (block::Height(0), network.genesis_hash());
    let (verified_tip, _) = verified_block_tip_from_state(finalized_tip, tip, empty_state_tip);
    block::Height(verified_tip.0.saturating_add(1))
}

/// Read a finalized/verified tip height+hash via a one-shot state read, returning `None` on any
/// error or unexpected response (the caller then treats the tip as the empty-state genesis).
async fn read_state_tip(
    read_state: &ReadStateService,
    request: ReadRequest,
) -> Option<(block::Height, block::Hash)> {
    match read_state.clone().oneshot(request).await {
        Ok(ReadResponse::FinalizedTip(tip)) | Ok(ReadResponse::Tip(tip)) => tip,
        Ok(response) => {
            tracing::warn!(?response, "tree_aux: unexpected tip response");
            None
        }
        Err(error) => {
            tracing::warn!(?error, "tree_aux: failed to read tip for root-fetch start");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tower::service_fn;
    use zebra_chain::{orchard, parameters::Network, sapling};
    use zebra_network::zakura::{testkit::ZakuraTestNode, TreeAuxService};
    use zebra_state::init_test_services_with_tree_aux_writer;

    use super::*;

    fn root_at(height: u32) -> BlockCommitmentRoots {
        BlockCommitmentRoots {
            height: block::Height(height),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        }
    }

    fn peer_id(seed: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![seed; 32]).expect("test peer id is within bounds")
    }

    fn add_duration(instant: Instant, duration: Duration) -> Instant {
        instant
            .checked_add(duration)
            .expect("test instant arithmetic stays in range")
    }

    async fn tree_aux_test_node(
        seed: u64,
        roots: Vec<BlockCommitmentRoots>,
    ) -> Result<ZakuraTestNode, BoxError> {
        tree_aux_test_node_with_counter(seed, roots, None).await
    }

    async fn tree_aux_test_node_with_counter(
        seed: u64,
        roots: Vec<BlockCommitmentRoots>,
        requests: Option<Arc<AtomicUsize>>,
    ) -> Result<ZakuraTestNode, BoxError> {
        let read_state = service_fn(move |request: ReadRequest| {
            let roots = roots.clone();
            let requests = requests.clone();
            future::ready(match request {
                ReadRequest::BlockRoots { .. } => {
                    if let Some(requests) = requests {
                        requests.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(ReadResponse::BlockRoots(roots))
                }
                other => Err(format!("unexpected request: {other:?}").into()),
            })
        });

        ZakuraTestNode::builder(seed)
            .max_connections_per_ip(16)
            .service(Arc::new(TreeAuxService::new(Arc::new(
                StateTreeAuxPort::new(read_state),
            ))))
            .spawn()
            .await
    }

    async fn wait_for_outbound_peer_count(
        node: &ZakuraTestNode,
        expected_count: usize,
    ) -> Result<(), BoxError> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if node.supervisor().outbound_peer_handles().await.len() >= expected_count {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "timed out waiting for outbound tree_aux peers".into() })
    }

    async fn wait_for_peer_disconnect(
        node: &ZakuraTestNode,
        peer_id: &ZakuraPeerId,
    ) -> Result<(), BoxError> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if !node.supervisor().registered_ids().await.contains(peer_id) {
                    return;
                }

                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError {
            "timed out waiting for rejected tree_aux peer disconnect".into()
        })
    }

    #[tokio::test]
    async fn port_returns_block_roots_from_the_read_service() {
        let held: Vec<_> = (5..8).map(root_at).collect();
        let served = held.clone();
        let read_state = service_fn(move |request: ReadRequest| {
            let served = served.clone();
            future::ready(match request {
                ReadRequest::BlockRoots { .. } => Ok(ReadResponse::BlockRoots(served)),
                other => Err(format!("unexpected request: {other:?}").into()),
            })
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert_eq!(
            roots, held,
            "the port returns the roots the read service serves"
        );
    }

    #[tokio::test]
    async fn port_maps_read_errors_to_an_empty_serve() {
        // A read error must degrade to an empty (unavailable) serve, never a panic or
        // wrong data — the client treats an empty range as unavailable.
        let read_state = service_fn(|_request: ReadRequest| {
            future::ready(Err::<ReadResponse, BoxError>("read failed".into()))
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert!(roots.is_empty(), "a failed read serves an empty range");
    }

    #[tokio::test]
    async fn port_maps_an_unexpected_response_to_an_empty_serve() {
        let read_state = service_fn(|_request: ReadRequest| {
            future::ready(Ok::<_, BoxError>(ReadResponse::ValidBlockProposal))
        });

        let port = StateTreeAuxPort::new(read_state);
        let roots = port.read_block_roots(block::Height(5), 3).await;

        assert!(
            roots.is_empty(),
            "a non-BlockRoots response serves an empty range, not wrong data"
        );
    }

    /// End-to-end serving integration: a real finalized state serves per-block roots
    /// through the production [`StateTreeAuxPort`] → `TreeAuxService` over the real
    /// loopback Zakura transport, and a peer's [`fetch_roots`] receives exactly what the
    /// state serves (`ReadRequest::BlockRoots`). This joins the serving stack and the
    /// wire on real state — the piece the in-crate unit tests (mock port / committer
    /// PeerSource) cannot cover. The committer consumption of these roots is covered by
    /// `vct_peer_source_filled_incrementally_drives_byte_identical_state` in `zebra-state`.
    #[tokio::test]
    async fn tree_aux_serves_real_state_roots_over_the_wire() -> Result<(), BoxError> {
        use tower::ServiceExt;
        use zebra_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};
        use zebra_network::zakura::fetch_roots;
        use zebra_state::{populated_state, ReadResponse};

        let _guard = zebra_test::init();
        let network = Network::Mainnet;
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

        // A real finalized state with per-height trees: a contiguous mainnet prefix
        // (genesis onward) committed as checkpoint-verified blocks.
        let blocks: Vec<std::sync::Arc<Block>> = zebra_test::vectors::CONTINUOUS_MAINNET_BLOCKS
            .values()
            .map(|bytes| {
                std::sync::Arc::new(
                    bytes
                        .zcash_deserialize_into::<Block>()
                        .expect("test vector block deserializes"),
                )
            })
            .collect();
        let tip = (blocks.len() - 1) as u32;
        assert!(tip >= 2, "need a few blocks to serve a range");
        let (_state, read_state, _latest_tip, _change) =
            populated_state(blocks.clone(), &network).await;

        // The roots the state serves directly for [1, tip] — the expected truth.
        let count = tip;
        let ReadResponse::BlockRoots(expected) = read_state
            .clone()
            .oneshot(ReadRequest::BlockRoots {
                start_height: block::Height(1),
                count,
            })
            .await?
        else {
            panic!("expected a BlockRoots response");
        };
        assert!(
            !expected.is_empty(),
            "the populated state serves roots for the range"
        );

        // Two real Zakura nodes; the server serves roots from the real state via the
        // production port, the client only advertises the cap and requests.
        let server = ZakuraTestNode::builder(101)
            .max_connections_per_ip(16)
            .service(std::sync::Arc::new(TreeAuxService::new(
                std::sync::Arc::new(StateTreeAuxPort::new(read_state.clone())),
            )))
            .spawn()
            .await?;
        let client = ZakuraTestNode::builder(102)
            .max_connections_per_ip(16)
            .service(std::sync::Arc::new(TreeAuxService::new(
                std::sync::Arc::new(StateTreeAuxPort::new(read_state.clone())),
            )))
            .spawn()
            .await?;

        client.connect_native(&server, CONNECT_TIMEOUT).await?;

        let mut collected = Vec::new();
        fetch_roots(
            &client.supervisor(),
            block::Height(1),
            block::Height(tip),
            |batch| collected.extend(batch),
        )
        .await?;

        assert_eq!(
            collected, expected,
            "roots fetched over tree_aux match the roots the real state serves"
        );

        // Negative / safe-fallback: a range the server's state cannot serve (above the
        // tip) yields an error from the fetch, so the driver leaves that range un-fetched
        // and the committer keeps it on the legacy path — never wrong data.
        let mut unavailable = Vec::new();
        let result = fetch_roots(
            &client.supervisor(),
            block::Height(tip + 5),
            block::Height(tip + 10),
            |batch| unavailable.extend(batch),
        )
        .await;
        assert!(
            result.is_err(),
            "an unavailable range returns an error (the committer keeps it legacy)"
        );
        assert!(
            unavailable.is_empty(),
            "no roots are delivered for an unavailable range"
        );

        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn tree_aux_policy_disconnects_rejected_supplier_and_refetches_from_another_peer(
    ) -> Result<(), BoxError> {
        let _guard = zebra_test::init();
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
        let rejected_height = block::Height(43);
        let rejected_roots: Vec<_> = (42..=45).map(root_at).collect();
        let replacement_roots = rejected_roots.clone();
        let replacement_requests = Arc::new(AtomicUsize::new(0));

        let rejected_server = tree_aux_test_node(201, rejected_roots.clone()).await?;
        let replacement_server = tree_aux_test_node_with_counter(
            202,
            replacement_roots,
            Some(Arc::clone(&replacement_requests)),
        )
        .await?;
        let client = tree_aux_test_node(203, Vec::new()).await?;
        let rejected_peer = ZakuraPeerId::new(
            rejected_server
                .node_addr()
                .await
                .node_id
                .as_bytes()
                .to_vec(),
        )?;

        client
            .connect_native(&rejected_server, CONNECT_TIMEOUT)
            .await?;
        client
            .connect_native(&replacement_server, CONNECT_TIMEOUT)
            .await?;
        wait_for_outbound_peer_count(&client, 2).await?;

        let (_state, _read_state, _latest_tip, _chain_tip, writer) =
            init_test_services_with_tree_aux_writer(&Network::Mainnet).await;
        let writer = writer.expect("mainnet checkpoint-sync state exposes a tree_aux roots writer");
        writer.insert_roots(rejected_roots.clone());

        let mut policy = TreeAuxPeerPolicy::default();
        policy.record_inserted_roots(
            rejected_roots
                .iter()
                .map(|root| (root.height, rejected_peer.clone())),
            writer.committed_through(),
            Instant::now(),
        );
        let previous_offense = Instant::now();
        policy.hard_failed.insert(
            rejected_peer.clone(),
            PeerFailure {
                cooldown_until: previous_offense,
                offenses: TREE_AUX_DISCONNECT_AFTER_FAILURES - 1,
                last_offense: previous_offense,
            },
        );

        handle_refetch_request(
            Ok(rejected_height),
            &client.supervisor(),
            &writer,
            &mut policy,
        )
        .await
        .expect("the production refetch handler keeps running after one request");

        wait_for_peer_disconnect(&client, &rejected_peer).await?;
        assert!(
            policy.is_excluded(&rejected_peer, Instant::now()),
            "the rejected supplier remains excluded after the production refetch handler runs"
        );
        assert!(
            replacement_requests.load(Ordering::Relaxed) > 0,
            "the production refetch handler fetches the rejected height from another peer"
        );

        client.shutdown().await;
        replacement_server.shutdown().await;
        rejected_server.shutdown().await;
        Ok(())
    }

    #[test]
    fn root_fetch_start_is_one_above_the_verified_tip() {
        let net = Network::Mainnet;
        let h = block::Height;
        // The hash is irrelevant to the height math; reuse the genesis hash.
        let at = |n| Some((h(n), net.genesis_hash()));

        // Genesis-empty node: fetch from height 1 — the only case where "from genesis" is correct.
        assert_eq!(root_fetch_start(None, None, &net), h(1));

        // Snapshot node at T (finalized == best tip): fetch from T + 1, NOT genesis. This is the
        // regression this guards: the old driver hard-coded Height(1), refetching ~T already-held
        // roots and starving the window the committer actually needs.
        assert_eq!(
            root_fetch_start(at(3_338_006), at(3_338_006), &net),
            h(3_338_007)
        );

        // Non-finalized blocks above the finalized tip: the verified tip is the higher best tip.
        assert_eq!(root_fetch_start(at(100), at(105), &net), h(106));

        // Finalized tip above the best-chain tip: use the finalized tip.
        assert_eq!(root_fetch_start(at(200), at(150), &net), h(201));
    }

    #[test]
    fn fetch_window_starts_ahead_of_initial_tip() {
        let h = block::Height;

        assert_eq!(
            fetch_window_end(h(10), h(100), None, 16),
            h(25),
            "without a commit watermark, the window starts one below the initial fetch height"
        );
        assert_eq!(
            next_fetch_window(h(10), h(10), h(100), None, 16),
            Some((h(10), h(25))),
            "the initial fetch fills only the bounded fetch-ahead window"
        );
    }

    #[test]
    fn fetch_window_waits_until_commits_open_room() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(10), h(26), h(100), None, 16),
            None,
            "a full fetch-ahead window blocks further prefetch"
        );
        assert_eq!(
            next_fetch_window(h(10), h(26), h(100), Some(h(10)), 16),
            Some((h(26), h(26))),
            "committing one height opens room for one more fetched root"
        );
        assert_eq!(
            next_fetch_window(h(10), h(27), h(100), Some(h(20)), 16),
            Some((h(27), h(36))),
            "additional commit progress opens a larger contiguous window"
        );
    }

    #[test]
    fn fetch_window_does_not_go_behind_next_fetch() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(100), h(100), h(200), Some(h(10)), 16),
            None,
            "a stale watermark below the startup fetch height does not fetch below next_fetch"
        );
    }

    #[test]
    fn fetch_window_clamps_at_handoff() {
        let h = block::Height;

        assert_eq!(
            next_fetch_window(h(10), h(10), h(20), None, 16),
            Some((h(10), h(20))),
            "the bounded window never extends past the checkpoint handoff"
        );
        assert_eq!(
            next_fetch_window(h(10), h(21), h(20), Some(h(20)), 16),
            None,
            "there is no fetch window after the handoff"
        );
    }

    #[test]
    fn fetch_window_uses_saturating_height_math() {
        let h = block::Height;

        assert_eq!(
            fetch_window_end(h(u32::MAX - 5), h(u32::MAX), None, 16),
            h(u32::MAX),
            "fetch-ahead arithmetic saturates before clamping to the handoff"
        );
    }

    #[test]
    fn process_queued_refetch_requests_drains_channel() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);

        tx.send(block::Height(42)).expect("receiver is live");
        tx.send(block::Height(43)).expect("receiver is live");

        let drained = drain_queued_refetch_requests_for_test(&mut rx);
        assert_eq!(drained, 2);
        assert_eq!(drain_queued_refetch_requests_for_test(&mut rx), 0);
    }

    #[test]
    fn staged_roots_insert_after_full_fetch_success() -> Result<(), BoxError> {
        let staged = vec![root_at(42), root_at(43)];
        let expected = staged.clone();
        let mut inserted = None;

        insert_staged_roots_after_success(staged, Vec::new(), Ok(()), |roots, _provenance| {
            inserted = Some(roots)
        })?;

        assert_eq!(
            inserted,
            Some(expected),
            "a successful full-range fetch publishes the staged roots exactly once"
        );
        Ok(())
    }

    #[test]
    fn staged_roots_publish_provenance_after_full_fetch_success() -> Result<(), BoxError> {
        let staged = vec![root_at(42)];
        let provenance = vec![(block::Height(42), peer_id(3))];
        let expected_provenance = provenance.clone();
        let mut inserted_provenance = None;

        insert_staged_roots_after_success(staged, provenance, Ok(()), |_roots, provenance| {
            inserted_provenance = Some(provenance)
        })?;

        assert_eq!(
            inserted_provenance,
            Some(expected_provenance),
            "provenance is published atomically with the successful staged roots"
        );
        Ok(())
    }

    #[test]
    fn staged_roots_are_dropped_after_fetch_error() {
        let staged_prefix = vec![root_at(42), root_at(43)];
        let mut inserted = false;

        let result = insert_staged_roots_after_success(
            staged_prefix,
            Vec::new(),
            Err("suffix unavailable".into()),
            |_roots, _provenance| inserted = true,
        );

        assert!(
            result.is_err(),
            "the original fetch error is returned to the retry loop"
        );
        assert!(
            !inserted,
            "a failed full-range fetch must not publish its staged prefix"
        );
    }

    #[test]
    fn staged_provenance_is_dropped_after_fetch_error() {
        let staged_prefix = vec![root_at(42)];
        let provenance = vec![(block::Height(42), peer_id(4))];
        let mut inserted = false;

        let result = insert_staged_roots_after_success(
            staged_prefix,
            provenance,
            Err("suffix unavailable".into()),
            |_roots, _provenance| inserted = true,
        );

        assert!(
            result.is_err(),
            "the original fetch error is returned to the retry loop"
        );
        assert!(
            !inserted,
            "failed fetches must not publish provenance for roots the committer never saw"
        );
    }

    #[tokio::test]
    async fn refetch_request_fetches_requested_height() {
        let mut fetched = None;
        let mut peer_policy = TreeAuxPeerPolicy::default();

        let result = handle_refetch_request_with_fetch(
            Ok(block::Height(42)),
            &mut peer_policy,
            Instant::now(),
            |height, _excluded| {
                fetched = Some(height);
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "a fetched refetch height keeps the driver alive"
        );
        assert_eq!(
            fetched,
            Some(block::Height(42)),
            "the refetch handler fetches exactly the requested height"
        );
    }

    #[tokio::test]
    async fn refetch_request_keeps_running_after_fetch_error() {
        let mut peer_policy = TreeAuxPeerPolicy::default();
        let result = handle_refetch_request_with_fetch(
            Ok(block::Height(42)),
            &mut peer_policy,
            Instant::now(),
            |_height, _excluded| future::ready(Err::<(), BoxError>("fetch failed".into())),
        )
        .await;

        assert!(
            result.is_ok(),
            "a peer fetch error is retryable and keeps the driver subscribed"
        );
    }

    #[tokio::test]
    async fn refetch_request_ignores_lagged_notifications() {
        let mut fetched = false;
        let mut peer_policy = TreeAuxPeerPolicy::default();

        let result = handle_refetch_request_with_fetch(
            Err(tokio::sync::broadcast::error::RecvError::Lagged(3)),
            &mut peer_policy,
            Instant::now(),
            |_height, _excluded| {
                fetched = true;
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "lagged notifications are recoverable because future retries can resend heights"
        );
        assert!(
            !fetched,
            "lagged notifications do not fetch an unknown height"
        );
    }

    #[tokio::test]
    async fn refetch_request_stops_after_closed_channel() {
        let mut fetched = false;
        let mut peer_policy = TreeAuxPeerPolicy::default();

        let result = handle_refetch_request_with_fetch(
            Err(tokio::sync::broadcast::error::RecvError::Closed),
            &mut peer_policy,
            Instant::now(),
            |_height, _excluded| {
                fetched = true;
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_err(),
            "a closed refetch channel tells the driver there are no more refetch requests"
        );
        assert!(!fetched, "closed channels do not fetch an unknown height");
    }

    #[test]
    fn peer_policy_records_and_prunes_driver_owned_provenance() {
        let mut policy = TreeAuxPeerPolicy::default();
        let peer = peer_id(1);
        let now = Instant::now();

        policy.record_inserted_roots(
            [
                (block::Height(40), peer.clone()),
                (block::Height(41), peer.clone()),
                (block::Height(42), peer.clone()),
            ],
            None,
            now,
        );

        policy.prune_committed(Some(block::Height(41)));

        assert!(
            policy
                .mark_rejected_supplier(block::Height(40), now)
                .is_none(),
            "provenance at committed heights is pruned from the driver side table"
        );
        let rejected = policy
            .mark_rejected_supplier(block::Height(42), now)
            .expect("uncommitted root provenance remains available for refetch policy");
        assert_eq!(rejected.peer_id, peer);
        assert_eq!(
            rejected.heights,
            vec![block::Height(42)],
            "only the remaining uncommitted supplier height is bulk-evicted"
        );
    }

    #[tokio::test]
    async fn refetch_request_excludes_last_supplier_for_height() {
        let mut policy = TreeAuxPeerPolicy::default();
        let bad_peer = peer_id(2);
        let now = Instant::now();
        policy.record_inserted_roots([(block::Height(42), bad_peer.clone())], None, now);

        let mut fetched = None;
        let result = handle_refetch_request_with_fetch(
            Ok(block::Height(42)),
            &mut policy,
            now,
            |height, excluded| {
                fetched = Some((height, excluded));
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(result.is_ok(), "refetch requests keep the driver alive");
        assert_eq!(
            fetched,
            Some((block::Height(42), vec![bad_peer.clone()])),
            "the retry excludes the peer that supplied the rejected height"
        );
        assert!(
            policy.is_excluded(&bad_peer, now),
            "the hard-failed supplier remains in the cooldown set"
        );
    }

    #[test]
    fn soft_failure_demotes_peer_then_expires() {
        let mut policy = TreeAuxPeerPolicy::default();
        let soft_peer = peer_id(5);
        let now = Instant::now();

        policy.record_fetch_event(
            PeerFetchEvent {
                peer_id: soft_peer.clone(),
                height: block::Height(42),
                count: 1,
                status: PeerFetchStatus::SoftFailure {
                    error: "peer cannot serve tree_aux roots".to_string(),
                },
            },
            now,
        );

        assert_eq!(
            policy.peer_preference(&soft_peer, now),
            PeerPreference::Demoted,
            "a soft failure moves the peer behind normal peers"
        );
        assert_eq!(
            policy.peer_preference(
                &soft_peer,
                add_duration(now, TREE_AUX_SOFT_FAILURE_DEMOTION + Duration::from_secs(1))
            ),
            PeerPreference::Normal,
            "soft demotion is temporary"
        );
    }

    #[test]
    fn successful_fetch_clears_soft_demotion() {
        let mut policy = TreeAuxPeerPolicy::default();
        let soft_peer = peer_id(6);
        let now = Instant::now();

        policy.record_soft_failure(soft_peer.clone(), now);
        policy.record_fetch_event(
            PeerFetchEvent {
                peer_id: soft_peer.clone(),
                height: block::Height(42),
                count: 1,
                status: PeerFetchStatus::Success,
            },
            now,
        );

        assert_eq!(
            policy.peer_preference(&soft_peer, now),
            PeerPreference::Normal,
            "a well-shaped response clears the peer's soft-failure state"
        );
    }

    #[test]
    fn hard_failure_overrides_soft_demotion() {
        let mut policy = TreeAuxPeerPolicy::default();
        let bad_peer = peer_id(7);
        let now = Instant::now();

        policy.record_soft_failure(bad_peer.clone(), now);
        policy.record_inserted_roots([(block::Height(42), bad_peer.clone())], None, now);
        policy
            .mark_rejected_supplier(block::Height(42), now)
            .expect("the rejected height has provenance");

        assert_eq!(
            policy.peer_preference(&bad_peer, now),
            PeerPreference::Excluded,
            "verification failures exclude peers even if they were only soft-demoted before"
        );
        assert!(
            !policy.soft_failed.contains_key(&bad_peer),
            "hard failure removes stale soft-failure state for the same peer"
        );
    }

    #[test]
    fn soft_failure_table_is_bounded() {
        let mut policy = TreeAuxPeerPolicy::default();
        let now = Instant::now();

        for seed in 0..=TREE_AUX_SOFT_FAILURE_CAPACITY {
            let seed = u32::try_from(seed).expect("test seed fits in u32");
            let mut bytes = vec![0; 32];
            bytes[..4].copy_from_slice(&seed.to_le_bytes());
            policy.record_soft_failure(
                ZakuraPeerId::new(bytes).expect("test peer id is within bounds"),
                now,
            );
        }

        assert_eq!(
            policy.soft_failed.len(),
            TREE_AUX_SOFT_FAILURE_CAPACITY,
            "soft-failure memory remains bounded"
        );
    }

    #[test]
    fn first_offense_cools_down_but_does_not_disconnect() {
        let mut policy = TreeAuxPeerPolicy::default();
        let bad_peer = peer_id(9);
        let now = Instant::now();
        policy.record_inserted_roots(
            [
                (block::Height(42), bad_peer.clone()),
                (block::Height(43), bad_peer.clone()),
            ],
            None,
            now,
        );

        let rejected = policy
            .mark_rejected_supplier(block::Height(42), now)
            .expect("the rejected height has provenance");

        assert_eq!(rejected.peer_id, bad_peer);
        assert_eq!(rejected.offenses, 1);
        assert!(!rejected.should_disconnect);
        assert_eq!(
            rejected.heights,
            vec![block::Height(42), block::Height(43)],
            "first offense still bulk-evicts the supplier's cached roots"
        );
        assert!(
            policy.is_excluded(&bad_peer, now),
            "first offense excludes the supplier from tree_aux selection"
        );
    }

    #[test]
    fn repeat_offenses_escalate_to_disconnect() {
        let mut policy = TreeAuxPeerPolicy::default();
        let bad_peer = peer_id(10);
        let start = Instant::now();
        let second = add_duration(
            start,
            TREE_AUX_HARD_FAILURE_COOLDOWN + Duration::from_secs(1),
        );
        let third = add_duration(
            second,
            TREE_AUX_HARD_FAILURE_COOLDOWN + Duration::from_secs(1),
        );

        for (height, now, expected_offenses, should_disconnect) in [
            (40, start, 1, false),
            (41, second, 2, false),
            (42, third, 3, true),
        ] {
            policy.record_inserted_roots([(block::Height(height), bad_peer.clone())], None, now);
            let rejected = policy
                .mark_rejected_supplier(block::Height(height), now)
                .expect("the rejected height has provenance");

            assert_eq!(rejected.peer_id, bad_peer);
            assert_eq!(rejected.offenses, expected_offenses);
            assert_eq!(rejected.should_disconnect, should_disconnect);
        }
    }

    #[test]
    fn peer_policy_rejects_all_cached_heights_from_supplier() {
        let mut policy = TreeAuxPeerPolicy::default();
        let bad_peer = peer_id(7);
        let other_peer = peer_id(8);
        let now = Instant::now();
        policy.record_inserted_roots(
            [
                (block::Height(40), bad_peer.clone()),
                (block::Height(41), other_peer.clone()),
                (block::Height(42), bad_peer.clone()),
                (block::Height(43), bad_peer.clone()),
            ],
            None,
            now,
        );

        let rejected = policy
            .mark_rejected_supplier(block::Height(42), now)
            .expect("the rejected height has provenance");

        assert_eq!(
            rejected.peer_id, bad_peer,
            "the supplier of the rejected height is marked as the offender"
        );
        assert_eq!(
            rejected.heights,
            vec![block::Height(40), block::Height(42), block::Height(43)],
            "all still-cached heights from the offender are returned for bulk invalidation"
        );
        assert_eq!(
            policy
                .mark_rejected_supplier(block::Height(41), now)
                .expect("other peer's provenance remains")
                .peer_id,
            other_peer,
            "bulk rejection only removes provenance for the offending supplier"
        );
    }

    #[tokio::test]
    async fn refetch_without_provenance_does_not_exclude_a_peer() {
        let mut policy = TreeAuxPeerPolicy::default();
        let mut fetched = None;

        let result = handle_refetch_request_with_fetch(
            Ok(block::Height(99)),
            &mut policy,
            Instant::now(),
            |height, excluded| {
                fetched = Some((height, excluded));
                future::ready(Ok(()))
            },
        )
        .await;

        assert!(
            result.is_ok(),
            "unknown-height refetches are still retryable"
        );
        assert_eq!(
            fetched,
            Some((block::Height(99), Vec::new())),
            "without provenance, a height-only refetch does not invent an offender"
        );
    }

    #[test]
    fn cooldown_expiry_preserves_offense_count() {
        let mut policy = TreeAuxPeerPolicy::default();
        let peer = peer_id(5);
        let start = Instant::now();
        let after_cooldown = add_duration(
            start,
            TREE_AUX_HARD_FAILURE_COOLDOWN + Duration::from_secs(1),
        );
        policy.hard_failed.insert(
            peer.clone(),
            PeerFailure {
                cooldown_until: add_duration(start, TREE_AUX_HARD_FAILURE_COOLDOWN),
                offenses: 1,
                last_offense: start,
            },
        );

        assert!(
            !policy.is_excluded(&peer, after_cooldown),
            "cooldown expiry makes the peer selectable again"
        );
        assert_eq!(
            policy
                .hard_failed
                .get(&peer)
                .expect("offense record survives cooldown expiry")
                .offenses,
            1,
            "cooldown expiry preserves the offense count inside the decay window"
        );

        policy.record_inserted_roots([(block::Height(50), peer.clone())], None, after_cooldown);
        let rejected = policy
            .mark_rejected_supplier(block::Height(50), after_cooldown)
            .expect("the rejected height has provenance");
        assert_eq!(
            rejected.offenses, 2,
            "a new offense after cooldown continues the same streak"
        );
        assert!(
            !rejected.should_disconnect,
            "the second offense remains below the disconnect threshold"
        );
    }

    #[test]
    fn offense_streak_decays_after_window() {
        let mut policy = TreeAuxPeerPolicy::default();
        let peer = peer_id(11);
        let start = Instant::now();
        let second = add_duration(
            start,
            TREE_AUX_HARD_FAILURE_COOLDOWN + Duration::from_secs(1),
        );
        let decayed = add_duration(second, TREE_AUX_OFFENSE_DECAY + Duration::from_secs(1));

        for (height, now) in [(40, start), (41, second)] {
            policy.record_inserted_roots([(block::Height(height), peer.clone())], None, now);
            let rejected = policy
                .mark_rejected_supplier(block::Height(height), now)
                .expect("the rejected height has provenance");
            assert!(!rejected.should_disconnect);
        }

        policy.record_inserted_roots([(block::Height(42), peer.clone())], None, decayed);
        let rejected = policy
            .mark_rejected_supplier(block::Height(42), decayed)
            .expect("the rejected height has provenance");

        assert_eq!(
            rejected.offenses, 1,
            "old offenses decay before the next failure"
        );
        assert!(
            !rejected.should_disconnect,
            "a decayed streak starts over instead of disconnecting"
        );
    }

    #[test]
    fn peer_policy_bounds_provenance_by_oldest_height() {
        let mut policy = TreeAuxPeerPolicy::default();
        let peer = peer_id(6);
        let now = Instant::now();

        policy.record_inserted_roots(
            (0..=TREE_AUX_PROVENANCE_CAPACITY).map(|height| {
                (
                    block::Height(
                        u32::try_from(height).expect("test provenance height fits in u32"),
                    ),
                    peer.clone(),
                )
            }),
            None,
            now,
        );

        assert_eq!(
            policy.suppliers_by_height.len(),
            TREE_AUX_PROVENANCE_CAPACITY,
            "the driver-owned provenance table stays bounded"
        );
        assert!(
            policy
                .mark_rejected_supplier(block::Height(0), now)
                .is_none(),
            "the oldest height is evicted when the provenance table exceeds its cap"
        );
        let rejected = policy
            .mark_rejected_supplier(block::Height(1), now)
            .expect("newer provenance remains after capacity eviction");
        assert_eq!(rejected.peer_id, peer);
        assert!(
            rejected.heights.contains(&block::Height(1)),
            "bulk eviction includes the rejected height"
        );
    }
}
