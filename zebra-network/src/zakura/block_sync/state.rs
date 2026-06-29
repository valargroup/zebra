use super::{config::*, request::*, work_queue::WorkQueue, *};
use crate::zakura::{
    chain_frontier_from_parts, Frontier, FrontierUpdate, ServicePeerDirection, ServicePeerSnapshot,
    ZakuraBlockSyncCandidateState,
};

/// Hard ceiling on outbound block-range requests kept in flight to one peer.
///
/// A safety bound only; the binding per-peer concurrency is the peer's advertised
/// `max_inflight_requests` (config `max_inflight_requests`, clamped to
/// [`MAX_BS_INFLIGHT_REQUESTS`]).
// `MAX_BS_INFLIGHT_REQUESTS` is a `u32`, which fits in `usize` on supported targets.
pub(super) const EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER: usize = MAX_BS_INFLIGHT_REQUESTS as usize;
/// Consecutive error-free responses that make up one window-growth epoch.
///
/// The adaptive window holds flat for a full epoch of successes before each
/// growth step, so it never opens faster than the peer has demonstrably served.
const OUTBOUND_WINDOW_GROWTH_EPOCH_SUCCESSES: usize = 16;
/// Cubic coefficient for the streak-gated window ramp.
///
/// Target window = `growth_base + COEFF * epoch^3`, capped at the hard cap, where
/// `epoch` counts completed [`OUTBOUND_WINDOW_GROWTH_EPOCH_SUCCESSES`]-success
/// runs since the last timeout. Growth is gentle for the first few epochs and
/// accelerates the longer the peer serves without an error, then resets on the
/// next timeout. With the default base of 64 and a 16-success epoch, the window
/// ramps cubically toward the peer's advertised hard cap (locally clamped to
/// [`MAX_BS_INFLIGHT_REQUESTS`] = 32,768; the default advertisement is 32,000),
/// reaching the default 32,000 ceiling after 16 epochs (256 consecutive
/// error-free successes).
const OUTBOUND_WINDOW_GROWTH_CUBIC_COEFF: usize = 8;
/// Cubic coefficient for the streak-gated timeout backoff.
const OUTBOUND_WINDOW_REDUCTION_CUBIC_COEFF: usize = OUTBOUND_WINDOW_GROWTH_CUBIC_COEFF;
/// Consecutive timeout batches that make up one window-reduction epoch.
const OUTBOUND_WINDOW_REDUCTION_EPOCH_TIMEOUTS: usize = OUTBOUND_WINDOW_GROWTH_EPOCH_SUCCESSES;

/// Cached chain frontiers used by the block-sync reactor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncFrontiers {
    /// Shared finalized height supplied by state.
    pub finalized_height: block::Height,
    /// Highest verified block-body height supplied by state.
    pub verified_block_tip: block::Height,
    /// Hash of [`verified_block_tip`](Self::verified_block_tip).
    pub verified_block_hash: block::Hash,
}

/// Startup inputs for the dependency-neutral block-sync reactor.
#[derive(Clone, Debug)]
pub struct BlockSyncStartup {
    /// Cached state frontiers at startup.
    pub frontiers: BlockSyncFrontiers,
    /// Durable best header tip at startup.
    pub best_header_tip: (block::Height, block::Hash),
    /// Header-sync best-tip watch used as the moving body-download target.
    pub header_tip: Option<watch::Receiver<(block::Height, block::Hash)>>,
    /// Shared sync exchange frontier stream used as the moving body-download target.
    pub frontier_updates: Option<watch::Receiver<FrontierUpdate>>,
    /// Local stream-6 configuration.
    pub config: ZakuraBlockSyncConfig,
    /// Shared shutdown signal owned by the embedding endpoint or test harness.
    pub shutdown: CancellationToken,
    /// Enables query actions for state-backed metadata.
    pub state_queries_enabled: bool,
    /// JSONL trace emitter for block-sync scheduling, download, and commit rows.
    pub trace: ZakuraTrace,
}

impl BlockSyncStartup {
    /// Build block-sync startup config from durable/frontier facts.
    pub fn new(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
        header_tip: watch::Receiver<(block::Height, block::Hash)>,
        config: ZakuraBlockSyncConfig,
    ) -> Self {
        Self {
            frontiers,
            best_header_tip,
            header_tip: Some(header_tip),
            frontier_updates: None,
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: true,
            trace: ZakuraTrace::noop(),
        }
    }

    /// Build block-sync startup config from shared sync exchange frontiers.
    pub fn new_with_exchange(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
        frontier_updates: watch::Receiver<FrontierUpdate>,
        config: ZakuraBlockSyncConfig,
    ) -> Self {
        Self {
            frontiers,
            best_header_tip,
            header_tip: None,
            frontier_updates: Some(frontier_updates),
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: true,
            trace: ZakuraTrace::noop(),
        }
    }

    /// Build a latest-value frontier update stream from legacy startup pieces.
    pub fn frontier_update_from_parts(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
    ) -> FrontierUpdate {
        FrontierUpdate {
            frontier: chain_frontier_from_parts(
                frontiers.finalized_height,
                Frontier::new(frontiers.verified_block_tip, frontiers.verified_block_hash),
                Frontier::new(best_header_tip.0, best_header_tip.1),
            ),
            change: crate::zakura::FrontierChange::Snapshot,
        }
    }

    pub(super) fn inert(config: ZakuraBlockSyncConfig) -> Self {
        Self {
            frontiers: BlockSyncFrontiers {
                finalized_height: block::Height::MIN,
                verified_block_tip: block::Height::MIN,
                verified_block_hash: block::Hash([0; 32]),
            },
            best_header_tip: (block::Height::MIN, block::Hash([0; 32])),
            header_tip: None,
            frontier_updates: None,
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: false,
            trace: ZakuraTrace::noop(),
        }
    }
}

/// Cheap cloneable handle used by services and drivers to inform block sync.
///
/// per-peer routines carries the shared per-peer download primitives here too, so
/// `service::add_peer` (the pipe-routine spawn point) can wire each per-peer
/// pipe-routine with the same `WorkQueue`/`ByteBudget`/`PeerRegistry`/Sequencer/
/// action/routine-to-reactor channels the reactor created.
#[derive(Clone, Debug)]
pub struct BlockSyncHandle {
    pub(super) events: mpsc::Sender<BlockSyncEvent>,
    pub(super) lifecycle: mpsc::UnboundedSender<BlockSyncEvent>,
    pub(super) peers: watch::Receiver<ServicePeerSnapshot>,
    pub(super) status: watch::Receiver<BlockSyncStatus>,
    pub(super) candidates: watch::Receiver<ZakuraBlockSyncCandidateState>,
    /// Shared primitives every per-peer pipe-routine is wired with at spawn
    /// (`service::add_peer`). `None` for the inert/handle-less test constructors
    /// that never spawn routines.
    pub(super) routine_wiring: Option<RoutineWiring>,
}

/// The shared download primitives a per-peer pipe-routine is constructed with.
/// Created once in `spawn_block_sync_reactor` and threaded through the handle to
/// `service::add_peer`.
#[derive(Clone, Debug)]
pub(super) struct RoutineWiring {
    pub(super) config: ZakuraBlockSyncConfig,
    pub(super) budget: ByteBudget,
    pub(super) work: Arc<WorkQueue>,
    pub(super) registry: Arc<super::peer_registry::PeerRegistry>,
    pub(super) received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
    pub(super) sequencer_input: mpsc::Sender<super::sequencer_task::SequencedBody>,
    pub(super) sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    pub(super) sequencer_control:
        mpsc::UnboundedSender<super::sequencer_task::SequencerControlInput>,
    pub(super) actions: mpsc::Sender<BlockSyncAction>,
    pub(super) routine_to_reactor: mpsc::Sender<super::events::RoutineToReactor>,
    pub(super) view: watch::Receiver<super::sequencer_task::SequencerView>,
    pub(super) trace: ZakuraTrace,
}

impl BlockSyncHandle {
    /// Send a fact/event to the block-sync reactor.
    pub async fn send(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.events.send(event).await
    }

    /// Try to send a fact/event without awaiting.
    pub fn try_send(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::TrySendError<BlockSyncEvent>> {
        self.events.try_send(event)
    }

    /// Send a control-plane event without sharing the bounded wire-event queue.
    pub fn send_control(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.lifecycle
            .send(event)
            .map_err(|error| mpsc::error::SendError(error.0))
    }

    /// Send a peer lifecycle event without sharing the bounded wire-event queue.
    pub fn send_lifecycle(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.send_control(event)
    }

    /// Return the currently cached peer slot snapshot.
    pub fn peer_snapshot(&self) -> ServicePeerSnapshot {
        *self.peers.borrow()
    }

    /// Subscribe to local block-sync status advertisements.
    pub fn subscribe_status(&self) -> watch::Receiver<BlockSyncStatus> {
        self.status.clone()
    }

    /// Return the currently cached local status advertisement.
    pub fn local_status(&self) -> BlockSyncStatus {
        *self.status.borrow()
    }

    /// Subscribe to block-sync candidate-selection hints.
    pub fn subscribe_candidate_state(&self) -> watch::Receiver<ZakuraBlockSyncCandidateState> {
        self.candidates.clone()
    }

    /// Return the currently cached block-sync candidate-selection hints.
    pub fn candidate_state(&self) -> ZakuraBlockSyncCandidateState {
        self.candidates.borrow().clone()
    }
}

#[derive(Debug)]
pub(super) struct BlockSyncState {
    pub(super) finalized_height: block::Height,
    pub(super) verified_block_hash: block::Hash,
    pub(super) servable_high: block::Height,
    pub(super) servable_hash: block::Hash,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    /// Thin per-peer handles the reactor keeps for demux/serving/admission. The
    /// per-peer *download* state moved into the spawned [`PeerRoutine`](super::peer_routine)
    /// (per-peer routines); the cross-peer facts the reactor/producer need live in the
    /// [`PeerRegistry`](super::peer_registry).
    pub(super) peers: HashMap<ZakuraPeerId, PeerBlockState>,
    pub(super) parked_peers: HashSet<ZakuraPeerId>,
    /// Sorted set of needed download heights. Replaces the central
    /// `BlockRangeScheduler`: the per-peer issuance path pulls work in its own
    /// servable range, dedup/covered are `in_flight`, and the floor is GC only.
    /// `Arc` so the state stays cheaply `Clone` and the queue is shared with the
    /// Sequencer task and the per-peer routines.
    pub(super) work_queue: Arc<WorkQueue>,
    pub(super) budget: ByteBudget,
    pub(super) needed_heights: Vec<block::Height>,
    pub(super) status_refresh: RateMeter,
    pub(super) pending_status_refresh: bool,
    pub(super) last_advertised_status: BlockSyncStatus,
    /// Throughput of bodies received off the wire (the download rate). Shared
    /// with the per-peer routines (they `record` on receipt); the reactor samples
    /// it each trace tick. Compared against the Sequencer task's committed
    /// throughput it separates a download-limited sync from a commit-limited one.
    pub(super) received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
}

impl BlockSyncState {
    pub(super) fn new(startup: &BlockSyncStartup) -> Self {
        let last_advertised_status = BlockSyncStatus {
            servable_low: block::Height::MIN,
            servable_high: startup.frontiers.verified_block_tip,
            tip_hash: startup.frontiers.verified_block_hash,
            max_blocks_per_response: startup.config.advertised_max_blocks_per_response(),
            max_inflight_requests: startup.config.advertised_max_inflight_requests(),
            max_response_bytes: startup.config.advertised_max_response_bytes(),
        };

        Self {
            finalized_height: startup.frontiers.finalized_height,
            verified_block_hash: startup.frontiers.verified_block_hash,
            servable_high: startup.frontiers.verified_block_tip,
            servable_hash: startup.frontiers.verified_block_hash,
            best_header_tip: startup.best_header_tip.0,
            best_header_hash: startup.best_header_tip.1,
            peers: HashMap::new(),
            parked_peers: HashSet::new(),
            work_queue: Arc::new(WorkQueue::new(startup.frontiers.verified_block_tip)),
            budget: ByteBudget::new(startup.config.max_inflight_block_bytes),
            needed_heights: Vec::new(),
            status_refresh: RateMeter::new(startup.config.status_refresh_interval),
            pending_status_refresh: false,
            last_advertised_status,
            received_throughput: Arc::new(std::sync::Mutex::new(ThroughputMeter::new(
                Instant::now(),
            ))),
        }
    }

    pub(super) fn peer_snapshot(&self, limits: ServicePeerLimits) -> ServicePeerSnapshot {
        let inbound = self
            .peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Inbound)
            .count();
        let outbound = self
            .peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Outbound)
            .count();
        ServicePeerSnapshot::new(inbound, outbound, limits)
    }
}

/// Adaptive per-peer outbound request window + outstanding requests.
///
/// Kept as a standalone type so the window math stays unit-testable; the per-peer
/// download state lives in the spawned [`PeerRoutine`](super::peer_routine), which
/// embeds one of these.
#[derive(Clone, Debug)]
pub(super) struct DownloadWindow {
    pub(super) max_inflight_requests: u32,
    pub(super) outbound_request_window: usize,
    pub(super) timeout_recovery_slots: usize,
    pub(super) outstanding: Vec<OutstandingBlockRange>,
    /// Completed error-free responses since the last timeout-driven reduction.
    /// Drives the streak-gated cubic ramp in
    /// [`increase_outbound_window_after_success`](Self::increase_outbound_window_after_success).
    consecutive_successes: usize,
    /// Consecutive timeout batches since the last successful response.
    ///
    /// Drives the same streak-gated cubic shape as successful response growth,
    /// but downward. The window holds flat for a full timeout epoch, then lowers
    /// faster as the consecutive timeout streak grows.
    consecutive_timeouts: usize,
    /// Window value at the last reduction — the floor the cubic ramp grows back
    /// up from, so probing resumes from the post-backoff window rather than the
    /// original slow-start point.
    growth_base: usize,
    /// Window value when the current timeout streak began.
    reduction_base: usize,
    /// Deadline by which an active peer must send another accepted full block.
    pub(super) block_liveness_deadline: Option<Instant>,
    /// Last time this peer sent an accepted full block body.
    pub(super) last_block_at: Option<Instant>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LivenessOutcome {
    Ok,
    Disarm,
    Disconnect,
}

impl DownloadWindow {
    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        let max_inflight_requests = config.advertised_max_inflight_requests();
        // Slow-start: open at the configured initial window (clamped to the
        // advertised hard cap) and grow toward the cap on success, rather than
        // opening at the full `max_inflight`.
        let initial_window = config
            .initial_inflight_requests
            .clamp(1, max_inflight_requests);
        let initial_window = usize::try_from(initial_window)
            .expect("u32 initial inflight requests fits in usize on supported targets");
        Self {
            max_inflight_requests,
            outbound_request_window: initial_window,
            timeout_recovery_slots: 0,
            outstanding: Vec::new(),
            consecutive_successes: 0,
            consecutive_timeouts: 0,
            growth_base: initial_window,
            reduction_base: initial_window,
            block_liveness_deadline: None,
            last_block_at: None,
        }
    }

    pub(super) fn available_slots(&self) -> usize {
        let hard_capacity = self.hard_outbound_capacity();
        let adaptive_limit = hard_capacity.min(self.outbound_request_window);
        let adaptive_slots = adaptive_limit.saturating_sub(self.outstanding.len());
        if adaptive_slots > 0 {
            return adaptive_slots;
        }

        if self.outbound_request_window == 1 {
            return 0;
        }

        self.timeout_recovery_slots
            .min(hard_capacity.saturating_sub(self.outstanding.len()))
    }

    pub(super) fn reduce_outbound_window_after_timeout(&mut self) {
        if self.consecutive_timeouts == 0 {
            self.reduction_base = self.outbound_request_window;
        }

        // Increment the consecutive timeout streak.
        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);

        let epoch = self.consecutive_timeouts / OUTBOUND_WINDOW_REDUCTION_EPOCH_TIMEOUTS;
        if epoch > 0 {
            let cubic_reduction = epoch
                .saturating_mul(epoch)
                .saturating_mul(epoch)
                .saturating_mul(OUTBOUND_WINDOW_REDUCTION_CUBIC_COEFF);
            let target = self.reduction_base.saturating_sub(cubic_reduction).max(1);
            // Reduction only: never grow the window on a timeout.
            self.outbound_request_window = self.outbound_request_window.min(target).max(1);
        }

        // A timeout ends the current success streak; the cubic ramp restarts from
        // the reduced window, so growth probes back up from the post-backoff floor.
        self.consecutive_successes = 0;
        self.growth_base = self.outbound_request_window;
        self.timeout_recovery_slots = self
            .timeout_recovery_slots
            .saturating_add(1)
            .min(self.hard_outbound_capacity());
    }

    /// Grow the adaptive window on a successful response using a streak-gated
    /// cubic ramp: hold the window flat until a full epoch of consecutive
    /// successes, then raise it to `growth_base + COEFF * epoch^3` (capped at the
    /// hard cap). The step grows cubically with the no-error streak, so the window
    /// opens gently at first and accelerates the longer the peer serves without a
    /// timeout. Any timeout resets the streak and the base (see
    /// [`reduce_outbound_window_after_timeout`](Self::reduce_outbound_window_after_timeout)).
    pub(super) fn increase_outbound_window_after_success(&mut self) {
        self.consecutive_timeouts = 0;
        self.reduction_base = self.outbound_request_window;
        self.consecutive_successes = self.consecutive_successes.saturating_add(1);
        let max_window = self.hard_outbound_capacity();
        if self.outbound_request_window >= max_window {
            return;
        }
        let epoch = self.consecutive_successes / OUTBOUND_WINDOW_GROWTH_EPOCH_SUCCESSES;
        if epoch == 0 {
            // Still inside the first flat epoch: do not open the window yet.
            return;
        }
        let cubic = epoch
            .saturating_mul(epoch)
            .saturating_mul(epoch)
            .saturating_mul(OUTBOUND_WINDOW_GROWTH_CUBIC_COEFF);
        let target = self.growth_base.saturating_add(cubic).min(max_window);
        // Growth only: never shrink the window on a success.
        self.outbound_request_window = self.outbound_request_window.max(target);
    }

    pub(super) fn record_outbound_request_scheduled(&mut self) {
        let adaptive_limit = self
            .hard_outbound_capacity()
            .min(self.outbound_request_window);
        if self.outstanding.len() >= adaptive_limit && self.timeout_recovery_slots > 0 {
            self.timeout_recovery_slots = self.timeout_recovery_slots.saturating_sub(1);
        }
    }

    pub(super) fn arm_liveness(&mut self, now: Instant, timeout: Duration) {
        if self.block_liveness_deadline.is_none() {
            self.block_liveness_deadline = Some(now + timeout);
        }
    }

    pub(super) fn note_block_progress(&mut self, now: Instant, timeout: Duration) {
        self.last_block_at = Some(now);
        self.block_liveness_deadline = if self.outstanding.is_empty() {
            None
        } else {
            Some(now + timeout)
        };
    }

    pub(super) fn disarm_liveness_if_idle(&mut self) {
        if self.outstanding.is_empty() {
            self.block_liveness_deadline = None;
        }
    }

    pub(super) fn check_liveness(&self, now: Instant) -> LivenessOutcome {
        match self.block_liveness_deadline {
            None => LivenessOutcome::Ok,
            Some(deadline) if now < deadline => LivenessOutcome::Ok,
            Some(_) if self.outstanding.is_empty() => LivenessOutcome::Disarm,
            Some(_) => LivenessOutcome::Disconnect,
        }
    }

    pub(super) fn hard_outbound_capacity(&self) -> usize {
        usize::try_from(self.max_inflight_requests)
            .expect("u32 max inflight requests fits in usize on supported targets")
            .min(EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER)
    }

    pub(super) fn outstanding_index_for_height(&self, height: block::Height) -> Option<usize> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.request.contains(height))
    }

    pub(super) fn outstanding_index_for_start(&self, start_height: block::Height) -> Option<usize> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.request.start_height == start_height)
    }
}

/// Thin per-peer handle the reactor keeps to serve inbound
/// `GetBlocks` (the session clone + serving meters), advertise our `Status`, count
/// admission, and tear down. The per-peer *download* state + inbound decode live
/// in the per-peer pipe-routine ([`PeerRoutine`](super::peer_routine)); servable/
/// caps live in the [`PeerRegistry`](super::peer_registry). There is no reactor→
/// routine channel (inverted data flow): the routine owns its own `FramedRecv`.
#[derive(Debug)]
pub(super) struct PeerBlockState {
    pub(super) session: BlockSyncPeerSession,
    pub(super) direction: ServicePeerDirection,
    /// Per-peer rate meter for the reactor's `Status` *advertisement* refresh
    /// (serving-tip change broadcast + retry to peers that have not acknowledged
    /// our Status). The inbound-status *reply* half lives on the routine's
    /// `status_reply_meter`; this half stays reactor-side because the reactor owns
    /// serving-tip advertisement.
    pub(super) refresh_meter: RateMeter,
    pub(super) served_blocks_inflight: u32,
    pub(super) served_block_requests: VecDeque<(block::Height, Instant)>,
}

impl PeerBlockState {
    pub(super) fn new(session: BlockSyncPeerSession, config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            direction: session.direction(),
            session,
            refresh_meter: RateMeter::new(config.status_refresh_interval),
            served_blocks_inflight: 0,
            served_block_requests: VecDeque::new(),
        }
    }

    pub(super) fn try_start_serving_blocks(
        &mut self,
        local_inflight_cap: u32,
        start_height: block::Height,
    ) -> bool {
        if self.served_blocks_inflight >= local_inflight_cap {
            return false;
        }
        self.served_blocks_inflight = self.served_blocks_inflight.saturating_add(1);
        self.served_block_requests
            .push_back((start_height, Instant::now()));
        true
    }

    pub(super) fn serving_blocks_elapsed(&self, start_height: block::Height) -> Option<Duration> {
        self.served_block_requests
            .iter()
            .find_map(|(start, started)| (*start == start_height).then(|| started.elapsed()))
    }

    pub(super) fn finish_serving_blocks(
        &mut self,
        start_height: block::Height,
    ) -> Option<Duration> {
        self.served_blocks_inflight = self.served_blocks_inflight.saturating_sub(1);
        self.served_block_requests
            .iter()
            .position(|(start, _)| *start == start_height)
            .and_then(|index| self.served_block_requests.remove(index))
            .map(|(_, started)| started.elapsed())
    }
}

#[derive(Clone, Debug)]
pub(super) struct OutstandingBlockRange {
    pub(super) request: BlockRangeRequest,
    pub(super) queued_at: Instant,
    pub(super) deadline: Instant,
    pub(super) received: ReceivedBlockTracker,
}

impl OutstandingBlockRange {
    /// Bytes still reserved for this request: the sum of the per-height size
    /// estimates for every requested height not yet received. Each received body
    /// shrinks its estimate toward the actual size, so releasing this (on
    /// timeout/disconnect/short response) never over-releases bytes already handed
    /// to the reorder buffer.
    #[cfg(test)]
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| !self.has_received(expected.height))
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    pub(super) fn estimated_bytes_for_height(&self, height: block::Height) -> Option<u64> {
        self.request.estimated_bytes_for_height(height)
    }

    pub(super) fn has_received(&self, height: block::Height) -> bool {
        self.request
            .offset_for_height(height)
            .is_some_and(|offset| self.received.contains_offset(offset))
    }

    pub(super) fn mark_received(&mut self, height: block::Height) {
        if let Some(offset) = self.request.offset_for_height(height) {
            self.received.insert_offset(offset);
        }
    }

    /// Mark every requested height at or below `tip` as received and return the
    /// sum of the per-height size estimates those newly-received heights still
    /// held, so the caller releases exactly the reservation those heights held.
    pub(super) fn mark_received_through(&mut self, tip: block::Height) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| {
                expected.height <= tip
                    && self
                        .request
                        .offset_for_height(expected.height)
                        .is_some_and(|offset| self.received.insert_offset(offset))
            })
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    pub(super) fn is_complete(&self) -> bool {
        self.received.len() == self.request.expected_blocks.len()
    }
}

/// Pure per-height byte-accounting state.
///
/// The shared [`ByteBudget`] is just the atomic sink. This ledger owns the
/// lifecycle arithmetic for one requested height:
/// `Reserved(estimate) -> Held(actual) -> Released`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BlockBudgetLedger {
    Reserved(u64),
    Held(u64),
    Released,
}

impl BlockBudgetLedger {
    pub(super) fn reserved(estimate: u64) -> Self {
        Self::Reserved(estimate)
    }

    pub(super) fn current_charge(self) -> u64 {
        match self {
            Self::Reserved(bytes) | Self::Held(bytes) => bytes,
            Self::Released => 0,
        }
    }

    pub(super) fn release_reserved(&mut self) -> u64 {
        let released = match *self {
            Self::Reserved(bytes) => bytes,
            Self::Held(_) | Self::Released => 0,
        };
        *self = Self::Released;
        released
    }

    pub(super) fn reserved_charge(self) -> u64 {
        match self {
            Self::Reserved(bytes) => bytes,
            Self::Held(_) | Self::Released => 0,
        }
    }

    pub(super) fn is_reserved(self) -> bool {
        matches!(self, Self::Reserved(_))
    }

    /// Move a reserved height to held bytes and return the signed budget delta.
    ///
    /// Positive means charge more bytes; negative means release bytes.
    pub(super) fn settle(&mut self, actual: u64) -> i128 {
        match *self {
            Self::Reserved(reserved) => {
                *self = Self::Held(actual);
                i128::from(actual) - i128::from(reserved)
            }
            Self::Released => 0,
            Self::Held(_) => 0,
        }
    }

    /// Release the current charge exactly once.
    pub(super) fn release(&mut self) -> u64 {
        let charge = self.current_charge();
        *self = Self::Released;
        charge
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct ReceivedBlockTracker {
    bits: u128,
    count: usize,
}

impl ReceivedBlockTracker {
    pub(super) fn len(&self) -> usize {
        self.count
    }

    fn contains_offset(&self, offset: u32) -> bool {
        Self::bit_for_offset(offset).is_some_and(|bit| self.bits & bit != 0)
    }

    fn insert_offset(&mut self, offset: u32) -> bool {
        let Some(bit) = Self::bit_for_offset(offset) else {
            return false;
        };
        if self.bits & bit != 0 {
            return false;
        }
        self.bits |= bit;
        self.count = self.count.saturating_add(1);
        true
    }

    fn bit_for_offset(offset: u32) -> Option<u128> {
        1u128.checked_shl(offset)
    }
}

#[derive(Clone, Debug)]
pub(super) struct RateMeter {
    pub(super) next_allowed: Instant,
    pub(super) interval: Duration,
}

impl RateMeter {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            next_allowed: Instant::now(),
            interval,
        }
    }

    pub(super) fn try_take(&mut self, now: Instant) -> bool {
        if now < self.next_allowed {
            return false;
        }
        self.next_allowed = now + self.interval;
        true
    }

    pub(super) fn mark_taken(&mut self, now: Instant) {
        self.next_allowed = now + self.interval;
    }
}

/// Tracks block-body throughput (bytes and block counts) over the interval
/// between samples, so the trace snapshot can report download/commit rates while
/// driving toward the 1–2 Gbps target. `record` accumulates; `sample` snapshots
/// the per-second rate since the last sample and resets the window. The last
/// computed rate is cached so it can be read from the immutable trace path. Cost
/// is two saturating adds per body and one division per sample tick.
#[derive(Clone, Debug)]
pub(super) struct ThroughputMeter {
    bytes: u64,
    blocks: u64,
    window_start: Instant,
    last_bytes_per_sec: u64,
    last_blocks_per_sec: u64,
}

impl ThroughputMeter {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            bytes: 0,
            blocks: 0,
            window_start: now,
            last_bytes_per_sec: 0,
            last_blocks_per_sec: 0,
        }
    }

    pub(super) fn record(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.blocks = self.blocks.saturating_add(1);
    }

    /// Recompute the cached per-second rates from the bytes/blocks accumulated
    /// since the last sample, then reset the window. A non-positive interval
    /// (clock not advanced between samples) leaves the cached rates untouched.
    pub(super) fn sample(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.window_start)
            .as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        // `as u64` truncates a finite, non-negative rate; both numerator and
        // denominator are non-negative so the cast cannot wrap or go negative.
        self.last_bytes_per_sec = (self.bytes as f64 / elapsed) as u64;
        self.last_blocks_per_sec = (self.blocks as f64 / elapsed) as u64;
        self.bytes = 0;
        self.blocks = 0;
        self.window_start = now;
    }

    pub(super) fn bytes_per_sec(&self) -> u64 {
        self.last_bytes_per_sec
    }

    pub(super) fn blocks_per_sec(&self) -> u64 {
        self.last_blocks_per_sec
    }
}

// `ByteBudget` was promoted to `transport/guard.rs` so byte-rate protection is
// reusable across services. Re-exported here so existing block_sync call sites
// (`reorder.rs`, `scheduler.rs`, `tests.rs`, and the field on this module's
// state) keep resolving unchanged.
pub(crate) use crate::zakura::transport::ByteBudget;

pub(super) fn next_height(height: block::Height) -> Option<block::Height> {
    height.0.checked_add(1).map(block::Height)
}

pub(super) fn previous_height(height: block::Height) -> Option<block::Height> {
    height.0.checked_sub(1).map(block::Height)
}

pub(super) fn height_after_count(start: block::Height, count: u32) -> Option<block::Height> {
    start.0.checked_add(count).map(block::Height)
}
