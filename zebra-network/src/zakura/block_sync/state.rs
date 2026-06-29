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
/// BBR-lite multiplicative cwnd dip applied on a real request timeout (one dip,
/// not the cubic ladder), bounded below by `bbr_min_cwnd`.
const BBR_TIMEOUT_DIP: f64 = 0.85;

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

/// A time-windowed set of `f64` samples supporting `min` (RTprop) and `max` (BtlBw)
/// filters — the BBR-lite estimators. Samples older than `horizon` are pruned on
/// insert; the windows are small (seconds of per-request samples) so the linear
/// scan is cheap and runs once per completed request.
#[derive(Clone, Debug)]
struct WindowedSamples {
    horizon: Duration,
    samples: Vec<(Instant, f64)>,
}

impl WindowedSamples {
    fn new(horizon: Duration) -> Self {
        Self {
            horizon,
            samples: Vec::new(),
        }
    }

    fn observe(&mut self, now: Instant, value: f64) {
        self.samples.push((now, value));
        if let Some(cutoff) = now.checked_sub(self.horizon) {
            self.samples.retain(|(at, _)| *at >= cutoff);
        }
    }

    fn min(&self) -> Option<f64> {
        self.samples
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::min)
    }

    fn max(&self) -> Option<f64> {
        self.samples
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::max)
    }
}

/// Per-peer BBR-lite control parameters extracted from config (Copy, lock-free).
#[derive(Copy, Clone, Debug)]
struct BbrParams {
    cwnd_gain: f64,
    min_cwnd: usize,
    startup_cwnd: usize,
    rtprop_window: Duration,
    delivery_rate_window: Duration,
    /// How long between ProbeRTT drains (the cadence at which RTprop is refreshed).
    probe_rtt_interval: Duration,
    /// How long to hold the cwnd at `min_cwnd` once the queue has drained, so at
    /// least one uncontended request completes and yields a clean RTprop sample.
    probe_rtt_duration: Duration,
}

impl BbrParams {
    fn from_config(config: &ZakuraBlockSyncConfig) -> Self {
        let min_cwnd = usize::try_from(config.bbr_min_cwnd).unwrap_or(1).max(1);
        // Cold start opens at the configured initial window until the first BDP sample.
        let startup_cwnd = usize::try_from(config.initial_inflight_requests)
            .unwrap_or(min_cwnd)
            .max(min_cwnd);
        Self {
            cwnd_gain: f64::from(config.bbr_cwnd_gain_percent) / 100.0,
            min_cwnd,
            startup_cwnd,
            rtprop_window: config.bbr_rtprop_window,
            delivery_rate_window: config.bbr_delivery_rate_window,
            probe_rtt_interval: config.bbr_probe_rtt_interval,
            probe_rtt_duration: config.bbr_probe_rtt_duration,
        }
    }
}

/// BBR-lite control phase. `ProbeBw` is the steady state (cwnd tracks BDP × gain);
/// `ProbeRtt` periodically drains the queue to `min_cwnd` to take a fresh, uncontended
/// RTprop sample. Without ProbeRtt, a peer's RTprop min-filter stays inflated under a
/// sustained queue (the round-trip we measure is queue + serve + RTT), so the cwnd never
/// collapses for a genuinely slow peer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum BbrPhase {
    ProbeBw,
    ProbeRtt,
}

impl BbrPhase {
    /// Numeric code for the JSONL trace (0 = ProbeBw, 1 = ProbeRtt).
    fn trace_code(self) -> u64 {
        match self {
            BbrPhase::ProbeBw => 0,
            BbrPhase::ProbeRtt => 1,
        }
    }
}

/// Per-peer BBR-lite estimators: an RTprop min-filter over request round-trips, a
/// BtlBw max-filter over per-ack delivery rate, and a delivered-block counter. The
/// owning routine samples these lock-free on each completed request. Stage 1 measures
/// and traces them; the control law (`available_slots`) consumes them in a later stage.
#[derive(Clone, Debug)]
struct BbrState {
    params: BbrParams,
    rtprop_secs: WindowedSamples,
    btlbw_blocks_per_sec: WindowedSamples,
    delivered: u64,
    /// Effective cwnd in blocks currently applied by `available_slots`: the
    /// BDP-derived target once measured, the startup window before that, dipped on
    /// timeouts. Never below `min_cwnd`. Ignored while in `ProbeRtt` (which forces
    /// `min_cwnd`) but preserved so the cwnd restores on exit.
    cwnd_cap: usize,
    /// Current control phase.
    phase: BbrPhase,
    /// When the last ProbeRtt completed (or the first delivery, to anchor the first
    /// probe one interval out). `None` until the first delivery is recorded.
    last_probe_rtt_at: Option<Instant>,
    /// Set the moment the queue first drains to `min_cwnd` during a ProbeRtt; the
    /// `probe_rtt_duration` hold timer runs from here.
    probe_rtt_drained_at: Option<Instant>,
}

impl BbrState {
    fn new(config: &ZakuraBlockSyncConfig) -> Self {
        let params = BbrParams::from_config(config);
        Self {
            rtprop_secs: WindowedSamples::new(params.rtprop_window),
            btlbw_blocks_per_sec: WindowedSamples::new(params.delivery_rate_window),
            delivered: 0,
            cwnd_cap: params.startup_cwnd,
            phase: BbrPhase::ProbeBw,
            last_probe_rtt_at: None,
            probe_rtt_drained_at: None,
            params,
        }
    }

    /// Record a completed request: `elapsed` from send to the final body, `blocks` in
    /// it, `inflight` = requests still outstanding to this peer *after* this completion.
    /// The RTprop sample is the round-trip; the BtlBw sample is the delivery rate, with
    /// the interval floored at the current RTprop so a burst of buffered bodies arriving
    /// within one tick cannot inflate the bandwidth estimate. Re-derives the applied cwnd
    /// from the fresh BDP estimate, then advances the ProbeBw/ProbeRtt phase machine.
    fn record_delivery(&mut self, now: Instant, elapsed: Duration, blocks: u32, inflight: usize) {
        let secs = elapsed.as_secs_f64();
        self.rtprop_secs.observe(now, secs);
        let floor = self.rtprop_secs.min().unwrap_or(secs).max(1e-4);
        let rate = f64::from(blocks) / secs.max(floor);
        self.btlbw_blocks_per_sec.observe(now, rate);
        self.delivered = self.delivered.saturating_add(u64::from(blocks));
        if let Some(target) = self.cwnd_target() {
            self.cwnd_cap = target;
        }
        self.advance_phase(now, inflight);
    }

    /// Drive the ProbeBw/ProbeRtt cycle off completed deliveries (the only event that
    /// carries both a fresh timestamp and the current inflight count). ProbeRtt forces
    /// the cwnd to `min_cwnd`, which drains the queue; once drained, it holds for
    /// `probe_rtt_duration` so an uncontended request completes and refreshes RTprop.
    fn advance_phase(&mut self, now: Instant, inflight: usize) {
        // Anchor the first probe one interval after the first delivery.
        let anchor = *self.last_probe_rtt_at.get_or_insert(now);
        match self.phase {
            BbrPhase::ProbeBw => {
                if now.saturating_duration_since(anchor) >= self.params.probe_rtt_interval {
                    self.phase = BbrPhase::ProbeRtt;
                    self.probe_rtt_drained_at = None;
                }
            }
            BbrPhase::ProbeRtt => {
                // Start the hold timer the moment the queue first reaches the floor.
                if self.probe_rtt_drained_at.is_none() && inflight <= self.params.min_cwnd {
                    self.probe_rtt_drained_at = Some(now);
                }
                if let Some(drained_at) = self.probe_rtt_drained_at {
                    if now.saturating_duration_since(drained_at) >= self.params.probe_rtt_duration {
                        // Exit: a clean RTprop sample has been taken at low queue depth.
                        self.phase = BbrPhase::ProbeBw;
                        self.last_probe_rtt_at = Some(now);
                        self.probe_rtt_drained_at = None;
                        if let Some(target) = self.cwnd_target() {
                            self.cwnd_cap = target;
                        }
                    }
                }
            }
        }
    }

    /// The effective cwnd in blocks currently applied (never below `min_cwnd`). During
    /// ProbeRtt the cwnd is pinned to `min_cwnd` to drain the queue.
    fn effective_cwnd(&self) -> usize {
        match self.phase {
            BbrPhase::ProbeRtt => self.params.min_cwnd,
            BbrPhase::ProbeBw => self.cwnd_cap.max(self.params.min_cwnd),
        }
    }

    /// Apply one multiplicative dip on a real timeout (BBR-style), bounded by the
    /// minimum cwnd. Does not run the cubic backoff ladder. Suppressed during ProbeRtt,
    /// where the cwnd is already pinned to `min_cwnd` and timeouts are an expected
    /// consequence of the drain, not congestion signal.
    fn dip_on_timeout(&mut self) {
        if self.phase == BbrPhase::ProbeRtt {
            return;
        }
        let scaled = (self.cwnd_cap as f64 * BBR_TIMEOUT_DIP).round();
        // A non-negative product of a usize and 0.85; the cast is safe.
        let dipped = if scaled.is_finite() && scaled >= 0.0 {
            scaled as usize
        } else {
            self.params.min_cwnd
        };
        self.cwnd_cap = dipped.max(self.params.min_cwnd);
    }

    /// Bandwidth-delay product in blocks: BtlBw (blocks/s) × RTprop (s). `None` until
    /// at least one delivery sample exists (cold start).
    fn bdp_blocks(&self) -> Option<f64> {
        match (self.btlbw_blocks_per_sec.max(), self.rtprop_secs.min()) {
            (Some(rate), Some(rtprop)) => Some(rate * rtprop),
            _ => None,
        }
    }

    /// Target cwnd in blocks = `max(min_cwnd, BDP × gain)`. `None` until the first
    /// delivery sample exists, so the cwnd stays at the cold-start value until then.
    fn cwnd_target(&self) -> Option<usize> {
        let bdp = self.bdp_blocks()?;
        let scaled = (bdp * self.params.cwnd_gain).round();
        // BDP × gain is a non-negative, finite product of measured rates; clamp
        // defensively and the cast is safe.
        let cwnd = if scaled.is_finite() && scaled >= 0.0 {
            scaled as usize
        } else {
            self.params.min_cwnd
        };
        Some(cwnd.max(self.params.min_cwnd))
    }

    fn rtprop_ms(&self) -> Option<u64> {
        // A rounded non-negative round-trip in milliseconds fits u64 for any real RTT.
        self.rtprop_secs
            .min()
            .map(|secs| (secs * 1000.0).round() as u64)
    }

    fn btlbw_milliblocks_per_sec(&self) -> Option<u64> {
        // A rounded non-negative rate scaled by 1000 fits u64 for any real rate.
        self.btlbw_blocks_per_sec
            .max()
            .map(|rate| (rate * 1000.0).round() as u64)
    }

    /// Numeric phase code for the trace (0 = ProbeBw, 1 = ProbeRtt).
    fn phase_code(&self) -> u64 {
        self.phase.trace_code()
    }
}

/// Carved out of the old `PeerBlockState` so the window math stays unit-testable
/// while the per-peer download state moves into the spawned
/// [`PeerRoutine`](super::peer_routine) (per-peer routines). The routine embeds one of these.
#[derive(Clone, Debug)]
pub(super) struct DownloadWindow {
    pub(super) max_inflight_requests: u32,
    pub(super) outstanding: Vec<OutstandingBlockRange>,
    /// Per-peer BBR-lite estimators + cwnd — the sole congestion controller.
    bbr: BbrState,
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
        Self {
            max_inflight_requests: config.advertised_max_inflight_requests(),
            outstanding: Vec::new(),
            bbr: BbrState::new(config),
            block_liveness_deadline: None,
            last_block_at: None,
        }
    }

    /// Record a completed request into the BBR estimators (RTprop / BtlBw / delivered)
    /// and advance the ProbeRtt phase machine. Call after removing the completed request
    /// from `outstanding`, so `outstanding.len()` is the inflight count the ProbeRtt
    /// drain check needs.
    pub(super) fn record_delivery(&mut self, now: Instant, elapsed: Duration, blocks: u32) {
        let inflight = self.outstanding.len();
        self.bbr.record_delivery(now, elapsed, blocks, inflight);
    }

    /// The effective BBR cwnd in blocks currently applied.
    pub(super) fn bbr_effective_cwnd(&self) -> usize {
        self.bbr.effective_cwnd()
    }

    /// The current RTprop estimate in milliseconds, for tracing.
    pub(super) fn bbr_rtprop_ms(&self) -> Option<u64> {
        self.bbr.rtprop_ms()
    }

    /// The current BtlBw estimate in milli-blocks/sec (blocks/sec × 1000), for tracing.
    pub(super) fn bbr_btlbw_milliblocks(&self) -> Option<u64> {
        self.bbr.btlbw_milliblocks_per_sec()
    }

    /// Total blocks delivered through this peer's completed requests, for tracing.
    pub(super) fn bbr_delivered(&self) -> u64 {
        self.bbr.delivered
    }

    /// The current BBR phase as a numeric code (0 = ProbeBw, 1 = ProbeRtt), for tracing.
    pub(super) fn bbr_phase_code(&self) -> u64 {
        self.bbr.phase_code()
    }

    pub(super) fn available_slots(&self) -> usize {
        // BBR-lite is the sole congestion controller: cap in-flight at the
        // BDP-derived cwnd (clamped to the hard cap), so a peer's queue stays at
        // ~one BDP and head-of-line latency tracks RTprop instead of growing with
        // the byte budget.
        let cwnd = self.bbr.effective_cwnd().min(self.hard_outbound_capacity());
        cwnd.saturating_sub(self.outstanding.len())
    }

    /// Apply the BBR cwnd dip on a real request timeout (one multiplicative dip,
    /// bounded by the minimum cwnd).
    pub(super) fn record_timeout(&mut self) {
        self.bbr.dip_on_timeout();
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

#[cfg(test)]
mod bbr_tests {
    use super::*;

    /// A config with a short ProbeRTT cadence and predictable cwnd math for the unit
    /// tests below. The probe interval/duration are scaled down so a handful of
    /// deliveries crosses a full ProbeBw → ProbeRtt → ProbeBw cycle.
    fn bbr_test_config() -> ZakuraBlockSyncConfig {
        ZakuraBlockSyncConfig {
            bbr_min_cwnd: 4,
            bbr_cwnd_gain_percent: 200,
            bbr_probe_rtt_interval: Duration::from_secs(1),
            bbr_probe_rtt_duration: Duration::from_millis(200),
            bbr_rtprop_window: Duration::from_secs(10),
            bbr_delivery_rate_window: Duration::from_secs(10),
            initial_inflight_requests: 16,
            ..Default::default()
        }
    }

    /// A clean delivery: 40 blocks in 10 ms ⇒ rate 4000 blk/s, RTprop 0.01 s,
    /// BDP 40 blocks, ×2 gain ⇒ cwnd target 80.
    const CLEAN_ELAPSED: Duration = Duration::from_millis(10);
    const CLEAN_BLOCKS: u32 = 40;
    const EXPECTED_CWND: usize = 80;

    #[test]
    fn cwnd_tracks_bdp_after_first_delivery() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();
        // Cold start: the configured initial window until the first BDP sample.
        assert_eq!(bbr.effective_cwnd(), 16);
        bbr.record_delivery(t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert_eq!(bbr.phase, BbrPhase::ProbeBw);
    }

    #[test]
    fn probe_rtt_pins_min_cwnd_then_drains_and_exits() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();

        // Establish a healthy cwnd; anchors the first probe at t0.
        bbr.record_delivery(t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // One interval later, a delivery trips ProbeRtt: cwnd pins to min_cwnd even
        // though the BDP estimate is unchanged.
        let t1 = t0 + Duration::from_millis(1_100);
        bbr.record_delivery(t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);

        // Queue not yet drained (inflight still above min): hold ProbeRtt, no timer.
        let t2 = t1 + Duration::from_millis(50);
        bbr.record_delivery(t2, CLEAN_ELAPSED, 10, min_cwnd + 5);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert!(bbr.probe_rtt_drained_at.is_none());

        // Queue drains to the floor: the hold timer starts here.
        let t3 = t2 + Duration::from_millis(20);
        bbr.record_delivery(t3, CLEAN_ELAPSED, 10, min_cwnd - 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.probe_rtt_drained_at, Some(t3));

        // Before the hold elapses, still draining.
        let t4 = t3 + Duration::from_millis(100);
        bbr.record_delivery(t4, CLEAN_ELAPSED, 10, 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);

        // After probe_rtt_duration past the drain, exit to ProbeBw and restore cwnd.
        let t5 = t3 + Duration::from_millis(200);
        bbr.record_delivery(t5, CLEAN_ELAPSED, 10, 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeBw);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert_eq!(bbr.last_probe_rtt_at, Some(t5));
    }

    #[test]
    fn probe_rtt_collapses_cwnd_for_a_slow_peer() {
        // The headline case: a peer whose RTprop inflated under a deep queue. ProbeRtt
        // forces the cwnd to min_cwnd while it drains, regardless of the (stale, large)
        // BDP estimate — this is the slow-peer collapse the trace analysis motivated.
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        bbr.record_delivery(t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        let t1 = t0 + Duration::from_millis(1_100);
        bbr.record_delivery(t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }

    #[test]
    fn timeout_dip_applies_in_probe_bw_but_is_suppressed_in_probe_rtt() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        bbr.record_delivery(t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // In ProbeBw a timeout dips the cwnd by the multiplicative factor.
        bbr.dip_on_timeout();
        let expected_dip = (EXPECTED_CWND as f64 * BBR_TIMEOUT_DIP).round() as usize;
        assert_eq!(bbr.effective_cwnd(), expected_dip);

        // Enter ProbeRtt; a timeout there is an expected drain consequence, not
        // congestion signal, so cwnd_cap is left untouched.
        let t1 = t0 + Duration::from_millis(1_100);
        bbr.record_delivery(t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        let cap_before = bbr.cwnd_cap;
        bbr.dip_on_timeout();
        assert_eq!(bbr.cwnd_cap, cap_before);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }
}
