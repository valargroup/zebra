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
/// EWMA weight for the smoothed request round-trip the delay-gradient compares against
/// RTprop (higher = more responsive, noisier).
const BBR_DELAY_EWMA_ALPHA: f64 = 0.25;
/// Multiplicative shrink applied to the delay-gradient ceiling on each delivery whose
/// smoothed round-trip exceeds `RTprop × delay_gradient` (queue building).
const BBR_DELAY_CAP_DOWN: f64 = 0.9;

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
    /// The receiving end of the `applyQ`, handed once to the node-side `Committer`
    /// via [`take_apply_queue`](Self::take_apply_queue). The cloneable handle
    /// cannot hold a `Receiver` directly, so it is a take-once slot. `None` for the
    /// inert/handle-less test constructors that never spawn a Sequencer.
    pub(super) apply_queue_rx:
        Arc<StdMutex<Option<mpsc::UnboundedReceiver<super::apply_item::ApplyItem>>>>,
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

    /// Take the receiving end of the `applyQ` to hand to the node-side `Committer`.
    ///
    /// The slot is one-shot: production wiring takes it once, right after the
    /// reactor is spawned, to construct the `Committer`. A second take (or the
    /// inert/handle-less constructors) returns `None`.
    pub fn take_apply_queue(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<super::apply_item::ApplyItem>> {
        self.apply_queue_rx
            .lock()
            .expect("apply queue slot mutex is never poisoned")
            .take()
    }

    /// Inject a durable frontier advance into the Sequencer.
    ///
    /// Called by the node-side durable chain-tip watcher on each `set_finalized_tip`
    /// change; it produces exactly the [`SequencerControlInput::FrontierAdvance`]
    /// the deleted 200 ms checkpoint-frontier poll used to, releasing the held
    /// ledger bytes ≤ the durable tip and advancing the floor. Idempotent with the
    /// endpoint-frontier mirror path via the Sequencer's stale guard. A no-op for
    /// the inert/handle-less constructors that never spawn a Sequencer.
    pub fn report_durable_frontier(&self, frontiers: BlockSyncFrontiers) {
        if let Some(wiring) = self.routine_wiring.as_ref() {
            let _ = wiring.sequencer_control.send(
                super::sequencer_task::SequencerControlInput::FrontierAdvance {
                    frontiers,
                    release_applied: true,
                },
            );
        }
    }

    /// Report a Committer-side commit rejection (consensus-invalid body or apply
    /// timeout) back to the Sequencer.
    ///
    /// The Sequencer rolls the download floor back below the failed height, drops
    /// the body and every successor, and — for [`CommitRejection::Invalid`] —
    /// scores the delivering peer. A no-op for the inert/handle-less constructors
    /// that never spawn a Sequencer (no routine wiring).
    pub fn report_commit_rejected(&self, reset: CommitterReset) {
        if let Some(wiring) = self.routine_wiring.as_ref() {
            let _ = wiring
                .sequencer_control
                .send(super::sequencer_task::SequencerControlInput::CommitRejected(reset));
        }
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
    pub(super) work: Arc<WorkQueue>,
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
            work: Arc::new(WorkQueue::new(startup.frontiers.verified_block_tip)),
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
    /// Unit the cwnd/BtlBw/`delivered` are denominated in. `Blocks` keeps the
    /// request-counting controller (the A/B baseline); `Bytes` makes the controller
    /// reason in header-hinted body bytes so the in-flight request count falls out as
    /// `cwnd_bytes / advertised_block_size`.
    unit: CwndUnit,
    cwnd_gain: f64,
    /// Minimum / cold-start cwnd, in the active unit (`bbr_min_cwnd` blocks or
    /// `bbr_min_cwnd_bytes` bytes).
    min_cwnd: usize,
    startup_cwnd: usize,
    rtprop_window: Duration,
    delivery_rate_window: Duration,
    /// How long between ProbeRTT drains (the cadence at which RTprop is refreshed).
    probe_rtt_interval: Duration,
    /// How long to hold the cwnd at `min_cwnd` once the queue has drained, so at
    /// least one uncontended request completes and yields a clean RTprop sample.
    probe_rtt_duration: Duration,
    /// Smoothed-RTT / RTprop ratio above which the queue is judged to be building and
    /// the delay-gradient ceiling ratchets the cwnd down (e.g. 1.5 = shrink once the
    /// recent round-trip runs 50% over the uncontended minimum).
    delay_gradient: f64,
}

impl BbrParams {
    fn from_config(config: &ZakuraBlockSyncConfig) -> Self {
        let (min_cwnd, startup_cwnd) = match config.bbr_cwnd_unit {
            CwndUnit::Blocks => {
                let min = usize::try_from(config.bbr_min_cwnd).unwrap_or(1).max(1);
                // Cold start opens at the configured initial window until the first
                // BDP sample.
                let startup = usize::try_from(config.initial_inflight_requests)
                    .unwrap_or(min)
                    .max(min);
                (min, startup)
            }
            CwndUnit::Bytes => {
                // Byte denomination: the floor (and cold-start window) is the
                // configured minimum byte cwnd. The BDP estimate takes over once the
                // first delivery sample arrives; until then `bbr_min_cwnd_bytes`
                // primes the pipe with a few bodies' worth of in-flight budget.
                let min = usize::try_from(config.bbr_min_cwnd_bytes)
                    .unwrap_or(usize::MAX)
                    .max(1);
                (min, min)
            }
        };
        Self {
            unit: config.bbr_cwnd_unit,
            cwnd_gain: f64::from(config.bbr_cwnd_gain_percent) / 100.0,
            min_cwnd,
            startup_cwnd,
            rtprop_window: config.bbr_rtprop_window,
            delivery_rate_window: config.bbr_delivery_rate_window,
            probe_rtt_interval: config.bbr_probe_rtt_interval,
            probe_rtt_duration: config.bbr_probe_rtt_duration,
            delay_gradient: f64::from(config.bbr_delay_gradient_percent.max(100)) / 100.0,
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
    /// Windowed-min of the **raw request round-trip** (seconds) — the BDP's RTprop term
    /// (`bdp = BtlBw × RTprop`) under both units: the genuine fastest observed round trip,
    /// which never collapses to zero. (The earlier byte-unit model fed this the
    /// *size-residual* `elapsed − bytes/BtlBw`; on a high-BtlBw carrier the fastest
    /// delivery's residual is ≈0, which zeroed the BDP and pinned the cwnd at the floor.
    /// The size-residual now lives in [`rtprop_residual_secs`](Self::rtprop_residual_secs),
    /// used only by the size-aware delay gate.)
    rtprop_secs: WindowedSamples,
    /// Windowed-min of the **size-residual** round-trip (`elapsed − bytes/BtlBw` under
    /// `Bytes`; the raw round-trip under `Blocks`) — the transmission-stripped propagation
    /// latency. Used **only** as the delay gate's healthy-round-trip base, so a big block's
    /// honest transfer time is not mistaken for a standing queue. It never feeds the BDP,
    /// which must reflect real in-flight depth rather than a residual that collapses to ~0
    /// on a fast carrier.
    rtprop_residual_secs: WindowedSamples,
    /// Max-filter over per-ack delivery rate, in **units per second** (blocks/s under
    /// [`CwndUnit::Blocks`], bytes/s under [`CwndUnit::Bytes`]). The byte denomination
    /// makes `BtlBw × RTprop` a true bandwidth-delay product over heterogeneous body
    /// sizes; the block denomination is the A/B baseline.
    btlbw_per_sec: WindowedSamples,
    /// Cumulative delivered amount in the active unit (blocks or bytes), used as the
    /// per-ack delivery-rate numerator via [`DeliverySnapshot`].
    delivered: u64,
    delivered_at: Option<Instant>,
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
    /// EWMA of the request round-trip, compared against RTprop by the delay-gradient.
    smoothed_elapsed_secs: Option<f64>,
    /// Delay-gradient ceiling on the effective cwnd. Starts unbounded (`usize::MAX`) so
    /// it never limits an uncongested peer; ratchets down toward the true operating
    /// point whenever the smoothed round-trip rises above `RTprop × delay_gradient`, and
    /// relaxes back up when the queue clears. Guards against a `BtlBw × RTprop` BDP that
    /// overshoots the sustainable rate (max-rate and min-RTT can come from different
    /// samples under variable queueing), which would otherwise inflate the cwnd.
    delay_cap: usize,
}

impl BbrState {
    fn new(config: &ZakuraBlockSyncConfig) -> Self {
        let params = BbrParams::from_config(config);
        Self {
            rtprop_secs: WindowedSamples::new(params.rtprop_window),
            rtprop_residual_secs: WindowedSamples::new(params.rtprop_window),
            btlbw_per_sec: WindowedSamples::new(params.delivery_rate_window),
            delivered: 0,
            delivered_at: None,
            cwnd_cap: params.startup_cwnd,
            phase: BbrPhase::ProbeBw,
            last_probe_rtt_at: None,
            probe_rtt_drained_at: None,
            smoothed_elapsed_secs: None,
            delay_cap: usize::MAX,
            params,
        }
    }

    fn delivery_snapshot(&self, now: Instant) -> DeliverySnapshot {
        DeliverySnapshot {
            delivered: self.delivered,
            delivered_at: self.delivered_at.unwrap_or(now),
        }
    }

    /// Record a completed request: `elapsed` from send to the final body, `blocks` in
    /// it, and `inflight` = requests still outstanding to this peer *after* this
    /// completion. The RTprop sample is the request round-trip. The BtlBw sample is
    /// measured over the request's pipe interval (`delivered_delta / elapsed_since_snapshot`),
    /// so one-block responses can still observe concurrent completions while the request
    /// was in flight. The interval is floored at the previous RTprop so a burst of
    /// buffered bodies arriving within one tick cannot inflate the bandwidth estimate.
    /// Re-derives the applied cwnd from the fresh BDP estimate, then advances the
    /// ProbeBw/ProbeRtt phase machine.
    fn record_delivery(
        &mut self,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        delivered_bytes: u64,
        inflight: usize,
        snapshot: DeliverySnapshot,
    ) {
        let secs = elapsed.as_secs_f64();
        // Floor the delivery-rate interval at the *previous* RTprop min (captured
        // before this sample is observed) so a burst of buffered bodies arriving within
        // one tick cannot inflate the bandwidth estimate.
        let rate_floor = self.rtprop_secs.min().unwrap_or(secs).max(1e-4);

        // Accumulate the delivered amount in the active unit and push a per-ack rate
        // sample into the BtlBw max-filter (blocks/s under `Blocks`, bytes/s under
        // `Bytes`).
        let delivered_amount = match self.params.unit {
            CwndUnit::Blocks => u64::from(blocks),
            CwndUnit::Bytes => delivered_bytes,
        };
        let delivered_after = self.delivered.saturating_add(delivered_amount);
        let delivered_delta = delivered_after.saturating_sub(snapshot.delivered).max(1);
        let interval = now.saturating_duration_since(snapshot.delivered_at);
        // `delivered_delta` is a count/byte total over a short sampling window;
        // converting it to `f64` is exact for the operating ranges this controller sees.
        let rate = delivered_delta as f64 / interval.as_secs_f64().max(rate_floor);
        self.btlbw_per_sec.observe(now, rate);
        self.delivered = delivered_after;
        self.delivered_at = Some(now);

        // Observe the BDP's RTprop sample: the **raw** round trip under both units. Its
        // windowed min ≈ the base round trip of the fastest deliveries, which is the real
        // in-flight depth the BDP needs. Feeding the BDP the size residual instead would
        // collapse it to ~0 on a high-BtlBw carrier (the fastest delivery's residual
        // `elapsed − bytes/BtlBw` ≈ 0), pinning the cwnd at the floor.
        self.rtprop_secs.observe(now, secs);
        // Observe the size-residual separately, for the delay gate only: under `Bytes` it
        // strips the body's transmission time so a big block's honest transfer is not read
        // as a standing queue; under `Blocks` it is the raw round trip (A/B baseline).
        let residual_sample = match self.params.unit {
            CwndUnit::Blocks => secs,
            CwndUnit::Bytes => self.size_residual_rtprop(secs, delivered_bytes),
        };
        self.rtprop_residual_secs.observe(now, residual_sample);

        if let Some(target) = self.cwnd_target() {
            self.cwnd_cap = target;
        }
        // Delay-gradient runs in ProbeBw only: the drained round-trips ProbeRtt produces
        // are artificially short and would spuriously relax the ceiling. `phase` here is
        // still the pre-`advance_phase` value, so a tick that flips into ProbeRtt this
        // call last updated the ceiling under genuine ProbeBw conditions.
        if self.phase == BbrPhase::ProbeBw {
            self.update_delay_cap(secs, delivered_bytes);
        }
        self.advance_phase(now, inflight);
    }

    /// Size-residual RTprop sample (`Bytes` unit): subtract the body's transmission
    /// time at the bottleneck rate from the round trip, leaving the fixed-latency
    /// component. Falls back to the raw round trip before any rate is known, and is
    /// clamped to `[ε, elapsed]` (the residual can never exceed the time elapsed, and a
    /// tiny positive floor keeps the byte-BDP well-defined).
    fn size_residual_rtprop(&self, secs: f64, delivered_bytes: u64) -> f64 {
        let btlbw = self.btlbw_per_sec.max().unwrap_or(0.0);
        let residual = if btlbw > 0.0 {
            // `delivered_bytes as f64` is exact for real body sizes.
            secs - delivered_bytes as f64 / btlbw
        } else {
            secs
        };
        residual.clamp(1e-4, secs.max(1e-4))
    }

    /// Update the delay-gradient ceiling from this delivery's round-trip. When the
    /// smoothed round-trip rises above `RTprop × delay_gradient` the queue is building,
    /// so ratchet the ceiling down from the current operating cwnd; otherwise relax it
    /// back up so a cleared queue lets the cwnd re-probe for bandwidth.
    fn update_delay_cap(&mut self, secs: f64, delivered_bytes: u64) {
        let smoothed = match self.smoothed_elapsed_secs {
            Some(prev) => prev * (1.0 - BBR_DELAY_EWMA_ALPHA) + secs * BBR_DELAY_EWMA_ALPHA,
            None => secs,
        };
        self.smoothed_elapsed_secs = Some(smoothed);
        // The delay gate's base is the *residual* RTprop (transmission stripped), not the
        // raw round trip the BDP uses: the size-aware `expected` below adds the body's
        // transmission back, so basing it on the raw round trip would double-count it.
        let rtprop = self.rtprop_residual_secs.min().unwrap_or(secs).max(1e-4);
        // The expected round trip for a healthy (unqueued) delivery. Under `Bytes` it is
        // size-aware — `RTprop + transmission time` — so a big block's honest transfer
        // time is not mistaken for a standing queue; under `Blocks` it is just RTprop
        // (the A/B baseline).
        let expected = match self.params.unit {
            CwndUnit::Blocks => rtprop,
            CwndUnit::Bytes => {
                let btlbw = self.btlbw_per_sec.max().unwrap_or(0.0);
                let transmit = if btlbw > 0.0 {
                    delivered_bytes as f64 / btlbw
                } else {
                    0.0
                };
                rtprop + transmit
            }
        };
        if smoothed > expected * self.params.delay_gradient {
            // Queue building: shrink the ceiling relative to the current operating cwnd.
            let operating = self.cwnd_cap.min(self.delay_cap).max(self.params.min_cwnd);
            let shrunk = (operating as f64 * BBR_DELAY_CAP_DOWN).round();
            // A non-negative product of a usize and 0.9; the cast is safe.
            let shrunk = if shrunk.is_finite() && shrunk >= 0.0 {
                shrunk as usize
            } else {
                self.params.min_cwnd
            };
            self.delay_cap = shrunk.max(self.params.min_cwnd);
        } else {
            // Headroom: relax the ceiling up (~12%/delivery), saturating so an
            // uncongested peer's ceiling stays effectively unbounded.
            let grow = (self.delay_cap / 8).max(1);
            self.delay_cap = self.delay_cap.saturating_add(grow);
        }
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
    /// ProbeRtt the cwnd is pinned to `min_cwnd` to drain the queue; in ProbeBw it is the
    /// BDP-derived cwnd capped by the delay-gradient ceiling.
    fn effective_cwnd(&self) -> usize {
        match self.phase {
            BbrPhase::ProbeRtt => self.params.min_cwnd,
            BbrPhase::ProbeBw => self.cwnd_cap.min(self.delay_cap).max(self.params.min_cwnd),
        }
    }

    /// Apply one multiplicative dip on a real timeout (BBR-style), bounded by the
    /// minimum cwnd. Does not run the cubic backoff ladder. Suppressed during ProbeRtt,
    /// where the cwnd is already pinned to `min_cwnd` and timeouts are an expected
    /// consequence of the drain, not congestion signal. A timeout is strong congestion
    /// evidence, so it also ratchets the delay-gradient ceiling down to the dipped cwnd.
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
        self.delay_cap = self.delay_cap.min(self.cwnd_cap);
    }

    /// Bandwidth-delay product in the active unit: BtlBw (units/s) × RTprop (s) — blocks
    /// under `Blocks`, bytes under `Bytes`. `None` until at least one delivery sample
    /// exists (cold start).
    fn bdp(&self) -> Option<f64> {
        match (self.btlbw_per_sec.max(), self.rtprop_secs.min()) {
            (Some(rate), Some(rtprop)) => Some(rate * rtprop),
            _ => None,
        }
    }

    /// Target cwnd in the active unit = `max(min_cwnd, BDP × gain)`. `None` until the
    /// first delivery sample exists, so the cwnd stays at the cold-start value until then.
    fn cwnd_target(&self) -> Option<usize> {
        let bdp = self.bdp()?;
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

    /// Raw BtlBw max-filter value in the active unit per second (`None` cold-start).
    fn btlbw_units_per_sec(&self) -> Option<f64> {
        self.btlbw_per_sec.max()
    }

    fn btlbw_milliblocks_per_sec(&self) -> Option<u64> {
        // A rounded non-negative rate scaled by 1000 fits u64 for any real rate. Only
        // meaningful under `Blocks`; the byte trace path reports bytes/sec instead.
        self.btlbw_per_sec
            .max()
            .map(|rate| (rate * 1000.0).round() as u64)
    }

    /// Numeric phase code for the trace (0 = ProbeBw, 1 = ProbeRtt).
    fn phase_code(&self) -> u64 {
        self.phase.trace_code()
    }

    /// The smoothed request round-trip in milliseconds, for tracing the delay-gradient.
    fn smoothed_elapsed_ms(&self) -> Option<u64> {
        // A rounded non-negative round-trip in milliseconds fits u64 for any real RTT.
        self.smoothed_elapsed_secs
            .map(|secs| (secs * 1000.0).round() as u64)
    }

    /// The delay-gradient ceiling in blocks once it has bound the cwnd (`None` while
    /// still unbounded), for tracing.
    fn delay_cap(&self) -> Option<usize> {
        (self.delay_cap != usize::MAX).then_some(self.delay_cap)
    }
}

/// Carved out of the old `PeerBlockState` so the window math stays unit-testable
/// while the per-peer download state moves into the spawned
/// [`PeerRoutine`](super::peer_routine) (per-peer routines). The routine embeds one of these.
#[derive(Clone, Debug)]
pub(super) struct DownloadWindow {
    pub(super) max_inflight_requests: u32,
    pub(super) outstanding: Vec<OutstandingBlockRange>,
    /// Per-peer BBR-lite estimators + cwnd — the sole congestion controller. Under
    /// [`CwndUnit::Bytes`] the cwnd is itself a byte budget sourced from header size
    /// hints (no fixed per-request byte weight), so there is no `nominal_request_bytes`.
    bbr: BbrState,
    /// Whether the cwnd budgets outstanding work in request slots or reserved bytes.
    cwnd_unit: CwndUnit,
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
            cwnd_unit: config.bbr_cwnd_unit,
            block_liveness_deadline: None,
            last_block_at: None,
        }
    }

    pub(super) fn delivery_snapshot(&self, now: Instant) -> DeliverySnapshot {
        self.bbr.delivery_snapshot(now)
    }

    /// Record a completed request into the BBR estimators (RTprop / BtlBw / delivered)
    /// and advance the ProbeRtt phase machine. `delivered_bytes` is the request's total
    /// delivered body bytes — under the single-block-per-request invariant
    /// (`DEFAULT_BS_BLOCKS_PER_RESPONSE = 1`) this is the completing body's
    /// `serialized_bytes`. Call after removing the completed request from `outstanding`,
    /// so `outstanding.len()` is the inflight count the ProbeRtt drain check needs.
    pub(super) fn record_delivery(
        &mut self,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        delivered_bytes: u64,
        snapshot: DeliverySnapshot,
    ) {
        let inflight = self.outstanding.len();
        self.bbr
            .record_delivery(now, elapsed, blocks, delivered_bytes, inflight, snapshot);
    }

    /// The effective BBR cwnd as a **request count**, for diagnostics that compare
    /// against the request-count hard cap (the periodic slot trace, cross-peer floor
    /// bias). Under `Blocks` this is the cwnd directly; under `Bytes` it is the byte
    /// cwnd divided by a representative body size, so it reads as "requests this peer's
    /// byte window admits". The byte cwnd itself is available via
    /// [`bbr_effective_cwnd_bytes`](Self::bbr_effective_cwnd_bytes).
    pub(super) fn bbr_effective_cwnd(&self) -> usize {
        match self.cwnd_unit {
            CwndUnit::Blocks => self.bbr.effective_cwnd(),
            CwndUnit::Bytes => {
                let cwnd_bytes = self.bbr.effective_cwnd() as u64;
                let rep = self.representative_body_bytes();
                usize::try_from((cwnd_bytes / rep.max(1)).max(1)).unwrap_or(usize::MAX)
            }
        }
    }

    /// The effective byte cwnd under `Bytes` (`None` under `Blocks`), for tracing.
    pub(super) fn bbr_effective_cwnd_bytes(&self) -> Option<u64> {
        matches!(self.cwnd_unit, CwndUnit::Bytes).then(|| self.bbr.effective_cwnd() as u64)
    }

    /// A representative body size in bytes for converting a byte cwnd into a request
    /// count: the mean reserved bytes across in-flight requests, falling back to the
    /// per-block worst case when nothing is outstanding. Used only for diagnostics and
    /// the floor-bypass byte bonus, never for admission.
    fn representative_body_bytes(&self) -> u64 {
        let outstanding = self.outstanding.len() as u64;
        if outstanding == 0 {
            return block::MAX_BLOCK_BYTES;
        }
        (self.outstanding_reserved_bytes() / outstanding).max(1)
    }

    /// The current RTprop estimate in milliseconds, for tracing.
    pub(super) fn bbr_rtprop_ms(&self) -> Option<u64> {
        self.bbr.rtprop_ms()
    }

    /// The current BtlBw estimate in milli-blocks/sec (blocks/sec × 1000), for tracing.
    /// `None` under `Bytes`, where [`bbr_btlbw_bytes_per_sec`](Self::bbr_btlbw_bytes_per_sec)
    /// is the meaningful rate.
    pub(super) fn bbr_btlbw_milliblocks(&self) -> Option<u64> {
        matches!(self.cwnd_unit, CwndUnit::Blocks)
            .then(|| self.bbr.btlbw_milliblocks_per_sec())
            .flatten()
    }

    /// The current BtlBw estimate in bytes/sec under `Bytes` (`None` under `Blocks`).
    pub(super) fn bbr_btlbw_bytes_per_sec(&self) -> Option<u64> {
        if !matches!(self.cwnd_unit, CwndUnit::Bytes) {
            return None;
        }
        self.bbr
            .btlbw_units_per_sec()
            // A non-negative finite bytes/sec rate rounds into u64 for any real link.
            .map(|rate| rate.round() as u64)
    }

    /// Bytes reserved across this peer's in-flight requests, for tracing the byte window
    /// occupancy.
    pub(super) fn bbr_inflight_bytes(&self) -> u64 {
        self.outstanding_reserved_bytes()
    }

    /// Total delivered through this peer's completed requests, for tracing — blocks
    /// under `Blocks`, bytes under `Bytes`.
    pub(super) fn bbr_delivered(&self) -> u64 {
        self.bbr.delivered
    }

    /// The current BBR phase as a numeric code (0 = ProbeBw, 1 = ProbeRtt), for tracing.
    pub(super) fn bbr_phase_code(&self) -> u64 {
        self.bbr.phase_code()
    }

    /// The smoothed request round-trip in milliseconds the delay-gradient tracks.
    pub(super) fn bbr_smoothed_elapsed_ms(&self) -> Option<u64> {
        self.bbr.smoothed_elapsed_ms()
    }

    /// The delay-gradient cwnd ceiling in blocks once it binds (`None` while unbounded).
    pub(super) fn bbr_delay_cap(&self) -> Option<u64> {
        self.bbr
            .delay_cap()
            .map(|cap| u64::try_from(cap).unwrap_or(u64::MAX))
    }

    pub(super) fn available_slots(&self) -> usize {
        self.available_slots_with_bonus(0)
    }

    /// Available headroom allowing `bonus` extra in-flight requests beyond the BBR cwnd,
    /// still clamped to the peer's advertised hard cap. `bonus == 0` is the normal
    /// (above-floor) capacity used by [`available_slots`]; a small positive `bonus` is
    /// the floor bypass — it lets the lowest missing height be fetched even when the
    /// peer is saturated at its cwnd, without ever exceeding the advertised inflight.
    ///
    /// The return value is non-zero exactly when there is room for at least one more
    /// request; callers use it as a gate, not an absolute count. Under
    /// [`CwndUnit::Bytes`] the cwnd is itself a byte budget (`BtlBw_bytes × RTprop ×
    /// gain`, from header size hints) compared against reserved body bytes, so a peer
    /// serving large bodies holds fewer in flight and a peer serving small bodies holds
    /// many — the in-flight *request* count falls out of `cwnd_bytes / body_size`. The
    /// controller is unit-agnostic; only this comparison differs — the seam that makes
    /// switching units a small change.
    pub(super) fn available_slots_with_bonus(&self, bonus: usize) -> usize {
        // BBR-lite is the sole congestion controller: cap in-flight at the BDP-derived
        // cwnd so a peer's queue stays at ~one BDP and head-of-line latency tracks
        // RTprop. The floor bypass adds `bonus` on top.
        let hard_cap = self.hard_outbound_capacity();
        match self.cwnd_unit {
            CwndUnit::Blocks => {
                let cwnd_slots = self
                    .bbr
                    .effective_cwnd()
                    .saturating_add(bonus)
                    .min(hard_cap);
                cwnd_slots.saturating_sub(self.outstanding.len())
            }
            CwndUnit::Bytes => {
                // The peer's advertised request-count cap still binds in byte mode: a peer
                // serving tiny bodies must never be issued more in-flight *requests* than it
                // advertised it will service, however much byte headroom the cwnd still
                // shows. Once the request count reaches the hard cap there is no slot,
                // regardless of bytes — mirroring the blocks-unit ceiling (review fix F2).
                let outstanding = self.outstanding.len();
                if outstanding >= hard_cap {
                    return 0;
                }
                // The cwnd is already a byte budget. The floor bypass grants `bonus`
                // *representative* bodies of extra byte headroom — sized to the recent
                // per-request reservation, NOT the 2 MB worst case — so a starved floor
                // can still be fetched when the byte window is full without ballooning
                // the in-flight bytes far past the cwnd (which would defeat the byte
                // denomination's head-of-line bound). The take is still count-capped to
                // one block and passes the real `ByteBudget` reservation.
                let reserved = self.outstanding_reserved_bytes();
                let representative = if outstanding == 0 {
                    block::MAX_BLOCK_BYTES
                } else {
                    // A non-empty in-flight set: the mean reserved bytes per request.
                    (reserved / outstanding as u64).max(1)
                };
                let bonus_bytes = (bonus as u64).saturating_mul(representative);
                let cwnd_bytes = (self.bbr.effective_cwnd() as u64).saturating_add(bonus_bytes);
                usize::try_from(cwnd_bytes.saturating_sub(reserved)).unwrap_or(usize::MAX)
            }
        }
    }

    /// Bytes reserved across this peer's in-flight requests (the per-request size
    /// estimates of heights not yet received). Recomputed on demand — the byte unit is
    /// experimental; a hot path would maintain a running counter instead.
    fn outstanding_reserved_bytes(&self) -> u64 {
        self.outstanding.iter().fold(0u64, |acc, range| {
            acc.saturating_add(range.reserved_bytes())
        })
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
    /// our Status). The previous `unsolicited` meter was dual-use; its inbound-status
    /// *reply* half moved to the routine's `status_reply_meter`. This half stays
    /// reactor-side because the reactor owns serving-tip advertisement.
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
    pub(super) delivery_snapshot: DeliverySnapshot,
    pub(super) received: ReceivedBlockTracker,
}

#[derive(Copy, Clone, Debug)]
pub(super) struct DeliverySnapshot {
    pub(super) delivered: u64,
    pub(super) delivered_at: Instant,
}

impl OutstandingBlockRange {
    /// Bytes still reserved for this request: the sum of the per-height size
    /// estimates for every requested height not yet received. Each received body
    /// shrinks its estimate toward the actual size, so releasing this (on
    /// timeout/disconnect/short response) never over-releases bytes already handed
    /// to the reorder buffer.
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

    /// Record `blocks` committed bodies totalling `bytes`, for attributing a
    /// durable frontier advance that makes several held heights durable at once.
    pub(super) fn record_n(&mut self, blocks: u64, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.blocks = self.blocks.saturating_add(blocks);
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
            // These tests assert blocks-slot semantics; pin the unit so the production
            // default flip to `Bytes` does not change them.
            bbr_cwnd_unit: CwndUnit::Blocks,
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

    /// Blocks-mode delivery helper (the `delivered_bytes` arg is ignored under
    /// `CwndUnit::Blocks`, so it passes 0).
    fn record_delivery(
        bbr: &mut BbrState,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        inflight: usize,
    ) {
        let snapshot = DeliverySnapshot {
            delivered: bbr.delivered,
            delivered_at: now - elapsed,
        };
        bbr.record_delivery(now, elapsed, blocks, 0, inflight, snapshot);
    }

    #[test]
    fn cwnd_tracks_bdp_after_first_delivery() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();
        // Cold start: the configured initial window until the first BDP sample.
        assert_eq!(bbr.effective_cwnd(), 16);
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert_eq!(bbr.phase, BbrPhase::ProbeBw);
    }

    #[test]
    fn one_block_responses_observe_pipe_delivery_rate() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();
        let rtprop = Duration::from_millis(100);
        let sent_at = t0 - rtprop;
        let snapshots: Vec<_> = (0..16).map(|_| bbr.delivery_snapshot(sent_at)).collect();

        for snapshot in snapshots {
            bbr.record_delivery(t0, rtprop, 1, 0, 16, snapshot);
        }

        // Sixteen one-block responses completed during the same request interval:
        // BtlBw = 16 / 100 ms, BDP = 16, cwnd gain = 2.
        assert_eq!(bbr.effective_cwnd(), 32);
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(160_000));
    }

    #[test]
    fn delivery_rate_floor_uses_previous_rtprop_sample() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();

        // Establish a 100 ms RTprop and 100 blocks/s BtlBw sample.
        record_delivery(&mut bbr, t0, Duration::from_millis(100), 10, 10);
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(100_000));

        // A later 1 ms request is also the new RTprop, but it must not remove the
        // floor for its own delivery-rate sample. With the old ordering this sample
        // was 10 / 1 ms = 10_000 blocks/s and inflated BtlBw by 100x.
        record_delivery(
            &mut bbr,
            t0 + Duration::from_millis(10),
            Duration::from_millis(1),
            10,
            10,
        );
        assert_eq!(bbr.rtprop_ms(), Some(1));
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(100_000));
    }

    #[test]
    fn probe_rtt_pins_min_cwnd_then_drains_and_exits() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();

        // Establish a healthy cwnd; anchors the first probe at t0.
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // One interval later, a delivery trips ProbeRtt: cwnd pins to min_cwnd even
        // though the BDP estimate is unchanged.
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);

        // Queue not yet drained (inflight still above min): hold ProbeRtt, no timer.
        let t2 = t1 + Duration::from_millis(50);
        record_delivery(&mut bbr, t2, CLEAN_ELAPSED, 10, min_cwnd + 5);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert!(bbr.probe_rtt_drained_at.is_none());

        // Queue drains to the floor: the hold timer starts here.
        let t3 = t2 + Duration::from_millis(20);
        record_delivery(&mut bbr, t3, CLEAN_ELAPSED, 10, min_cwnd - 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.probe_rtt_drained_at, Some(t3));

        // Before the hold elapses, still draining.
        let t4 = t3 + Duration::from_millis(100);
        record_delivery(&mut bbr, t4, CLEAN_ELAPSED, 10, 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);

        // After probe_rtt_duration past the drain, exit to ProbeBw and restore cwnd.
        let t5 = t3 + Duration::from_millis(200);
        record_delivery(&mut bbr, t5, CLEAN_ELAPSED, 10, 1);
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
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }

    #[test]
    fn timeout_dip_applies_in_probe_bw_but_is_suppressed_in_probe_rtt() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // In ProbeBw a timeout dips the cwnd by the multiplicative factor.
        bbr.dip_on_timeout();
        let expected_dip = (EXPECTED_CWND as f64 * BBR_TIMEOUT_DIP).round() as usize;
        assert_eq!(bbr.effective_cwnd(), expected_dip);

        // Enter ProbeRtt; a timeout there is an expected drain consequence, not
        // congestion signal, so cwnd_cap is left untouched.
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        let cap_before = bbr.cwnd_cap;
        bbr.dip_on_timeout();
        assert_eq!(bbr.cwnd_cap, cap_before);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }

    /// Push `n` placeholder outstanding requests onto a window to drive its slot count.
    fn fill_outstanding(window: &mut DownloadWindow, n: usize) {
        let now = Instant::now();
        for _ in 0..n {
            window.outstanding.push(OutstandingBlockRange {
                request: BlockRangeRequest {
                    start_height: block::Height(0),
                    count: 1,
                    anchor_hash: block::Hash([0; 32]),
                    estimated_bytes: 0,
                    expected_blocks: Vec::new(),
                },
                queued_at: now,
                deadline: now,
                delivery_snapshot: window.delivery_snapshot(now),
                received: ReceivedBlockTracker::default(),
            });
        }
    }

    #[test]
    fn floor_bypass_grants_bonus_slots_only_when_cwnd_is_saturated() {
        // Cold-start cwnd 8, hard cap well above it so the bonus is not clamped.
        let cfg = ZakuraBlockSyncConfig {
            initial_inflight_requests: 8,
            max_inflight_requests: 256,
            ..bbr_test_config()
        };
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.bbr_effective_cwnd(), 8);

        // Below cwnd: normal capacity already covers the floor, bonus adds nothing extra
        // beyond the same headroom.
        fill_outstanding(&mut window, 6);
        assert_eq!(window.available_slots(), 2);
        assert_eq!(window.available_slots_with_bonus(2), 4);

        // Saturated at cwnd: normal capacity is 0 but the floor may borrow the bonus.
        fill_outstanding(&mut window, 2);
        assert_eq!(window.available_slots(), 0);
        assert_eq!(window.available_slots_with_bonus(2), 2);

        // Saturated even into the bonus region: nothing left for anyone.
        fill_outstanding(&mut window, 2);
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }

    #[test]
    fn delay_gradient_does_not_bind_an_uncongested_peer() {
        // Every delivery's round-trip equals RTprop (no queue), so the delay ceiling
        // stays unbounded and the cwnd tracks the full BDP target.
        let mut bbr = BbrState::new(&bbr_test_config());
        let mut now = Instant::now();
        for _ in 0..20 {
            record_delivery(&mut bbr, now, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
            now += Duration::from_millis(5);
        }
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert!(bbr.delay_cap().is_none(), "ceiling should stay unbounded");
    }

    #[test]
    fn delay_gradient_caps_cwnd_when_the_round_trip_inflates() {
        // RTprop is established low (10 ms), then every round-trip runs far above it
        // (queue building) while the BtlBw×RTprop target stays high — exactly the cwnd
        // overshoot the delay-gradient must contain. The ceiling ratchets the effective
        // cwnd well below the (inflated) BDP target.
        let cfg = bbr_test_config();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        // One clean delivery anchors RTprop at 10 ms and the BDP target at 80.
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // Now deliveries keep arriving at the same low RTprop sample for the min-filter
        // (so the target stays 80) but with long *smoothed* round-trips — model that with
        // a low-elapsed sample to hold RTprop and the cwnd target, interleaved with the
        // queue signal. Here we simply feed inflated round-trips: RTprop min stays 10 ms
        // (the first sample is in-window), smoothed climbs, the ceiling ratchets down.
        let inflated = Duration::from_millis(120);
        let mut now = t0;
        for _ in 0..40 {
            now += Duration::from_millis(5);
            record_delivery(&mut bbr, now, inflated, CLEAN_BLOCKS, 50);
        }
        assert_eq!(
            bbr.phase,
            BbrPhase::ProbeBw,
            "stay in ProbeBw for this test"
        );
        assert!(
            bbr.effective_cwnd() < EXPECTED_CWND,
            "delay-gradient should cap the cwnd below the BDP target, got {}",
            bbr.effective_cwnd(),
        );
        assert!(
            bbr.delay_cap().is_some(),
            "the ceiling should have bound the cwnd",
        );
    }

    /// Push `count` single-height requests each reserving `bytes_each` estimated bytes.
    fn push_outstanding_bytes(window: &mut DownloadWindow, count: usize, bytes_each: u64) {
        let now = Instant::now();
        for i in 0..count {
            // A `u32` index; the test count is tiny so the cast is safe.
            let height = block::Height(1 + i as u32);
            window.outstanding.push(OutstandingBlockRange {
                request: BlockRangeRequest {
                    start_height: height,
                    count: 1,
                    anchor_hash: block::Hash([0; 32]),
                    estimated_bytes: bytes_each,
                    expected_blocks: vec![ExpectedBlock {
                        height,
                        hash: block::Hash([0; 32]),
                        estimated_bytes: bytes_each,
                    }],
                },
                queued_at: now,
                deadline: now,
                delivery_snapshot: window.delivery_snapshot(now),
                received: ReceivedBlockTracker::default(),
            });
        }
    }

    /// A byte-unit config whose cold-start byte cwnd is exactly `min_cwnd_bytes` (the
    /// floor doubles as the cold-start window) with a request-count cap well above it.
    fn byte_test_config(min_cwnd_bytes: u64, max_inflight: u32) -> ZakuraBlockSyncConfig {
        ZakuraBlockSyncConfig {
            bbr_cwnd_unit: CwndUnit::Bytes,
            bbr_min_cwnd_bytes: min_cwnd_bytes,
            max_inflight_requests: max_inflight,
            ..bbr_test_config()
        }
    }

    #[test]
    fn cwnd_unit_bytes_budgets_in_flight_by_reserved_bytes() {
        // The byte cwnd is the byte floor at cold start: an 8000 B in-flight budget,
        // sourced from the controller's byte denomination — independent of how many
        // *requests* that is.
        let cfg = byte_test_config(8000, 256);
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.available_slots(), 8000);

        // Six 1000 B requests (their header-hinted `estimated_bytes`) leave 2000 B...
        push_outstanding_bytes(&mut window, 6, 1000);
        assert_eq!(window.available_slots(), 2000);
        // ...and two more exhaust the byte budget.
        push_outstanding_bytes(&mut window, 2, 1000);
        assert_eq!(window.available_slots(), 0);

        // A peer serving 4 KB bodies fills the same byte cwnd with far fewer requests —
        // the point of the byte unit. Two 4000 B requests already saturate the 8000 B
        // budget, so the in-flight request count self-adjusts to the body size.
        let mut big = DownloadWindow::new(&cfg);
        push_outstanding_bytes(&mut big, 2, 4000);
        assert_eq!(big.available_slots(), 0);
    }

    #[test]
    fn cwnd_unit_bytes_enforces_the_request_count_hard_cap() {
        // A peer advertising a small inflight cap but serving tiny bodies must not be
        // issued more *requests* than it will service, however much byte headroom the
        // cwnd still shows — the advertised request-count cap binds first (review fix F2).
        let cfg = byte_test_config(400_000, 4); // 400 KB byte cwnd, hard cap 4 requests
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.hard_outbound_capacity(), 4);
        // 400_000 B of byte headroom — room for many tiny bodies.
        assert!(window.available_slots() > 0);

        // Four tiny (10 B) requests reach the request-count hard cap. The byte budget is
        // nowhere near exhausted (40 B of 400_000 B), but the advertised cap must bind:
        // no further request may be issued.
        push_outstanding_bytes(&mut window, 4, 10);
        assert_eq!(
            window.available_slots(),
            0,
            "the advertised request-count cap must bind even with byte headroom left",
        );
        // The floor bypass must not breach the advertised cap either.
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }

    #[test]
    fn byte_mode_btlbw_is_bytes_per_sec_and_floor_binds_at_low_bdp() {
        // A 20 KB body served in 10 ms: BtlBw = 2 MB/s, raw round trip 10 ms ⇒ a genuine
        // byte-BDP of 20 KB, ×2 gain = 40 KB, below the 100 KB `min_cwnd_bytes` floor. So
        // the floor is the binding operating window — the low-BDP regime the floor exists
        // for. (Unlike the old size-residual model, this binds because the *real* BDP is
        // small, not because the residual spuriously collapsed to ~0.)
        let cfg = byte_test_config(100_000, 256);
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        let snapshot = DeliverySnapshot {
            delivered: 0,
            delivered_at: t0 - Duration::from_millis(10),
        };
        bbr.record_delivery(t0, Duration::from_millis(10), 1, 20_000, 50, snapshot);
        // BtlBw is denominated in bytes/sec now, not blocks/sec.
        assert_eq!(bbr.btlbw_units_per_sec(), Some(2_000_000.0));
        // The byte floor binds because BDP×gain (40 KB) < floor (100 KB).
        assert_eq!(bbr.effective_cwnd(), 100_000);
    }

    #[test]
    fn byte_bdp_uses_raw_rtt_so_a_fast_carrier_lifts_off_the_floor() {
        // The regression guard for the floor-pin fix. An 800 KB body served in 20 ms:
        // BtlBw = 40 MB/s, raw round trip 20 ms ⇒ byte-BDP 800 KB, ×2 gain = 1.6 MB, well
        // above the 256 KB floor. The cwnd lifts off the floor.
        //
        // The *size residual* of this same delivery collapses to the ε floor (its implied
        // transmission 800 KB / 40 MB/s = 20 ms equals the whole round trip), so the old
        // model would have computed BDP ≈ 0 and pinned the cwnd at 256 KB. Using the raw
        // round trip for the BDP is what keeps a genuinely fast carrier from being
        // under-pipelined.
        let cfg = byte_test_config(256_000, 256);
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        let snapshot = DeliverySnapshot {
            delivered: 0,
            delivered_at: t0 - Duration::from_millis(20),
        };
        bbr.record_delivery(t0, Duration::from_millis(20), 1, 800_000, 50, snapshot);
        assert_eq!(bbr.btlbw_units_per_sec(), Some(40_000_000.0));
        // The residual would have zeroed the BDP; the raw round trip does not.
        assert_eq!(bbr.size_residual_rtprop(0.02, 800_000), 1e-4);
        assert_eq!(bbr.effective_cwnd(), 1_600_000);
    }

    #[test]
    fn byte_residual_rtprop_subtracts_transmission_time() {
        // With an established 1 MB/s BtlBw, a 100 ms round trip that carried 50 KB has a
        // residual RTprop of 100 ms − 50 ms = 50 ms (the fixed-latency component), while a
        // round trip whose implied transmission exceeds it clamps to the positive floor.
        let cfg = byte_test_config(1, 256);
        let mut bbr = BbrState::new(&cfg);
        let now = Instant::now();
        bbr.btlbw_per_sec.observe(now, 1_000_000.0);
        let residual = bbr.size_residual_rtprop(0.1, 50_000);
        assert!(
            (residual - 0.05).abs() < 1e-9,
            "residual should subtract 50 ms of transmission, got {residual}",
        );
        // 200 KB at 1 MB/s implies 200 ms of transmission > the 100 ms round trip: clamp.
        assert_eq!(bbr.size_residual_rtprop(0.1, 200_000), 1e-4);
    }

    #[test]
    fn byte_size_aware_delay_gate_does_not_ratchet_a_big_block() {
        // A long smoothed round-trip that is fully explained by a big block's transmission
        // time must NOT ratchet the delay ceiling under `Bytes` (size-aware expected RT),
        // whereas the identical round trip WOULD ratchet under `Blocks` (RTprop-only).
        let now = Instant::now();

        let bytes_cfg = byte_test_config(1, 256);
        let mut bytes = BbrState::new(&bytes_cfg);
        bytes.btlbw_per_sec.observe(now, 1_000_000.0); // 1 MB/s
                                                       // The delay gate's base is the *residual* RTprop estimator (10 ms base RTT here).
        bytes.rtprop_residual_secs.observe(now, 0.01);
        // 200 ms round trip carrying a 190 KB body: expected ≈ 10 ms + 190 ms = 200 ms.
        bytes.update_delay_cap(0.2, 190_000);
        assert!(
            bytes.delay_cap().is_none(),
            "a big block's honest transfer time must not look like a standing queue",
        );

        let blocks_cfg = bbr_test_config();
        let mut blocks = BbrState::new(&blocks_cfg);
        blocks.rtprop_residual_secs.observe(now, 0.01);
        // Same 200 ms round trip, blocks mode: expected = RTprop (10 ms) → ratchets.
        blocks.update_delay_cap(0.2, 190_000);
        assert!(
            blocks.delay_cap().is_some(),
            "blocks mode treats the inflated round trip as a queue and ratchets down",
        );
    }

    #[test]
    fn floor_bypass_never_exceeds_the_advertised_hard_cap() {
        // cwnd == hard cap (8): the bypass must not push in-flight past what the peer
        // advertised it will service.
        let cfg = ZakuraBlockSyncConfig {
            initial_inflight_requests: 8,
            max_inflight_requests: 8,
            ..bbr_test_config()
        };
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.hard_outbound_capacity(), 8);
        fill_outstanding(&mut window, 8);
        assert_eq!(window.available_slots(), 0);
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }
}
