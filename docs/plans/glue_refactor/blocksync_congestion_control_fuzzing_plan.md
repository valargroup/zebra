# Blocksync Congestion-Control Fuzzing And Stability Plan

## Purpose

We need confidence that Zakura blocksync congestion control is stable under arbitrary mixes of
timely responses, late responses, missing responses, partial responses, and peer-local stalls. The
test target is not just the pure `DownloadWindow` math. The target is the real request loop:

```text
PeerRoutine::try_fill
  -> WorkQueue::take_in_range_budgeted
  -> ByteBudget::try_reserve
  -> BlockSyncPeerSession::try_send_get_blocks
  -> OutstandingBlockRange deadline
  -> PeerRoutine response / timeout handling
  -> WorkQueue return / settle / held bytes
  -> DownloadWindow backoff / growth / timeout recovery slots
  -> Sequencer input and durable release
```

The end state should let tests simulate "missing timeouts" and "not missing timeouts" in arbitrary
ways while measuring whether concurrent requests stay stable and as full as possible over the
timeout window.

This document is a planning document. It intentionally names the code seams, expected inputs, and
expected outputs before implementation.

## Current Code Shape

The congestion-control mechanism is split across these production pieces:

- `zebra-network/src/zakura/block_sync/state.rs`
  - `DownloadWindow`: per-peer adaptive window, timeout backoff, success growth, timeout recovery
    slots, repeated-floor-timeout disconnect decision.
  - `OutstandingBlockRange`: request metadata, deadline, and received-body bitmap.
  - `ByteBudget` re-export: shared global body-byte reservation counter.
- `zebra-network/src/zakura/block_sync/peer_routine.rs`
  - Owns one peer's `DownloadWindow`.
  - Sends `GetBlocks`.
  - Tracks `outstanding` requests and deadlines.
  - Receives `Block`, `BlocksDone`, and `RangeUnavailable`.
  - Applies timeout backoff in `expire_due_timeouts`.
- `zebra-network/src/zakura/block_sync/work_queue.rs`
  - Shared pending/in-flight set.
  - Moves heights `pending -> in_flight`.
  - Releases/returns heights on timeout, send failure, short response, reset, and floor advance.
- `zebra-network/src/zakura/transport/guard.rs`
  - `ByteBudget`: lock-free reserve/release/charge accounting and capacity notify.
- `zebra-network/src/zakura/block_sync/reactor.rs`
  - Wires `WorkQueue`, `ByteBudget`, `PeerRegistry`, `SequencerTask`, and per-peer routines.
  - Periodic metrics tick only; per-request timeouts are peer-local.

The existing unit tests already pin several local invariants:

```rust
#[test]
fn peer_outbound_request_window_backs_off_and_grows_with_streaks();

#[test]
fn peer_timeout_recovery_slot_replaces_timed_out_request_above_reduced_window();

#[test]
fn window_reduction_uses_consecutive_timeout_streak();

proptest! {
    fn block_budget_ledger_mirrors_byte_budget(...);
}
```

Those tests are necessary but not sufficient because they do not exercise concurrent real request
issuance, request deadlines, response ordering, `WorkQueue`, `ByteBudget`, and routine wakeups
together.

## Test Goal

The fuzz/stability harness should answer these questions:

1. Does the window remain bounded?
2. Does the peer avoid permanent stalls after any finite timeout pattern?
3. Does the peer eventually disconnect after repeated timeouts at the minimum window?
4. Do timeout recovery slots replace timed-out requests without reopening uncontrolled concurrency?
5. Does a healthy peer keep filling while an unhealthy peer backs off?
6. Does byte-budget pressure stop issuing requests without losing wakeups after release?
7. Do late responses after local timeout avoid double-release, double-return, or duplicate held
   bytes?
8. Can we maximize in-flight requests over a timeout window without overshooting the configured
   hard cap or byte budget?

"Stable" should mean all of these are true for every generated schedule:

```text
0 <= outstanding_requests <= hard_outbound_capacity
1 <= outbound_request_window <= hard_outbound_capacity
0 <= available_slots <= hard_outbound_capacity - outstanding_requests
0 <= timeout_recovery_slots <= hard_outbound_capacity
budget.reserved() <= max_inflight_block_bytes, except explicit charge() overshoot cases
WorkQueue has no height in both pending and in_flight
every live outstanding height is in WorkQueue::in_flight and in PeerRegistry
every timed-out unreceived height is eventually pending again or below the floor
every received height is settled exactly once and remains held until sequencer/floor release
```

## First Enabling Change: Deterministic Time

`PeerRoutine` currently stores request deadlines as `std::time::Instant` and waits with
`tokio::time::sleep(duration)`. That is awkward for `#[tokio::test(start_paused = true)]`: the
Tokio clock can advance while `std::time::Instant::now()` does not.

The smallest useful seam is to introduce a testable clock and use it anywhere timeout behavior is
part of the blocksync protocol.

```rust
#[derive(Copy, Clone, Debug)]
pub(crate) enum BlockSyncClock {
    Real,
    #[cfg(test)]
    Tokio,
}

impl BlockSyncClock {
    fn now(&self) -> tokio::time::Instant {
        match self {
            Self::Real => tokio::time::Instant::now(),
            #[cfg(test)]
            Self::Tokio => tokio::time::Instant::now(),
        }
    }

    fn sleep_until(&self, deadline: tokio::time::Instant) -> tokio::time::Sleep {
        tokio::time::sleep_until(deadline)
    }
}
```

Then change request deadline fields:

```rust
pub(super) struct OutstandingBlockRange {
    pub(super) request: BlockRangeRequest,
    pub(super) queued_at: tokio::time::Instant,
    pub(super) deadline: tokio::time::Instant,
    pub(super) received: ReceivedBlockTracker,
}
```

`PeerRoutine` should compute:

```rust
let queued_at = self.clock.now();
let deadline = queued_at + self.config.request_timeout;
```

and wait with:

```rust
fn earliest_deadline_sleep(&self) -> tokio::time::Sleep {
    let earliest = self
        .window
        .outstanding
        .iter()
        .map(|outstanding| outstanding.deadline)
        .chain(self.retry_avoid.values().copied())
        .min();

    self.clock
        .sleep_until(earliest.unwrap_or_else(|| self.clock.now() + Duration::from_secs(3600)))
}
```

Expected output from this enabling change:

```text
cargo test -p zebra-network peer_outbound_request_window_backs_off_and_grows_with_streaks
passes unchanged

new start-paused timeout tests can advance virtual time by request_timeout +/- epsilon
without sleeping wall-clock seconds
```

## Harness Layers

Use three layers. Each layer earns confidence at a different cost.

### Layer 1: Pure Congestion Model

This layer fuzzes `DownloadWindow` only. It should be very fast and run hundreds or thousands of
cases in normal `cargo test`.

The model input is a sequence of abstract events:

```rust
#[derive(Clone, Debug)]
enum WindowEvent {
    /// A request was sent if a slot is available.
    Send,
    /// One outstanding request completed successfully.
    Success { index: usize },
    /// A timeout batch fired and removed up to `count` outstanding requests.
    TimeoutBatch { count: usize },
    /// Peer advertised a new cap.
    SetAdvertisedCap { max_inflight: u32 },
}
```

The driver applies events to a real `DownloadWindow`:

```rust
fn apply_window_event(window: &mut DownloadWindow, event: WindowEvent) -> ModelObservation {
    match event {
        WindowEvent::Send => {
            if window.available_slots() > 0 {
                window.record_outbound_request_scheduled();
                window.outstanding.push(window_request(next_height()));
            }
        }
        WindowEvent::Success { index } => {
            if index < window.outstanding.len() {
                window.outstanding.remove(index);
                window.increase_outbound_window_after_success();
            }
        }
        WindowEvent::TimeoutBatch { count } => {
            let remove = count.min(window.outstanding.len());
            for _ in 0..remove {
                window.outstanding.remove(0);
            }
            let _ = window.reduce_outbound_window_after_timeout();
        }
        WindowEvent::SetAdvertisedCap { max_inflight } => {
            window.max_inflight_requests = clamp_advertised_inflight(max_inflight);
            let hard = window.hard_outbound_capacity();
            window.outbound_request_window = window.outbound_request_window.min(hard).max(1);
            window.timeout_recovery_slots = window.timeout_recovery_slots.min(hard);
        }
    }

    ModelObservation::from_window(window)
}
```

Example proptest input:

```rust
proptest! {
    #[test]
    fn download_window_fuzz_keeps_invariants(
        events in proptest::collection::vec(window_event_strategy(), 1..512)
    ) {
        let mut window = DownloadWindow::new(&ZakuraBlockSyncConfig {
            max_inflight_requests: 256,
            initial_inflight_requests: 16,
            ..ZakuraBlockSyncConfig::default()
        });

        for event in events {
            let observation = apply_window_event(&mut window, event);
            prop_assert!(observation.outstanding <= observation.hard_cap);
            prop_assert!(observation.effective_window >= 1);
            prop_assert!(observation.effective_window <= observation.hard_cap);
            prop_assert!(observation.timeout_recovery_slots <= observation.hard_cap);
            prop_assert!(observation.available_slots <= observation.hard_cap);
        }
    }
}
```

Expected failure output should be small and useful:

```text
minimal failing input:
[
  SetAdvertisedCap { max_inflight: 8 },
  Send x 8,
  TimeoutBatch { count: 8 },
  Send x 8,
]

last observation:
hard_cap=8 effective_window=1 timeout_recovery_slots=7 outstanding=8 available_slots=0
```

### Layer 2: PeerRoutine With Real WorkQueue And ByteBudget

This is the core harness. It should run the real `PeerRoutine::run` with:

- real `DownloadWindow`
- real `WorkQueue`
- real `ByteBudget`
- real `PeerRegistry`
- real `SequencerView` watch
- real request construction
- encoded/decoded `BlockSyncMessage` frames

It should replace only the network transport and block bodies with deterministic in-memory test
drivers.

The desired harness shape:

```rust
struct CongestionHarness {
    clock: BlockSyncClock,
    config: ZakuraBlockSyncConfig,
    budget: ByteBudget,
    work: Arc<WorkQueue>,
    registry: Arc<PeerRegistry>,
    view_tx: watch::Sender<SequencerView>,
    sequencer_rx: mpsc::Receiver<SequencedBody>,
    peers: Vec<PeerHarness>,
}

struct PeerHarness {
    id: ZakuraPeerId,
    outbound_rx: mpsc::Receiver<BlockSyncMessage>,
    inbound_tx: mpsc::Sender<BlockSyncMessage>,
    task: JoinHandle<Result<(), SinkReject>>,
}
```

The test should start a peer by injecting a real `Status` frame:

```rust
harness.peer(0).send_inbound(BlockSyncMessage::Status(BlockSyncStatus {
    servable_low: block::Height(1),
    servable_high: block::Height(50_000),
    tip_hash,
    max_blocks_per_response: 1,
    max_inflight_requests: 64,
    max_response_bytes: 2 * 1024 * 1024,
})).await;
```

The work source should be real block metadata:

```rust
harness.work.extend((1..=50_000).map(|height| {
    (
        block::Height(height),
        fake_hash(height),
        BlockSizeEstimate::Advertised(16 * 1024),
    )
}));
```

The peer response script is the main input:

```rust
#[derive(Clone, Debug)]
enum ResponsePlan {
    /// Send every requested body before request_timeout.
    CompleteBeforeTimeout { delay: Duration },
    /// Send no frames. The request must timeout locally.
    Missing,
    /// Send after local timeout. This exercises late-response fallthroughs.
    CompleteAfterTimeout { extra_delay: Duration },
    /// Send a prefix and BlocksDone before timeout.
    ShortBeforeTimeout { returned: u32, delay: Duration },
    /// Send RangeUnavailable before timeout.
    RangeUnavailable { delay: Duration },
    /// Send the body before timeout but delay BlocksDone past timeout.
    BodyThenLateDone { body_delay: Duration, done_extra_delay: Duration },
}

#[derive(Clone, Debug)]
struct PeerScript {
    peer: usize,
    plans: Vec<ResponsePlan>,
}
```

Example input for "not missing timeouts":

```rust
PeerScript {
    peer: 0,
    plans: vec![
        ResponsePlan::CompleteBeforeTimeout {
            delay: Duration::from_millis(7_999),
        };
        512
    ],
}
```

Expected output:

```text
disconnect=false
requests_sent >= initial_inflight_requests
timeouts=0
successes=512
window_after >= initial_inflight_requests
budget_drift=0
no duplicate pending/in_flight heights
```

Example input for "missing timeouts":

```rust
PeerScript {
    peer: 0,
    plans: vec![ResponsePlan::Missing; 128],
}
```

Expected output:

```text
timeouts > 0
window_after eventually reaches 1
timeout_recovery_slots > 0 while replacing timed-out work
disconnect=true after repeated timeouts at floor
all unreceived heights returned to pending or claimed by another peer
budget_drift=0
```

Example input for a boundary sweep:

```rust
let timeout = Duration::from_secs(8);
let epsilon = Duration::from_millis(1);

vec![
    ResponsePlan::CompleteBeforeTimeout { delay: timeout - epsilon },
    ResponsePlan::CompleteAfterTimeout { extra_delay: epsilon },
    ResponsePlan::BodyThenLateDone {
        body_delay: timeout - epsilon,
        done_extra_delay: epsilon,
    },
]
```

Expected output:

```text
first request: success, no timeout
second request: timeout path fires before late body is classified
third request: body is held exactly once; late terminator does not double-release
budget_drift=0
```

### Layer 3: Full Reactor Harness

This layer should use `spawn_block_sync_reactor` and then add peers through the same service path
that production uses. It costs more but proves the integration:

```rust
let startup = BlockSyncStartup::new(
    BlockSyncFrontiers {
        finalized_height: block::Height(0),
        verified_block_tip: block::Height(0),
        verified_block_hash: fake_hash(0),
    },
    (block::Height(50_000), fake_hash(50_000)),
    header_tip_rx,
    config,
);

let (handle, actions, reactor_task) = spawn_block_sync_reactor(startup);
```

The driver should answer real `BlockSyncAction::QueryNeededBlocks` actions:

```rust
match actions.recv().await {
    Some(BlockSyncAction::QueryNeededBlocks {
        verified_block_tip,
        best_header_tip,
    }) => {
        handle.send(BlockSyncEvent::NeededBlocks(
            needed_range(verified_block_tip, best_header_tip)
        )).await?;
    }
    Some(BlockSyncAction::Misbehavior { peer, reason }) => {
        observations.misbehavior.push((peer, reason));
    }
    _ => {}
}
```

Expected output for a full-reactor healthy case:

```json
{
  "case": "two_healthy_peers_boundary_before_timeout",
  "requests_sent": 4096,
  "responses_completed": 4096,
  "timeouts": 0,
  "disconnects": 0,
  "max_total_outstanding": 128,
  "mean_window_utilization": 0.95,
  "budget_drift": 0
}
```

Expected output for one unhealthy peer and one healthy peer:

```json
{
  "case": "one_missing_one_healthy",
  "peer_0": {
    "timeouts": 64,
    "window_final": 1,
    "disconnect": true
  },
  "peer_1": {
    "timeouts": 0,
    "window_final": 64,
    "requests_completed": 1024
  },
  "global": {
    "completed_heights_are_gap_free": true,
    "budget_drift": 0,
    "healthy_peer_kept_filling": true
  }
}
```

## Delay Injection

The harness should delay responses, not production code. The peer driver observes outgoing
`GetBlocks` and schedules inbound frames:

```rust
async fn drive_peer_script(mut peer: PeerHarness, script: PeerScript, blocks: BlockStore) {
    let mut request_index = 0usize;
    while let Some(message) = peer.outbound_rx.recv().await {
        let BlockSyncMessage::GetBlocks { start_height, count } = message else {
            continue;
        };

        let plan = script
            .plans
            .get(request_index)
            .cloned()
            .unwrap_or(ResponsePlan::CompleteBeforeTimeout {
                delay: Duration::from_millis(1),
            });
        request_index += 1;

        match plan {
            ResponsePlan::CompleteBeforeTimeout { delay } => {
                tokio::spawn(send_complete(peer.inbound_tx.clone(), blocks.clone(), start_height, count, delay));
            }
            ResponsePlan::Missing => {}
            ResponsePlan::CompleteAfterTimeout { extra_delay } => {
                let delay = peer.config.request_timeout + extra_delay;
                tokio::spawn(send_complete(peer.inbound_tx.clone(), blocks.clone(), start_height, count, delay));
            }
            ResponsePlan::ShortBeforeTimeout { returned, delay } => {
                tokio::spawn(send_short(peer.inbound_tx.clone(), blocks.clone(), start_height, returned, delay));
            }
            ResponsePlan::RangeUnavailable { delay } => {
                tokio::spawn(send_unavailable(peer.inbound_tx.clone(), start_height, count, delay));
            }
            ResponsePlan::BodyThenLateDone { body_delay, done_extra_delay } => {
                tokio::spawn(send_body_then_late_done(
                    peer.inbound_tx.clone(),
                    blocks.clone(),
                    start_height,
                    count,
                    body_delay,
                    peer.config.request_timeout + done_extra_delay,
                ));
            }
        }
    }
}
```

The important property is that missing timeouts are represented by absence of inbound frames, not
by a fake call to `reduce_outbound_window_after_timeout`. Timely responses are represented by real
`Block`/`BlocksDone` frames arriving before the deadline. Late responses arrive after the real
deadline arm has already run.

## Measuring "Maximize These Over The Timeout Window"

For each case, collect a time series of:

```rust
#[derive(Clone, Debug, Serialize)]
struct CongestionSample {
    at_ms: u64,
    peer: usize,
    hard_cap: usize,
    effective_window: usize,
    available_slots: usize,
    timeout_recovery_slots: usize,
    outstanding_requests: usize,
    requests_sent_total: u64,
    responses_completed_total: u64,
    timeouts_total: u64,
    budget_reserved: u64,
    work_pending: usize,
    work_in_flight: usize,
}
```

Derived metrics:

```rust
#[derive(Clone, Debug, Serialize)]
struct CongestionSummary {
    case_name: String,
    seed: u64,
    duration_ms: u64,
    request_timeout_ms: u64,
    peers: Vec<PeerSummary>,
    max_total_outstanding: usize,
    mean_total_outstanding: f64,
    mean_window_utilization: f64,
    timeout_boundary_misses: u64,
    duplicate_height_claims: u64,
    budget_drift_events: u64,
    stalled_periods_over_timeout: u64,
}

#[derive(Clone, Debug, Serialize)]
struct PeerSummary {
    peer: usize,
    requests_sent: u64,
    responses_completed: u64,
    timed_out_batches: u64,
    disconnected: bool,
    window_initial: usize,
    window_min: usize,
    window_max: usize,
    window_final: usize,
    max_outstanding: usize,
    mean_outstanding: f64,
}
```

For healthy peers, "maximize over the timeout window" means:

```text
mean_outstanding / effective_window >= 0.90
for each request-timeout-sized observation window after warmup
```

For unhealthy peers, it means:

```text
outstanding drains or is replaced within one timeout window
timeout recovery slots are consumed only by replacement requests
window eventually backs off instead of repeatedly filling at the old cap
```

For mixed peers, it means:

```text
healthy peers keep mean_outstanding / effective_window >= 0.90
while unhealthy peers back off or disconnect
```

Example JSONL output:

```json
{"event":"sample","at_ms":8000,"peer":0,"effective_window":64,"outstanding_requests":64,"available_slots":0,"timeouts_total":0,"budget_reserved":1048576}
{"event":"sample","at_ms":16000,"peer":0,"effective_window":56,"outstanding_requests":56,"available_slots":0,"timeouts_total":16,"budget_reserved":917504}
{"event":"summary","case_name":"boundary_sweep_seed_42","seed":42,"mean_window_utilization":0.94,"budget_drift_events":0,"stalled_periods_over_timeout":0}
```

## Fuzz Input Strategy

Generate scenarios with explicit peer count, config, work size, size hints, and response scripts.

```rust
#[derive(Clone, Debug)]
struct CongestionCase {
    seed: u64,
    config: TestConfig,
    peers: Vec<PeerCase>,
    heights: HeightRange,
    run_for: Duration,
}

#[derive(Clone, Debug)]
struct TestConfig {
    request_timeout: Duration,
    max_inflight_requests: u32,
    initial_inflight_requests: u32,
    max_blocks_per_response: u32,
    max_response_bytes: u32,
    max_inflight_block_bytes: u64,
    body_size_hint: u32,
}

#[derive(Clone, Debug)]
struct PeerCase {
    servable_low: block::Height,
    servable_high: block::Height,
    advertised_inflight: u32,
    response_plans: Vec<ResponsePlan>,
}
```

Suggested proptest ranges:

```rust
fn congestion_case_strategy() -> impl Strategy<Value = CongestionCase> {
    (
        any::<u64>(),
        1usize..=8,
        1u32..=256,       // max_inflight_requests
        1u32..=64,        // initial_inflight_requests
        1u32..=8,         // max_blocks_per_response
        64u32..=64 * 1024 // body size hint
    ).prop_flat_map(|(seed, peers, max_inflight, initial, blocks_per_response, body_size)| {
        // Build scripts whose delays cluster around timeout - epsilon,
        // timeout, and timeout + epsilon.
        ...
    })
}
```

Delay distribution should be biased toward boundary conditions:

```text
10% immediate response
25% timeout - epsilon
10% exactly timeout
25% timeout + epsilon
10% no response
10% partial response
10% RangeUnavailable
```

The exact-timeout case is useful because Tokio scheduling order can expose off-by-one assumptions.
The expected behavior should be defined by the code's deadline comparison: if the deadline arm wins,
the request times out; if the response arm wins first, it succeeds. The invariant is not which arm
wins at equality, but that either winner leaves coherent state.

## Required Oracles

Implement reusable assertion helpers rather than scattering checks through every test.

```rust
fn assert_window_invariants(peer: &PeerObservation) {
    assert!(peer.effective_window >= 1);
    assert!(peer.effective_window <= peer.hard_cap);
    assert!(peer.outstanding_requests <= peer.hard_cap);
    assert!(peer.timeout_recovery_slots <= peer.hard_cap);
    assert!(peer.available_slots <= peer.hard_cap.saturating_sub(peer.outstanding_requests)
        || peer.timeout_recovery_slots > 0);
}

fn assert_no_budget_drift(h: &CongestionHarness) {
    let expected = h.work.reserved_bytes()
        .saturating_add(h.sequencer_input_bytes())
        .saturating_add(h.sequencer_held_bytes());
    assert!(h.budget.audit(expected, "congestion harness"));
}

fn assert_work_queue_disjoint(h: &CongestionHarness) {
    let snapshot = h.work.snapshot_for_tests();
    for height in snapshot.pending.keys() {
        assert!(!snapshot.in_flight.contains_key(height));
    }
}

fn assert_no_timeout_stall(summary: &CongestionSummary) {
    assert_eq!(
        summary.stalled_periods_over_timeout, 0,
        "no peer with pending servable work and budget capacity may sit idle for a full timeout"
    );
}
```

The plan needs one test-only `WorkQueue` snapshot API:

```rust
#[cfg(test)]
#[derive(Clone, Debug)]
pub(super) struct WorkQueueSnapshot {
    pub pending: BTreeMap<block::Height, WorkItem>,
    pub in_flight: BTreeMap<block::Height, WorkItem>,
    pub floor: block::Height,
}

#[cfg(test)]
impl WorkQueue {
    pub(super) fn snapshot_for_tests(&self) -> WorkQueueSnapshot {
        let inner = self.lock();
        WorkQueueSnapshot {
            pending: inner.pending.clone(),
            in_flight: inner.in_flight.clone(),
            floor: inner.floor,
        }
    }
}
```

## Concrete Tests To Add

### 1. Pure Property: Arbitrary Window Events

Name:

```rust
download_window_fuzz_keeps_bounds_and_recovery_slots_coherent
```

Input:

```text
1..512 WindowEvent values
hard cap 1..512
initial window 1..hard cap
```

Expected output:

```text
No panics.
Window always in [1, hard_cap].
Outstanding never exceeds hard_cap.
Timeout recovery slots never exceed hard_cap.
Repeated timeout-at-floor eventually returns DisconnectPeer.
```

### 2. Peer Routine: All Responses Just Before Timeout

Name:

```rust
peer_routine_keeps_window_full_when_responses_arrive_before_timeout
```

Input:

```rust
request_timeout = Duration::from_secs(8)
initial_inflight_requests = 64
max_inflight_requests = 64
body_count = 4096
response delay = request_timeout - 1ms
```

Expected output:

```text
timeouts_total = 0
disconnect = false
mean_window_utilization >= 0.90 after warmup
window_final = 64
budget_drift = 0
```

### 3. Peer Routine: All Responses Just After Timeout

Name:

```rust
peer_routine_times_out_and_ignores_late_responses_without_budget_drift
```

Input:

```rust
request_timeout = Duration::from_secs(8)
initial_inflight_requests = 64
max_inflight_requests = 64
body_count = 4096
response delay = request_timeout + 1ms
```

Expected output:

```text
timeouts_total > 0
late responses do not create duplicate held heights
window backs off monotonically on timeout epochs
budget_drift = 0
eventual disconnect if the peer reaches repeated timeouts at window floor
```

### 4. Peer Routine: Mixed Before/After/Missing

Name:

```rust
peer_routine_fuzzes_missing_and_non_missing_timeouts
```

Input:

```rust
proptest-generated Vec<ResponsePlan>
1..8 peers
run_for = 4 * request_timeout
```

Expected output:

```text
No invariant failures.
Every unreceived timed-out height returns to pending or is claimed by another peer.
No height is pending and in_flight simultaneously.
No peer exceeds advertised max_inflight_requests.
```

### 5. Mixed Peers: Healthy Peer Keeps Filling

Name:

```rust
healthy_peer_keeps_filling_while_unhealthy_peer_backs_off
```

Input:

```rust
peer 0: ResponsePlan::Missing repeated
peer 1: CompleteBeforeTimeout { delay: 1ms } repeated
fanout = 1
max_inflight_requests = 64
initial_inflight_requests = 64
```

Expected output:

```text
peer 0 window_final = 1 or disconnect = true
peer 1 timeouts_total = 0
peer 1 mean_window_utilization >= 0.90
global completed height count keeps increasing during peer 0 backoff
budget_drift = 0
```

### 6. Byte Budget Saturation And Release

Name:

```rust
budget_saturation_stops_requests_and_release_wakes_fill_loop
```

Input:

```rust
body_size_hint = 1024
max_inflight_block_bytes = 64 * 1024
initial_inflight_requests = 256
max_inflight_requests = 256
responses held in sequencer input until explicit release
```

Expected output:

```text
requests stop when budget is full
outstanding_requests <= 64
after durable frontier release, new requests are sent without a wall-clock retry poll
budget.reserved() never exceeds max_inflight_block_bytes
```

### 7. Boundary Equality

Name:

```rust
response_at_exact_deadline_leaves_coherent_state
```

Input:

```rust
delay = request_timeout
repeat 512 cases with varied select scheduling
```

Expected output:

```text
Each request is either success or timeout.
No request is both success and timeout.
No budget drift.
No duplicate pending/in_flight height.
```

## Implementation Phases

### Phase A: Add Test Observability

Add only `#[cfg(test)]` helpers:

- `WorkQueue::snapshot_for_tests`.
- `DownloadWindow` observation helper or `PeerRegistry::slot_diagnostics_for_tests`.
- Counters in the harness, not production code, for requests sent, responses completed, timeouts,
  late responses, and disconnects.

Expected code footprint:

```text
small test-only additions in work_queue.rs / peer_registry.rs
no behavior changes
```

### Phase B: Make Time Deterministic

Move blocksync deadline storage to `tokio::time::Instant` or add an equivalent `BlockSyncClock`.

Expected code footprint:

```text
OutstandingBlockRange.queued_at: tokio::time::Instant
OutstandingBlockRange.deadline: tokio::time::Instant
PeerRoutine retry_avoid timestamps: tokio::time::Instant
elapsed trace helpers updated to use deadline/queued_at elapsed from Tokio instants
existing tests updated mechanically
```

### Phase C: Add Pure Window Proptest

This is cheap and should land first after deterministic-time prep if needed.

Expected command:

```bash
cargo test -p zebra-network download_window_fuzz_keeps_bounds_and_recovery_slots_coherent
```

Expected output:

```text
test result: ok
```

### Phase D: Add PeerRoutine Harness

Prefer in-memory transport adapters over a large mock of `PeerRoutine`. The harness should encode
`BlockSyncMessage` frames so decode and frame-size paths remain real.

Expected command:

```bash
cargo test -p zebra-network peer_routine_keeps_window_full_when_responses_arrive_before_timeout
cargo test -p zebra-network peer_routine_times_out_and_ignores_late_responses_without_budget_drift
```

Expected output:

```text
test result: ok
```

### Phase E: Add Scenario Fuzzer

Use `proptest` with deterministic seeds printed in failure output. Keep the case count moderate in
normal tests, and expose an ignored stress test for deeper runs.

```rust
proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]

    #[test]
    fn peer_routine_fuzzes_missing_and_non_missing_timeouts(case in congestion_case_strategy()) {
        run_congestion_case(case)?;
    }
}

#[tokio::test(start_paused = true)]
#[ignore]
async fn peer_routine_congestion_stress_seed_corpus() {
    for seed in load_seed_corpus("test-vectors/blocksync_congestion_seeds.txt") {
        run_congestion_case(CongestionCase::from_seed(seed)).await;
    }
}
```

Expected output on failure:

```text
thread 'peer_routine_fuzzes_missing_and_non_missing_timeouts' panicked at:
budget drift after late response
seed=0x91c0ffee
case={... compact Debug ...}
summary={... json ...}
```

### Phase F: Add Full Reactor Smoke Scenarios

Keep these few and high signal:

- two healthy peers at `timeout - 1ms`;
- one missing peer plus one healthy peer;
- budget saturation and release.

Expected command:

```bash
cargo test -p zebra-network reactor_congestion_
```

Expected output:

```text
test result: ok
```

## Non-Goals

- Do not add production-only delay injection to blocksync.
- Do not mock `DownloadWindow`, `WorkQueue`, or `ByteBudget` in the core harness.
- Do not assert exact `select!` winner ordering at the exact timeout boundary.
- Do not require wall-clock sleeps for timeout tests.
- Do not turn this into an end-to-end sync benchmark. Throughput benchmarking belongs in the
  existing throughput probe/bench paths; this harness is for stability and state coherence.

## Review Checklist

Before implementation is considered done:

```text
cargo fmt --all -- --check
cargo test -p zebra-network download_window_fuzz_keeps_bounds_and_recovery_slots_coherent
cargo test -p zebra-network peer_routine_keeps_window_full_when_responses_arrive_before_timeout
cargo test -p zebra-network peer_routine_times_out_and_ignores_late_responses_without_budget_drift
cargo test -p zebra-network healthy_peer_keeps_filling_while_unhealthy_peer_backs_off
cargo test -p zebra-network budget_saturation_stops_requests_and_release_wakes_fill_loop
```

For stress runs before a PR:

```bash
cargo test -p zebra-network peer_routine_congestion_stress_seed_corpus -- --ignored
```

The PR evidence should include:

```text
number of proptest cases
number of deterministic seed-corpus cases
largest peer count tested
largest max_inflight_requests tested
timeout boundary cases tested: timeout-1ms, timeout, timeout+1ms, missing
whether any stress tests are ignored and why
AI disclosure if AI was used
```
