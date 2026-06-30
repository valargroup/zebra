# Zakura Block-Sync Flow

This is a reading map for the current block-sync reactor and its collaborators.
It describes the code as it exists in this checkout.

## Short Version

Zakura block sync downloads block bodies for headers that header sync has already
committed. It does not own header validation or consensus validation.

```text
zebrad startup
  -> zebra_network::init_with_zakura_header_sync(...)
  -> spawn_block_sync_reactor(...)
  -> drive_block_sync_actions(...)        state reads for missing/served bodies
  -> Committer::run(...)                  consensus Request::Commit
  -> drive_block_sync_durable_frontier(...) durable-tip feedback (releases held bytes)
  -> mirror_zakura_full_block_commits(...) republish committed tip into the sync-frontier watch

peer stream 6
  -> BlockSyncService::add_peer
  -> PeerRoutine::run
  -> WorkQueue + ByteBudget + PeerRegistry
  -> SequencerTask
  -> applyQ
  -> Committer
  -> verifier/state
  -> durable frontier watch
  -> SequencerTask releases held bytes and advances the floor
```

The old mental model "all inbound block-sync messages go through the reactor" is
no longer true. The current shape is:

- `PeerRoutine`: per-peer transport read, decode, request issuance, response
  matching, peer-local timeouts.
- `BlockSyncReactor`: global/shared concerns: lifecycle, serving inbound
  `GetBlocks`, status advertisement, needed-work producer, candidate publication,
  metrics/trace snapshots.
- `SequencerTask`: serial commit pipeline: reorder bodies, drain contiguous
  bodies onto `applyQ`, hold bytes until durable, reset on commit rejection.
- `Committer` in `zebrad`: drains `applyQ` and calls the consensus verifier.

## Where It Initializes

Production block sync starts only when `network.v2_p2p` is enabled:

```rust
fn use_zakura_block_sync(config: &zebra_network::Config) -> bool {
    config.v2_p2p
}
```

Startup path:

1. `zebrad/src/commands/start.rs` initializes state and consensus.
2. It builds `zakura_header_sync_driver_startup(...)` when `v2_p2p` is enabled.
3. It calls `zebra_network::init_with_zakura_header_sync(...)`.
4. `zebra-network/src/zakura/handler.rs` builds a shared
   `ZakuraSyncExchange`, starts header sync, then starts block sync:

```rust
let mut startup = BlockSyncStartup::new_with_exchange(
    BlockSyncFrontiers {
        finalized_height: driver_startup.frontiers.finalized_height,
        verified_block_tip: driver_startup.frontiers.verified_block_tip,
        verified_block_hash: driver_startup.verified_block_tip_hash,
    },
    best_header_tip,
    frontier_updates,
    config.zakura.block_sync.clone(),
);
startup.shutdown = header_sync_shutdown.clone();
startup.trace = trace.clone();
let (handle, actions, task) = spawn_block_sync_reactor(startup);
```

5. The service registry installs `BlockSyncService::new_with_handle(...)`, so
   stream-6 peers are wired to this reactor handle.
6. `zebrad` takes the `BlockSyncAction` receiver and starts the node-side tasks:

```rust
drive_block_sync_actions(block_actions, ..., block_sync.clone(), read_only_state_service, ...)
Committer::new(block_sync.take_apply_queue().expect("taken once"), block_verifier_router, ...)
drive_block_sync_durable_frontier(chain_tip_change, latest_chain_tip, read_state, block_sync, ...)
mirror_zakura_full_block_commits(chain_tip_change, latest_chain_tip, read_state, header_sync, ...)
```

   There are two durable-tip-driven feedback tasks, not one.
   `drive_block_sync_durable_frontier` reports the durable frontier *into the
   sequencer* (`report_durable_frontier` -> `FrontierAdvance`) so it releases
   held bytes and advances the floor. `mirror_zakura_full_block_commits`
   republishes the committed tip *into the `ZakuraSyncExchange` frontier watch*
   (as `FrontierChange::VerifiedGrow`, or `VerifiedReset` on a reorg), which is
   the `frontier_updates` source the reactor consumes (and which serving /
   header-sync also read). Both wake on `ChainTipChange`.

7. Legacy `ChainSync` no longer downloads bodies:

```rust
if use_zakura_block_sync(&config.network) {
    syncer.bootstrap_genesis_then_pause(read_only_state_service.clone())
} else {
    syncer.sync()
}
```

## Reactor Construction

`spawn_block_sync_reactor()` creates the shared internal topology:

```rust
let state = BlockSyncState::new(&startup);
let (events_tx, events_rx) = mpsc::channel(...);
let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
let (actions_tx, actions_rx) = mpsc::channel(actions_capacity);
let (peers_tx, peers_rx) = watch::channel(...);
let (status_tx, status_rx) = watch::channel(...);
let (candidates_tx, candidates_rx) = watch::channel(...);

let (sequencer_input_tx, sequencer_body_input_rx) = mpsc::channel(submitted_apply_limit);
let (sequencer_control_tx, sequencer_control_rx) = mpsc::unbounded_channel();
let (apply_tx, apply_rx) = mpsc::unbounded_channel::<ApplyItem>();
let (sequencer_view_tx, sequencer_view_rx) = watch::channel(initial_view(startup.frontiers));

tokio::spawn(SequencerTask::new(...).run());

let registry = Arc::new(PeerRegistry::new());
let (routine_to_reactor_tx, routine_to_reactor_rx) = mpsc::channel(1024);
```

The returned `BlockSyncHandle` contains:

- bounded `events` sender for normal events,
- unbounded `lifecycle` sender for peer lifecycle/control,
- take-once `apply_queue_rx`,
- watch receivers for peer slots, local status, and candidate state,
- `RoutineWiring`, cloned into every peer routine by `BlockSyncService`.

## External Communication Points Outside P2P

These are the non-stream communication edges between block sync and the rest of
the node.

| Direction | Mechanism | Producer | Consumer | Purpose |
| --- | --- | --- | --- | --- |
| node -> reactor | `BlockSyncStartup.frontier_updates: watch::Receiver<FrontierUpdate>` | `ZakuraSyncExchange` / header sync driver | reactor | best-header and verified-body frontier changes |
| node -> reactor | `BlockSyncHandle::send_control(BlockSyncEvent::NeededBlocks)` | `drive_block_sync_actions` | reactor | state answered missing body metadata |
| node -> reactor | `BlockSyncHandle::send_control(BlockRangeResponseReady/Finished)` | `drive_block_sync_actions` | reactor | state answered an inbound peer `GetBlocks` request |
| reactor -> node | `mpsc::Receiver<BlockSyncAction>` | reactor / sequencer / routines | `drive_block_sync_actions` | state reads and record-only misbehavior |
| sequencer -> node | `applyQ: mpsc::UnboundedReceiver<ApplyItem>` | `SequencerTask` | `Committer` | contiguous bodies ready for consensus commit |
| node -> sequencer | `BlockSyncHandle::report_commit_rejected(CommitterReset)` | `Committer` | `SequencerTask` | rollback after invalid/timed-out commit |
| node -> sequencer | `BlockSyncHandle::report_durable_frontier(BlockSyncFrontiers)` | durable frontier watcher | `SequencerTask` | release held bytes after state makes commits durable |
| reactor -> discovery | `BlockSyncHandle::subscribe_candidate_state()` / `candidate_state()` | reactor | discovery/dialing | node ids that can help with missing bodies |
| reactor -> observers | `subscribe_status()`, `peer_snapshot()` | reactor | discovery/tests/diagnostics | local status and peer slot snapshots |
| all -> observability | metrics + `ZakuraTrace` | reactor/routines/sequencer/driver/committer | operators/tests | stall and throughput diagnosis |
| endpoint -> all tasks | `CancellationToken` | `ZakuraEndpoint::shutdown` | reactor/routines/drivers/committer | shutdown |

The `BlockSyncAction` enum is the main reactor-to-zebrad API:

```rust
pub enum BlockSyncAction {
    QueryNeededBlocks { verified_block_tip: block::Height, best_header_tip: block::Height },
    QueryBlocksByHeightRange { peer: ZakuraPeerId, start: block::Height, count: u32 },
    Misbehavior { peer: ZakuraPeerId, reason: BlockSyncMisbehavior },
}
```

`drive_block_sync_actions()` handles those by querying `zebra-state`:

- `QueryNeededBlocks` -> `MissingBlockBodies`, `HeadersByHeightRange`,
  `BlockSizeHints`, then sends `BlockSyncEvent::NeededBlocks`.
- `QueryBlocksByHeightRange` -> `BlocksByHeightRange`, then sends
  `BlockRangeResponseReady` or `BlockRangeResponseFinished`.
- `Misbehavior` is currently record-only; it does not disconnect peers.

The apply seam is intentionally split across crates:

```rust
pub struct ApplyItem {
    pub height: block::Height,
    pub hash: block::Hash,
    pub block: Arc<block::Block>,
    pub bytes: u64,
    pub source_peer: ZakuraPeerId,
    pub epoch: u64,
}
```

`zebra-network` can create `ApplyItem`, but it cannot name
`zebra_consensus::Request::Commit`, so `zebrad::commands::start::zakura::Committer`
owns verifier calls.

## Internal Communication Inside Block Sync

### Channel Map

```text
BlockSyncService::add_peer
  -> lifecycle: BlockSyncEvent::PeerConnected / PeerDisconnected
  -> spawns PeerRoutine with RoutineWiring

PeerRoutine
  -> routine_to_reactor: StatusReceived / ServeGetBlocks / RequeryNeeded / Misbehavior
  -> actions: Misbehavior
  -> sequencer_input: SequencedBody
  -> reads sequencer_view
  -> writes PeerRegistry and WorkQueue

BlockSyncReactor
  -> actions: QueryNeededBlocks / QueryBlocksByHeightRange / Misbehavior
  -> sequencer_control: FrontierAdvance / FrontierReset
  -> watches sequencer_view
  -> publishes peers/status/candidates
  -> reads PeerRegistry and WorkQueue

SequencerTask
  <- sequencer_input: downloaded bodies
  <- sequencer_control: frontier/reset/floor-funding/rejection
  -> applyQ: ApplyItem
  -> sequencer_view: SequencerView
  -> actions: Misbehavior on invalid commit reset
  -> mutates WorkQueue and ByteBudget on floor/reset/release
```

### Shared Structures

| Struct | Owner / writers | Readers | Responsibility |
| --- | --- | --- | --- |
| `BlockSyncState` | reactor | reactor | global reactor mirrors, thin peer handles, work queue handle, byte budget, local status state, trace counters |
| `PeerBlockState` | reactor | reactor | per-peer serving handle only: session clone, direction, status refresh meter, inbound `GetBlocks` serving slots |
| `PeerRoutine` | one task per peer | itself | decode stream-6, own `DownloadWindow`, request blocks, match responses, timeout/retry, forward bodies |
| `DownloadWindow` | one `PeerRoutine` | same routine / registry snapshot | adaptive per-peer request window, outstanding ranges, success growth, timeout backoff |
| `PeerRegistry` | routines write facts; reactor inserts/removes | reactor, routines | generation-gated cross-peer facts: servable ranges, caps, outstanding unreceived heights, slot diagnostics |
| `WorkQueue` | reactor extends; routines take/return/settle; sequencer advances/resets | reactor/routines/sequencer | sorted pending/in-flight body heights and per-height byte ledger |
| `ByteBudget` | routines reserve/settle; sequencer releases/charges | routines/reactor/sequencer | global in-flight body byte bound |
| `Sequencer` | `SequencerTask` only | `SequencerTask` only | reorder buffer, applying ledger, verified tip, body download floor |
| `SequencerTask` | its own serial task | reactor/routines through `SequencerView` | commit pipeline and durable byte release |
| `Committer` | zebrad task | none | drain `applyQ`, fire verifier commits concurrently, report failures |

## Reactor Run Loop

The reactor selects over:

- shutdown token,
- unbounded lifecycle events,
- bounded driver/control events,
- one of two mutually-exclusive frontier sources (a direct best-header-tip
  watch `header_tip`, or the shared `FrontierUpdate` watch from
  `ZakuraSyncExchange`; a `debug_assert` enforces exactly one is wired),
- `SequencerView` watch from `SequencerTask`,
- `RoutineToReactor` messages from peer routines,
- periodic metrics/status/floor-watchdog ticks.

Important: per-request timeouts are not reactor timers anymore. They live in
each `PeerRoutine`.

```rust
tokio::select! {
    _ = self.startup.shutdown.cancelled() => break,
    event = self.lifecycle.recv() => self.handle_event(event.unwrap()).await,
    event = self.events.recv() => self.handle_event(event.unwrap()).await,
    // Exactly one of these two frontier arms is active (see debug_assert above).
    changed = header_tip.changed(), if header_tip_open
        => self.handle_header_tip_changed(height, hash).await,
    changed = frontier_updates.changed(), if frontier_updates_open
        => self.handle_frontier_update(update).await,
    changed = self.sequencer_view.changed() => self.on_sequencer_view_changed(...).await,
    message = self.routine_to_reactor.recv() => self.handle_routine_message(...).await,
    _ = metrics_ticks.tick() => { self.publish_metrics(); self.refresh_throughput(); self.trace_sync_state(); }
    _ = status_ticks.tick() => self.flush_status_refresh().await,
    _ = floor_watchdog_ticks.tick() => self.run_floor_watchdog(Instant::now()),
}
```

## Peer Lifecycle

`BlockSyncService::add_peer()` owns the stream-6 spawn point:

```rust
let generation = wiring.registry.admit(&peer_id, direction, &wiring.config);
let routine = PeerRoutine::new(
    peer_id,
    block_sync_session,
    recv,
    wiring.config,
    generation,
    wiring.budget,
    wiring.work,
    wiring.registry,
    wiring.received_throughput,
    wiring.sequencer_input,
    wiring.sequencer_input_bytes,
    wiring.sequencer_control,
    wiring.actions,
    wiring.routine_to_reactor,
    wiring.view,
    run_cancel,
    wiring.trace,
);
routine.run().await
```

Then it sends lifecycle:

```rust
lifecycle.send(BlockSyncEvent::PeerConnected(block_sync_session));
```

On teardown it removes the service peer record and sends
`PeerDisconnected`. The routine's `Drop` guard returns unreceived outstanding
heights to `WorkQueue` and releases their reserved bytes.

The reactor's `PeerConnected` handler only admits/parks, stores a thin
`PeerBlockState`, publishes peer/candidate state, and sends initial `Status`.
It does not spawn the routine and it does not own inbound decode.

## Inbound Stream-6 Message Flow

Each peer routine owns the ordered stream reader:

```text
FramedRecv
  -> SessionGuard
  -> BlockSyncMessage::decode_frame_with_raw_block_payload
  -> match message
```

Dispatch:

- `Status`: routine validates/rate-limits it, updates its own servable range and
  caps, writes `PeerRegistry::upsert_status`, then sends
  `RoutineToReactor::StatusReceived`.
- `GetBlocks`: routine sends `RoutineToReactor::ServeGetBlocks`; reactor asks
  state for committed blocks and sends them through its session clone.
- `Block`: routine matches against its own outstanding requests, validates hash
  and size, settles byte budget, forwards `SequencedBody` to `SequencerTask`.
- `BlocksDone`: routine finishes the matching outstanding range and retries
  unreceived suffixes if needed.
- `RangeUnavailable`: routine returns the requested range to `WorkQueue` and
  applies retry bias.

Malformed frames report `MalformedMessage` through `RoutineToReactor` and return
a protocol reject so the supervised pipe closes the connection.

## Work Lifecycle: Queued To Applied

### 1. Header Sync Advances The Body Target

Header sync publishes a `FrontierUpdate` through `ZakuraSyncExchange`. The block
reactor receives it through `BlockSyncStartup.frontier_updates`:

```rust
FrontierChange::HeaderAdvanced => {
    self.handle_header_tip_changed(frontier.best_header.height, frontier.best_header.hash).await;
}
```

`handle_header_tip_changed()` updates `best_header_tip/hash` and calls
`query_needed_blocks()`.

### 2. Reactor Asks State Which Bodies Are Missing

`query_needed_blocks()` self-gates on:

- state queries enabled,
- `request_floor < best_header_tip`,
- no local work covering the header tip,
- local work below low-water,
- no identical query already pending.

Then it emits:

```rust
BlockSyncAction::QueryNeededBlocks {
    verified_block_tip: self.request_floor,
    best_header_tip: self.state.best_header_tip,
}
```

The driver answers by reading state:

```text
MissingBlockBodies { from, limit }
HeadersByHeightRange { start: first, count: span }
BlockSizeHints { from: first, count: span }
```

and sends `BlockSyncEvent::NeededBlocks(Vec<BlockSyncBlockMeta>)`.

### 3. Reactor Extends `WorkQueue`

`handle_needed_blocks()` filters out heights that are already:

- at/below `request_floor`,
- in `work.in_flight`,
- still outstanding in `PeerRegistry` for the same hash.

Then it extends the pending queue:

```rust
let count = self.state.work.extend(
    blocks.into_iter().map(|block| (block.height, block.hash, block.size)),
);
self.publish_candidate_state();
```

`WorkQueue::extend()` inserts only heights above its floor and not already
pending or in-flight, then wakes routines waiting on `work.subscribe_available()`.

### 4. Peer Routine Takes Work And Sends `GetBlocks`

`PeerRoutine::try_fill()` wakes on work availability, budget capacity, peer
responses, view changes, and timeouts. For an eligible peer with `Status`, it:

> TODO: refactor the shit out of try_fill, that is bonkers

1. checks adaptive slots in `DownloadWindow`,
2. takes a contiguous pending run in the peer's servable range,
3. reserves the summed estimated bytes in `ByteBudget`,
4. marks those heights reserved in `WorkQueue`,
5. sends `BlockSyncMessage::GetBlocks`,
6. records an `OutstandingBlockRange`,
7. publishes outstanding unreceived heights to `PeerRegistry`.

Representative path:

```rust
let items = self.work.take_in_range_budgeted(
    servable_low,
    servable_high,
    max_count,
    decision.max_request_bytes,
);

if self.reserve_request_budget(request_priority, reserved_bytes).await {
    self.work.mark_reserved(items.iter().map(|(height, _)| *height));
    self.session.try_send_get_blocks(request.start_height, request.count)?;
    self.window.outstanding.push(OutstandingBlockRange { request, queued_at, deadline, ... });
    self.publish_outstanding();
}
```

The fetch decision is bounded by byte budget and peer slots, not by distance
from the committed tip.

### 5. Peer Routine Receives Bodies

For a matching `Block` response, the routine:

1. extracts height/hash,
2. finds an outstanding range covering the height,
3. rejects duplicates, wrong hash, cancelled claims, and size mismatches,
4. records throughput,
5. settles the estimate reservation to actual serialized bytes,
6. marks the height received,
7. completes the outstanding range if all bodies arrived,
8. forwards a `SequencedBody` to the sequencer body input.

```rust
let Some(delta) = self.work.settle_active_reserved_height(height, serialized_bytes) else {
    self.finish_outstanding_at(index, Disposition::RetryMissing);
    return;
};
self.apply_budget_delta(delta);
outstanding.mark_received(height);
self.forward_body_to_sequencer(height, hash, body, serialized_bytes, body_permit).await;
```

The `sequencer_input` send is the one blocking routine send. If the commit
pipeline is slow, only this peer routine stalls.

Timeouts, `BlocksDone`, `RangeUnavailable`, send failures, and disconnects
return only unreceived heights to `pending` and release only the bytes still
owned by those heights.

### 6. Sequencer Buffers And Drains Contiguous Bodies

`SequencerTask` receives `SequencedBody`, subtracts its queued input byte count,
and calls `Sequencer::accept_buffered_body(...)`.

`Sequencer` owns:

- `ReorderBuffer`: received bodies above the floor, possibly with gaps.
- `applying`: bytes-only held ledger for bodies drained to `applyQ` but not yet
  durable.
- `body_download_floor`: highest contiguous body downloaded/drained.
- `verified_block_tip`: durable verified tip known to the sequencer.

When a contiguous prefix exists above the floor:

```rust
for drained in self.sequencer.drain_ready_into_applying(self.apply_epoch) {
    self.apply_tx.send(ApplyItem {
        height,
        hash,
        block,
        bytes,
        source_peer,
        epoch,
    });
}
```

`drain_ready_into_applying()` moves bodies from `ReorderBuffer` to the `applying`
ledger and advances the body download floor. The block `Arc` leaves the network
crate through `applyQ`; the sequencer retains only metadata/bytes for later
release or reset.

The task publishes a latest-wins `SequencerView` after body/control inputs. The
reactor and peer routines read this view for floor, reset epoch, buffering
counts, and throughput.

### 7. Committer Applies Bodies Through Consensus

The `Committer` drains `applyQ` in `zebrad`, not `zebra-network`, because it must
call the consensus verifier:

```rust
verifier.oneshot(zebra_consensus::Request::Commit(block))
```

It fires commit futures concurrently into `FuturesUnordered`:

```rust
self.in_flight.push(async move {
    let result = commit_one(verifier, block, class, &trace, commit_seq, height, hash, probe).await;
    CommitOutcome { meta, result }
}.boxed());
```

This is required for checkpoint verifier batching; a serial "submit one, wait
for it" loop can deadlock waiting for the rest of a checkpoint batch.

On success (`Committed` or `Duplicate`) the committer updates only its local
marker. It does not release byte budget. Durability feedback releases bytes.

On failure (`Rejected` or `TimedOut`) it reports:

```rust
self.reset_sink.report_commit_rejected(CommitterReset {
    height: meta.height,
    epoch: meta.epoch,
    source_peer: meta.source_peer,
    rejection,
});
```

Failures are coalesced, not reported per block: within one apply epoch the
committer raises a single `CommitterReset` at the *lowest* failing height
(height-aware lowest-wins in `on_commit_error`). A checkpoint range that
batch-rejects therefore rolls back once to its lowest invalid height; later
in-flight `Err`s for the same range, and stale failures from before the reset,
are dropped by the epoch guard.

### 8. Durable Frontier Releases Held Bytes

`drive_block_sync_durable_frontier()` waits for `ChainTipChange`, queries
`FinalizedTip` and `Tip` (via `query_block_sync_frontiers`), computes
`BlockSyncFrontiers`, then calls:

```rust
block_sync.report_durable_frontier(frontiers);
```

The watcher is edge-triggered on `ChainTipChange`, so a frontier read that
returns `None` is retried up to `DURABLE_FRONTIER_READ_MAX_ATTEMPTS` (5) with a
200 ms delay (shutdown-interruptible). Without the retry, a dropped read on the
*final* advance would strand the last range's held bytes — there is no later
tick to re-derive it from, since the 200 ms refresh poll was deleted.

That sends `SequencerControlInput::FrontierAdvance { release_applied: true }`.
The sequencer:

- advances verified tip and download floor,
- drops superseded reorder bodies through the new tip,
- releases held `applying` bytes through the durable tip,
- calls `work.advance_floor(tip)`,
- publishes `SequencerView`.

The reactor observes the `SequencerView` reaction epoch, updates its mirrors,
prunes `needed_heights`, refreshes status if the servable tip changed, and asks
state for more missing bodies if local work is below low-water.

## Serving Other Peers

Serving is a reactor/global concern because it needs state reads and local
serving slots.

Flow:

```text
peer sends GetBlocks
  -> PeerRoutine::handle_frame
  -> RoutineToReactor::ServeGetBlocks
  -> BlockSyncReactor::handle_get_blocks
  -> BlockSyncAction::QueryBlocksByHeightRange
  -> drive_block_sync_actions reads state BlocksByHeightRange
  -> BlockRangeResponseReady
  -> reactor sends Block frames and BlocksDone / RangeUnavailable
```

The reactor validates that the peer exists, has sent `Status`, is within inbound
serving budget, and asks only for a clamped range. Actual block frames are queued
through `BlockSyncPeerSession::try_send_block`.

## Reset And Failure Paths

### Commit Rejection

`CommitterReset` goes to `SequencerTask::handle_commit_rejected()`:

- ignored if the failed height no longer has the same apply epoch,
- release held `applying` entries at/above the failed height,
- roll the download floor below the failed height but not below verified tip,
- `work.reset_above(floor)`,
- drop reorder bodies at/above failed height,
- bump apply epoch,
- emit `Misbehavior::InvalidBlock` for invalid consensus rejection,
- drain any newly contiguous bodies.

The resulting view update bumps `reaction_epoch`; a destructive reset also bumps
`reset_epoch`. Peer routines observe `reset_epoch` and clear outstanding in
place, returning unreceived work.

### Frontier Reset / Reorg

The reactor receives a `FrontierUpdate` whose `change` is
`FrontierChange::VerifiedReset` or `FrontierChange::HeaderReanchored` and sends
`SequencerControlInput::FrontierReset`. (`FrontierUpdate` is the struct carried by
the watch; `FrontierChange` is its `change` enum.) It precomputes the
peer-outstanding parts of the reset decision because peer facts live outside the
sequencer.

The sequencer decides whether the reset is actually growth-classified or
destructive. A destructive reset clears reorder/applying, resets the work queue
above the new tip, bumps apply epoch, and increments `reset_epoch`.

### Congestion Control And Peer Liveness

These are peer-local in `PeerRoutine` and `DownloadWindow`:

- `request_timeout` is a rescue timer, not a peer-eviction timer.
- Request timeouts return unreceived heights to `WorkQueue`.
- `DownloadWindow` controls adaptive concurrency: cubic growth after successful
  responses, cubic backoff after timeout batches, with a floor of 1.
- `timeout_recovery_slots` can replace timed-out requests above the floor, but
  never create extra floor concurrency.
- Peer eviction is progress-based: while outstanding requests exist, the peer
  must deliver at least one accepted full block within 32s by default
  (`request_timeout * 4`).
- Idle peers are never disconnected by block-sync liveness.
- Received-and-buffered heights stay in-flight until the floor advances.
- The routine publishes updated outstanding state to `PeerRegistry`.

### Floor Watchdog

The reactor has a watchdog for the next height above `request_floor`. If a
published outstanding claim expires, the reactor can clear that claim, release
its reserved bytes, return the height, and optionally avoid that peer for the
height briefly. This is a central backstop; normal request timeouts are still
peer-local.

## What Each File Is For

- `block_sync/mod.rs`: module map and public re-exports. Its comment says to
  start in `pipe.rs`.
- `pipe.rs`: high-level reading guide and ingress guard.
- `wire.rs`: stream-6 message encoding/decoding and size/count validation.
- `config.rs`: local block-sync config and advertised caps.
- `events.rs`: public node/reactor events/actions plus private
  `RoutineToReactor`.
- `state.rs`: startup/handle types, reactor state, peer serving state,
  `DownloadWindow`, outstanding range, budget ledger, rate/throughput meters.
- `service.rs`: Zakura service implementation; takes stream-6, spawns
  `PeerRoutine`, sends lifecycle events.
- `peer_routine.rs`: per-peer core: decode, status, issue work, match bodies,
  timeout/retry, forward bodies.
- `peer_registry.rs`: generation-gated cross-peer fact table shared by routines
  and reactor.
- `work_queue.rs`: shared pending/in-flight height set and per-height byte
  ledger.
- `admission.rs`: admission/congestion decision for above-floor speculative
  buffering vs floor rescue.
- `request.rs`: block range request and per-height expected metadata.
- `reorder.rs`: reorder buffer for received bodies above the floor.
- `sequencer.rs`: pure reorder/applying/floor state machine.
- `sequencer_task.rs`: async owner of `Sequencer`, channels, view publication,
  `applyQ`, durable release, commit rejection reset.
- `apply_item.rs`: cross-crate apply seam (`ApplyItem`, `CommitterReset`).
- `reactor.rs`: global coordination hub.
- `zebrad/src/commands/start/zakura/block_sync_driver.rs`: node-side state-read
  driver and durable-frontier watcher.
- `zebrad/src/commands/start/zakura/committer.rs`: node-side commit pump.

> Note: we should rename the blocksync driver to block sync state adapter perhaps. it not "driving" anything.

## Key Invariants To Preserve

- Each height is in exactly one download state: below floor, `pending`, or
  `in_flight`.
- Held body bytes stay charged until the durable frontier crosses the height.
- `WorkQueue.in_flight` is the structural "held or outstanding" marker used to
  avoid duplicate scheduling.
- `PeerRegistry.outstanding` contains only unreceived requested heights, not
  received/buffered bodies.
- Peer routines never hold registry/work mutexes across `.await`.
- The reactor should not block on action sends; most sends are `try_send`.
- Sequencer control events must not wait behind body backlog.
- The `applyQ` is unbounded because the byte budget, not queue length, is the
  memory bound.
- Committer fires commits concurrently; serial commit can deadlock checkpoint
  verification.
- Commit rejection/reset is epoch-guarded; stale apply items and stale resets are
  discarded.
- Misbehavior is record-only in the current code; it no longer drives disconnects.

> TODO: clarify the difference between the committer and the block_sync_driver. Perhaps we could actually combine them? 

## Minimal Mental Model

If you are changing the fetch side, start with `PeerRoutine`, `WorkQueue`,
`PeerRegistry`, and `SequencerView`. The reactor is mostly the producer and
serving hub.

If you are changing apply/commit, start with `SequencerTask`, `Sequencer`,
`ApplyItem`, and `Committer`. The byte budget is released by durable frontier
advance, not by verifier success.

If you are changing startup/wiring, inspect `handler.rs` for reactor/service
construction and `start.rs` for the three zebrad tasks that attach state,
verifier, and durable frontier feedback.
