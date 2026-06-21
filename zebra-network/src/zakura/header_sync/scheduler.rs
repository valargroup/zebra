use std::sync::Mutex;

use tokio::sync::Notify;

use super::{config::*, state::*, wire::*, *};

/// Per-peer caps a routine carries when it pulls work from the shared queue.
///
/// These are the routine-local clamped advertisement values learned from this
/// peer's `Status` (advertised tip and per-response cap) — the same facts the
/// reactor's old push-path read off `PeerHeaderState` before sending `GetHeaders`.
#[derive(Copy, Clone, Debug)]
pub(super) struct PeerPullCaps {
    /// Highest height this peer has advertised; work is never assigned beyond it.
    pub(super) advertised_tip: block::Height,
    /// This peer's clamped advertised `max_headers_per_response`.
    pub(super) max_headers_per_response: u32,
}

/// One narrowed, count-clamped range a routine took from the shared queue and is
/// about to request with a single `GetHeaders`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct HeaderWork {
    /// The (possibly narrowed) range to request.
    pub(super) range: RangeRequest,
    /// The clamped `GetHeaders.count` — the `expected_max_count` of the response.
    pub(super) count: u32,
}

/// Why a routine returned a previously-taken range to the shared queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ReturnReason {
    /// The outbound `GetHeaders` send failed; the range was never requested.
    SendFailed,
    /// The peer answered with an empty `Headers`; re-fan after a short delay.
    EmptyResponse,
    /// The routine is tearing down (stream close, cancel, or panic); release the
    /// range so another peer can take it instead of stranding it.
    Teardown,
    /// The reactor timed the request out and asked the routine to drop it.
    Timeout,
}

#[derive(Clone, Debug, Default)]
pub(super) struct HeaderHashDedup {
    pub(super) hashes: HashSet<block::Hash>,
    pub(super) order: VecDeque<block::Hash>,
}

impl HeaderHashDedup {
    pub(super) fn contains(&self, hash: &block::Hash) -> bool {
        self.hashes.contains(hash)
    }

    pub(super) fn insert(&mut self, hash: block::Hash) -> bool {
        if !self.hashes.insert(hash) {
            return false;
        }
        self.order.push_back(hash);
        while self.order.len() > HEADER_SYNC_SEEN_HASH_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.hashes.remove(&oldest);
            }
        }
        true
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingCommitKey {
    pub(super) peer: ZakuraPeerId,
    pub(super) start_height: block::Height,
    pub(super) count: u32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CoveredRange {
    pub(super) start: block::Height,
    pub(super) end: block::Height,
}

/// The set of peers currently assigned a range, each tagged with the session
/// generation that holds it. Generation tagging lets a stale routine's return or
/// reset target only the session that actually took the work, so an old session's
/// teardown cannot clear a newer reconnected session's live assignment.
type AssignedPeers = HashMap<ZakuraPeerId, u64>;

#[derive(Clone, Debug)]
pub(super) struct RangeScheduler {
    pub(super) forward: VecDeque<RangeRequest>,
    pub(super) backward: VecDeque<RangeRequest>,
    pub(super) assigned: HashMap<RangeRequest, AssignedPeers>,
    pub(super) covered: Vec<CoveredRange>,
}

impl RangeScheduler {
    pub(super) fn new() -> Self {
        Self {
            forward: VecDeque::new(),
            backward: VecDeque::new(),
            assigned: HashMap::new(),
            covered: Vec::new(),
        }
    }

    pub(super) fn ensure_forward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Forward);
    }

    pub(super) fn ensure_backward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Backward);
    }

    pub(super) fn ensure(&mut self, range: RangeRequest, priority: RangePriority) {
        if self.is_covered(range)
            || self.assigned.contains_key(&range)
            || self.assigned.keys().any(|assigned| {
                assigned.start_height == range.start_height && assigned.priority == priority
            })
        {
            return;
        }
        let queue = match priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backward => &mut self.backward,
        };
        if !queue.contains(&range)
            && !queue.iter().any(|queued| {
                queued.start_height == range.start_height && queued.priority == priority
            })
        {
            queue.push_back(range);
        }
    }

    pub(super) fn next_for_peer(
        &mut self,
        peer_id: &ZakuraPeerId,
        advertised_tip: block::Height,
    ) -> Option<RangeRequest> {
        Self::pop_assignable(&mut self.forward, &self.assigned, peer_id, advertised_tip).or_else(
            || Self::pop_assignable(&mut self.backward, &self.assigned, peer_id, advertised_tip),
        )
    }

    pub(super) fn pop_assignable(
        queue: &mut VecDeque<RangeRequest>,
        assigned: &HashMap<RangeRequest, AssignedPeers>,
        peer_id: &ZakuraPeerId,
        advertised_tip: block::Height,
    ) -> Option<RangeRequest> {
        let index = queue.iter().position(|range| {
            range.end_height() <= advertised_tip
                && assigned.get(range).is_none_or(|peers| {
                    peers.len() < HEADER_SYNC_FANOUT && !peers.contains_key(peer_id)
                })
        })?;
        let range = queue[index];
        if assigned
            .get(&range)
            .is_some_and(|peers| peers.len() + 1 >= HEADER_SYNC_FANOUT)
        {
            queue.remove(index);
        }
        Some(range)
    }

    pub(super) fn mark_assigned(
        &mut self,
        peer: ZakuraPeerId,
        generation: u64,
        range: RangeRequest,
    ) {
        self.assigned
            .entry(range)
            .or_default()
            .insert(peer, generation);
    }

    /// Pull, narrow, count-clamp, and assign one eligible range for this peer.
    ///
    /// This is the routine-side replacement for the reactor's old push loop: it
    /// reproduces the exact range math the reactor ran before sending `GetHeaders`
    /// — pick the highest-priority assignable range bounded by the peer's
    /// advertised tip, clamp its count against the peer/hard/byte/frame caps, skip
    /// (and re-queue) a finalized range that would be shrunk, rewrite the queued
    /// entry to the narrowed range, and record the assignment under this session's
    /// generation. Returns `None` when there is no eligible work for this peer.
    pub(super) fn take_for_peer(
        &mut self,
        peer_id: &ZakuraPeerId,
        generation: u64,
        caps: PeerPullCaps,
        network: &Network,
        max_frame_bytes: u32,
    ) -> Option<HeaderWork> {
        let original_range = self.next_for_peer(peer_id, caps.advertised_tip)?;
        let count = clamp_header_sync_request_count(
            original_range.count,
            caps.max_headers_per_response,
            network,
            max_frame_bytes,
        );
        // A finalized/checkpoint range must be requested whole. If the clamp would
        // shrink it, do not request a partial range — re-queue it for a peer/frame
        // that can serve the full bracket, exactly as the old reactor push did.
        if original_range.finalized && count < original_range.count {
            self.retry(original_range);
            return None;
        }
        let mut range = original_range;
        range.count = count;
        self.narrow_queued_range(original_range, range);
        self.mark_assigned(peer_id.clone(), generation, range);
        Some(HeaderWork { range, count })
    }

    /// Return a previously-taken range to the queue after a non-fatal outcome
    /// (failed send, empty response, teardown, or reactor timeout), so it becomes
    /// eligible for other peers (or this peer on a later attempt) instead of being
    /// stranded.
    ///
    /// The clear is generation-scoped: a return whose generation no longer matches
    /// this peer's recorded assignment is ignored, so a late teardown of an OLD
    /// session cannot drop a reconnected session's live work.
    ///
    /// Reason chooses whether this peer's assignment is also cleared, preserving the
    /// reactor's old timeout semantics exactly:
    /// - `Timeout`: retry the range but KEEP this peer's assignment, mirroring the
    ///   old normal-timeout path (`clear_assignment_on_timeout = false`): the
    ///   timed-out peer must not immediately re-grab the range; another peer takes
    ///   it. The peer's routine drops its expectation separately on `DropExpectation`.
    /// - `EmptyResponse`/`SendFailed`/`Teardown`: clear this peer's assignment so the
    ///   range re-fans freely (the old empty-retry `clear_assignment_on_timeout =
    ///   true` path, plus the never-strand exit paths the inversion adds).
    ///
    /// Either way the range is pushed to the front of its priority queue, preserving
    /// forward/backward priority and immediate re-assignability.
    pub(super) fn return_work(
        &mut self,
        peer_id: &ZakuraPeerId,
        generation: u64,
        range: RangeRequest,
        reason: ReturnReason,
    ) {
        // A generation mismatch means a newer session now holds (or never held)
        // this assignment; an old session's return must not disturb it.
        let belongs_to_this_session = self
            .assigned
            .get(&range)
            .and_then(|peers| peers.get(peer_id))
            == Some(&generation);
        if belongs_to_this_session && reason != ReturnReason::Timeout {
            if let Some(peers) = self.assigned.get_mut(&range) {
                peers.remove(peer_id);
                if peers.is_empty() {
                    self.assigned.remove(&range);
                }
            }
        }
        self.retry(range);
    }

    pub(super) fn narrow_queued_range(&mut self, original: RangeRequest, narrowed: RangeRequest) {
        if original == narrowed {
            return;
        }

        let queue = match original.priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backward => &mut self.backward,
        };
        for queued in queue {
            if *queued == original {
                *queued = narrowed;
                break;
            }
        }
        if let Some(peers) = self.assigned.remove(&original) {
            self.assigned.entry(narrowed).or_default().extend(peers);
        }
    }

    pub(super) fn retry(&mut self, range: RangeRequest) {
        if self.is_covered(range) {
            return;
        }
        match range.priority {
            RangePriority::Forward => self.forward.push_front(range),
            RangePriority::Backward => self.backward.push_front(range),
        }
    }

    pub(super) fn forget_peer(&mut self, peer: &ZakuraPeerId) {
        for peers in self.assigned.values_mut() {
            peers.remove(peer);
        }
        self.assigned.retain(|_, peers| !peers.is_empty());
    }

    pub(super) fn clear_assignment(&mut self, range: RangeRequest) {
        self.assigned.remove(&range);
    }

    /// Drop queued and assigned forward work whose range ends above `height`.
    ///
    /// Used by a reset/reanchor that re-bases the best-header target to `height`:
    /// forward ranges past the new target are no longer wanted, so they are
    /// removed from the forward queue and their assignments cleared. Backward and
    /// covered state are untouched (reset is forward-only, matching reanchor).
    pub(super) fn reset_above(&mut self, height: block::Height) {
        self.forward.retain(|range| range.end_height() <= height);
        self.assigned.retain(|range, _| {
            range.priority != RangePriority::Forward || range.end_height() <= height
        });
    }

    pub(super) fn mark_height_covered(&mut self, height: block::Height) {
        self.mark_covered_interval(CoveredRange {
            start: height,
            end: height,
        });
        self.prune_covered();
    }

    pub(super) fn mark_range_covered(&mut self, start: block::Height, end: block::Height) {
        self.mark_covered_interval(CoveredRange { start, end });
        self.prune_covered();
    }

    pub(super) fn is_covered(&self, range: RangeRequest) -> bool {
        let end = range.end_height();
        self.covered
            .iter()
            .any(|covered| covered.start <= range.start_height && covered.end >= end)
    }

    pub(super) fn mark_covered_interval(&mut self, mut interval: CoveredRange) {
        if interval.end < interval.start {
            return;
        }

        let mut merged = Vec::with_capacity(self.covered.len().saturating_add(1));
        let mut inserted = false;
        for covered in self.covered.drain(..) {
            if covered.end.0.saturating_add(1) < interval.start.0 {
                merged.push(covered);
            } else if interval.end.0.saturating_add(1) < covered.start.0 {
                if !inserted {
                    merged.push(interval);
                    inserted = true;
                }
                merged.push(covered);
            } else {
                interval.start = interval.start.min(covered.start);
                interval.end = interval.end.max(covered.end);
            }
        }
        if !inserted {
            merged.push(interval);
        }
        self.covered = merged;
    }

    pub(super) fn prune_covered(&mut self) {
        let covered = self.covered.clone();
        let is_covered = |range: &RangeRequest| {
            let end = range.end_height();
            covered
                .iter()
                .any(|covered| covered.start <= range.start_height && covered.end >= end)
        };
        self.forward.retain(|range| !is_covered(range));
        self.backward.retain(|range| !is_covered(range));
        self.assigned.retain(|range, _| !is_covered(range));
    }
}

/// The shared header-sync range queue, reachable from both the reactor (the
/// producer of work via `ensure_*`/`mark_covered`/`reset_above` and the consumer
/// of coverage) and every per-peer routine (the consumer of work via
/// `take_for_peer`/`return_work`).
///
/// The inner [`RangeScheduler`] is a purely synchronous data structure, so a
/// plain [`std::sync::Mutex`] suffices and a guard is never held across an
/// `.await`: every method locks, runs the synchronous range math, and drops the
/// guard before notifying. A [`Notify`] wakes parked routines whenever a mutation
/// may have produced newly assignable work, so routines pull lazily instead of
/// the reactor pushing.
#[derive(Clone, Debug)]
pub(super) struct SharedHeaderRangeQueue {
    inner: Arc<Mutex<RangeScheduler>>,
    wake: Arc<Notify>,
}

impl SharedHeaderRangeQueue {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RangeScheduler::new())),
            wake: Arc::new(Notify::new()),
        }
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, RangeScheduler> {
        self.inner
            .lock()
            .expect("header-sync range queue mutex is never poisoned: guarded sections never panic")
    }

    /// Wake every routine currently parked waiting for work. Called after any
    /// mutation that may have made a range assignable.
    pub(super) fn wake(&self) {
        self.wake.notify_waiters();
    }

    /// Wait until a mutation signals that work may be available.
    ///
    /// A routine registers as a waiter, then re-checks via `take_for_peer`. A
    /// missed wake (notified while the routine was busy) self-heals because the
    /// routine also re-attempts a pull after every loop iteration.
    pub(super) async fn waked(&self) {
        self.wake.notified().await;
    }

    pub(super) fn ensure_forward(&self, range: RangeRequest) {
        self.locked().ensure_forward(range);
        self.wake();
    }

    pub(super) fn ensure_backward(&self, range: RangeRequest) {
        self.locked().ensure_backward(range);
        self.wake();
    }

    /// Routine entry: pull, narrow, clamp, and assign one eligible range. Does not
    /// wake (the caller is itself a consumer); other parked routines are woken by
    /// the producer mutations.
    pub(super) fn take_for_peer(
        &self,
        peer_id: &ZakuraPeerId,
        generation: u64,
        caps: PeerPullCaps,
        network: &Network,
        max_frame_bytes: u32,
    ) -> Option<HeaderWork> {
        self.locked()
            .take_for_peer(peer_id, generation, caps, network, max_frame_bytes)
    }

    /// Routine entry: return a previously-taken range so it re-fans to other
    /// peers. Wakes parked routines since a returned range is immediately
    /// assignable again.
    pub(super) fn return_work(
        &self,
        peer_id: &ZakuraPeerId,
        generation: u64,
        range: RangeRequest,
        reason: ReturnReason,
    ) {
        self.locked()
            .return_work(peer_id, generation, range, reason);
        self.wake();
    }

    pub(super) fn forget_peer(&self, peer: &ZakuraPeerId) {
        self.locked().forget_peer(peer);
        self.wake();
    }

    pub(super) fn clear_assignment(&self, range: RangeRequest) {
        self.locked().clear_assignment(range);
        self.wake();
    }

    pub(super) fn retry(&self, range: RangeRequest) {
        self.locked().retry(range);
        self.wake();
    }

    pub(super) fn reset_above(&self, height: block::Height) {
        self.locked().reset_above(height);
    }

    pub(super) fn mark_height_covered(&self, height: block::Height) {
        self.locked().mark_height_covered(height);
    }

    pub(super) fn mark_range_covered(&self, start: block::Height, end: block::Height) {
        self.locked().mark_range_covered(start, end);
    }

    pub(super) fn is_covered(&self, range: RangeRequest) -> bool {
        self.locked().is_covered(range)
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> RangeScheduler {
        self.locked().clone()
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
    }

    fn forward_range(start: u32, count: u32) -> RangeRequest {
        RangeRequest {
            start_height: block::Height(start),
            count,
            anchor_hash: block::Hash([1; 32]),
            finalized: false,
            priority: RangePriority::Forward,
        }
    }

    fn backward_range(start: u32, count: u32) -> RangeRequest {
        RangeRequest {
            start_height: block::Height(start),
            count,
            anchor_hash: block::Hash([2; 32]),
            finalized: false,
            priority: RangePriority::Backward,
        }
    }

    fn caps(tip: u32, max_headers: u32) -> PeerPullCaps {
        PeerPullCaps {
            advertised_tip: block::Height(tip),
            max_headers_per_response: max_headers,
        }
    }

    fn take(
        queue: &mut RangeScheduler,
        peer: &ZakuraPeerId,
        generation: u64,
        caps: PeerPullCaps,
    ) -> Option<HeaderWork> {
        queue.take_for_peer(
            peer,
            generation,
            caps,
            &Network::Mainnet,
            LOCAL_MAX_MESSAGE_BYTES,
        )
    }

    /// Forward work is offered before backward work to the same eligible peer, and
    /// only ranges within the peer's advertised tip are ever assigned.
    #[test]
    fn take_for_peer_prefers_forward_and_respects_advertised_tip() {
        let mut queue = RangeScheduler::new();
        queue.ensure_backward(backward_range(1, 3));
        queue.ensure_forward(forward_range(10, 2));
        let peer = pid(1);

        // Tip below the forward range's end: only backward is eligible.
        let low_tip = take(&mut queue, &peer, 1, caps(3, 100)).expect("backward is within tip");
        assert_eq!(low_tip.range.priority, RangePriority::Backward);
        assert_eq!(low_tip.range.start_height, block::Height(1));

        // A second peer with a tip covering the forward range gets forward first.
        let peer2 = pid(2);
        let forward = take(&mut queue, &peer2, 1, caps(100, 100)).expect("forward is within tip");
        assert_eq!(forward.range.priority, RangePriority::Forward);
        assert_eq!(forward.range.start_height, block::Height(10));
    }

    /// The same range is never assigned twice to one peer, but configured fanout
    /// hands it to up to `HEADER_SYNC_FANOUT` distinct peers.
    #[test]
    fn take_for_peer_dedups_per_peer_and_fans_out_to_distinct_peers() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));

        let first = pid(1);
        let work = take(&mut queue, &first, 1, caps(100, 100)).expect("first take succeeds");
        assert_eq!(work.range.start_height, block::Height(1));
        // The same peer cannot re-take the same range.
        assert!(
            take(&mut queue, &first, 1, caps(100, 100)).is_none(),
            "a peer must not be assigned the same range twice"
        );

        // Distinct peers can take it, up to the fanout cap.
        let second = pid(2);
        let third = pid(3);
        assert!(take(&mut queue, &second, 1, caps(100, 100)).is_some());
        assert!(take(&mut queue, &third, 1, caps(100, 100)).is_some());
        // A fourth distinct peer is beyond the fanout cap.
        let fourth = pid(4);
        assert!(
            take(&mut queue, &fourth, 1, caps(100, 100)).is_none(),
            "fanout is capped at HEADER_SYNC_FANOUT distinct peers"
        );
    }

    /// The outbound count is clamped by the peer's advertised cap.
    #[test]
    fn take_for_peer_clamps_count_to_peer_cap() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 50));
        let peer = pid(1);
        let work = take(&mut queue, &peer, 1, caps(100, 5)).expect("take succeeds");
        assert_eq!(
            work.count, 5,
            "count is clamped to the peer's advertised cap"
        );
    }

    /// A finalized range that would be shrunk by the peer cap is re-queued, not
    /// requested partially.
    #[test]
    fn take_for_peer_skips_finalized_range_that_would_shrink() {
        let mut queue = RangeScheduler::new();
        let mut range = forward_range(1, 50);
        range.finalized = true;
        queue.ensure_forward(range);
        let peer = pid(1);
        // Cap 5 would shrink the 50-count finalized range: must not be assigned.
        assert!(
            take(&mut queue, &peer, 1, caps(100, 5)).is_none(),
            "a finalized range that would shrink is not requested partially"
        );
        // It remains in the queue for a peer/frame that can serve it whole.
        let work = take(&mut queue, &peer, 1, caps(100, 100)).expect("full-cap peer takes it");
        assert_eq!(work.count, 50);
    }

    /// Returning timed-out work keeps the original peer's assignment (so it does not
    /// immediately re-grab it) while letting another peer take it; other return
    /// reasons clear the assignment so the same peer can re-take.
    #[test]
    fn return_work_timeout_keeps_assignment_other_reasons_clear_it() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));
        let first = pid(1);
        let work = take(&mut queue, &first, 1, caps(100, 100)).expect("take");

        // Timeout return: the range is re-queued but stays assigned to `first`.
        queue.return_work(&first, 1, work.range, ReturnReason::Timeout);
        assert!(
            take(&mut queue, &first, 1, caps(100, 100)).is_none(),
            "a timed-out peer must not immediately re-grab its own range"
        );
        let second = pid(2);
        assert!(
            take(&mut queue, &second, 1, caps(100, 100)).is_some(),
            "a different peer takes the timed-out range"
        );

        // Send-failed return clears the assignment, so the same peer can re-take.
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));
        let work = take(&mut queue, &first, 2, caps(100, 100)).expect("take");
        queue.return_work(&first, 2, work.range, ReturnReason::SendFailed);
        assert!(
            take(&mut queue, &first, 2, caps(100, 100)).is_some(),
            "a send-failed range is re-takeable by the same peer"
        );
    }

    /// A stale-generation return must not clear a newer session's live assignment.
    #[test]
    fn return_work_is_generation_scoped() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));
        let peer = pid(1);

        // The new session (generation 2) holds the range.
        let work = take(&mut queue, &peer, 2, caps(100, 100)).expect("take");
        // An OLD session (generation 1) returns the same range: must be ignored, so
        // the new session keeps its hold (the same peer cannot re-take).
        queue.return_work(&peer, 1, work.range, ReturnReason::Teardown);
        assert!(
            take(&mut queue, &peer, 2, caps(100, 100)).is_none(),
            "an old generation's return must not free the new session's assignment"
        );
    }

    /// Covering a range removes it from the queue and assignments (no over-fetch),
    /// while leaving uncovered ranges takeable (no stranding).
    #[test]
    fn mark_covered_prevents_over_fetch_without_stranding() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));
        queue.ensure_forward(forward_range(2, 1));
        let peer = pid(1);

        queue.mark_range_covered(block::Height(1), block::Height(1));
        // The covered range is never re-fetched; the next take returns the uncovered
        // range instead of the covered one.
        let work = take(&mut queue, &peer, 1, caps(100, 100)).expect("uncovered range remains");
        assert_eq!(
            work.range.start_height,
            block::Height(2),
            "a covered range is not re-fetched, the uncovered one is still served"
        );
    }

    /// `reset_above` drops forward work above the new target and clears its
    /// assignment, while leaving lower forward work intact.
    #[test]
    fn reset_above_drops_forward_work_past_the_target() {
        let mut queue = RangeScheduler::new();
        queue.ensure_forward(forward_range(1, 1));
        queue.ensure_forward(forward_range(10, 1));
        let peer = pid(1);
        let _high = take(&mut queue, &peer, 1, caps(100, 100));

        queue.reset_above(block::Height(5));
        let snapshot = queue.clone();
        assert!(
            snapshot
                .forward
                .iter()
                .all(|range| range.end_height() <= block::Height(5)),
            "forward work above the reset target is dropped"
        );
        assert!(
            !snapshot
                .assigned
                .keys()
                .any(|range| range.priority == RangePriority::Forward
                    && range.end_height() > block::Height(5)),
            "assignments above the reset target are cleared"
        );
    }
}
