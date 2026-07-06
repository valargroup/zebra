use super::{state::*, wire::*, *};

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

#[derive(Clone, Debug)]
pub(super) struct RangeScheduler {
    pub(super) forward: VecDeque<RangeRequest>,
    pub(super) backfill: VecDeque<RangeRequest>,
    pub(super) assigned: HashMap<RangeRequest, HashSet<ZakuraPeerId>>,
    pub(super) covered: Vec<CoveredRange>,
}

impl RangeScheduler {
    pub(super) fn new() -> Self {
        Self {
            forward: VecDeque::new(),
            backfill: VecDeque::new(),
            assigned: HashMap::new(),
            covered: Vec::new(),
        }
    }

    pub(super) fn ensure_forward(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Forward);
    }

    pub(super) fn ensure_backfill(&mut self, range: RangeRequest) {
        self.ensure(range, RangePriority::Backfill);
    }

    pub(super) fn ensure(&mut self, range: RangeRequest, priority: RangePriority) {
        // Covered intervals track the forward frontier's committed spans; backfill progress is
        // tracked by the backfill frontier instead, and the sync-start stitch range deliberately
        // overlaps forward-covered heights, so backfill ranges bypass the covered check.
        if (priority == RangePriority::Forward && self.is_covered(range))
            || self.assigned.contains_key(&range)
            || self.assigned.keys().any(|assigned| {
                assigned.start_height == range.start_height && assigned.priority == priority
            })
        {
            return;
        }
        let queue = match priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backfill => &mut self.backfill,
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
        peer: &PeerHeaderState,
    ) -> Option<RangeRequest> {
        Self::pop_assignable(&mut self.forward, &self.assigned, peer_id, peer)
            .or_else(|| Self::pop_assignable(&mut self.backfill, &self.assigned, peer_id, peer))
    }

    pub(super) fn pop_assignable(
        queue: &mut VecDeque<RangeRequest>,
        assigned: &HashMap<RangeRequest, HashSet<ZakuraPeerId>>,
        peer_id: &ZakuraPeerId,
        peer: &PeerHeaderState,
    ) -> Option<RangeRequest> {
        let index = queue.iter().position(|range| {
            range.end_height() <= peer.advertised_tip
                && assigned.get(range).is_none_or(|peers| {
                    peers.len() < HEADER_SYNC_FANOUT && !peers.contains(peer_id)
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

    pub(super) fn mark_assigned(&mut self, peer: ZakuraPeerId, range: RangeRequest) {
        self.assigned.entry(range).or_default().insert(peer);
    }

    pub(super) fn narrow_queued_range(&mut self, original: RangeRequest, narrowed: RangeRequest) {
        if original == narrowed {
            return;
        }

        let queue = match original.priority {
            RangePriority::Forward => &mut self.forward,
            RangePriority::Backfill => &mut self.backfill,
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
        match range.priority {
            RangePriority::Forward => {
                if self.is_covered(range) {
                    return;
                }
                self.forward.push_front(range);
            }
            // Backfill ranges are exempt from covered checks (see `ensure`).
            RangePriority::Backfill => self.backfill.push_front(range),
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

    pub(super) fn retire_request(&mut self, range: RangeRequest) {
        match range.priority {
            RangePriority::Forward => self.forward.retain(|queued| *queued != range),
            RangePriority::Backfill => self.backfill.retain(|queued| *queued != range),
        }
        self.clear_assignment(range);
    }

    pub(super) fn clear_forward(&mut self) {
        self.forward.clear();
        self.assigned
            .retain(|range, _| range.priority != RangePriority::Forward);
    }

    /// Drops queued and assigned backfill ranges that ended at or below the backfill frontier.
    ///
    /// Backfill ranges are exempt from the forward covered machinery (see `ensure`), so this is
    /// their staleness rule: once the frontier passed a range's end (a fanout duplicate or a
    /// timed-out retry of an already-committed bracket), redelivering it can only mismatch the
    /// advanced backfill tree.
    pub(super) fn retire_stale_backfill(&mut self, backfill_tip: block::Height) {
        self.backfill
            .retain(|range| range.end_height() > backfill_tip);
        self.assigned.retain(|range, _| {
            range.priority != RangePriority::Backfill || range.end_height() > backfill_tip
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
        // Backfill ranges are exempt from covered pruning (see `ensure`).
        self.forward.retain(|range| !is_covered(range));
        self.assigned
            .retain(|range, _| range.priority == RangePriority::Backfill || !is_covered(range));
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

    pub(super) fn is_ready(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }

    pub(super) fn mark_taken(&mut self, now: Instant) {
        self.next_allowed = now + self.interval;
    }
}
