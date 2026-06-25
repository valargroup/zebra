//! Download work source for Zakura block sync.
//!
//! The [`WorkQueue`] is the sole shared download-scheduling primitive: a sorted
//! set of needed block heights the per-peer issuance path pulls from. It replaces
//! the central `BlockRangeScheduler`'s eligibility/dedup/retry roles with a small
//! API the caller drives from its own per-peer state (see the ):
//!
//! - a height is in **exactly one** of `{below-floor (gone), pending, in_flight}`;
//! - [`take_in_range`](WorkQueue::take_in_range) moves a contiguous-ascending run
//!   `pending → in_flight` (so one taken chunk maps to one `BlockRangeRequest`),
//!   bounded only by the caller's servable range and a count cap — never by how
//!   far above the committed floor the heights already are;
//! - only [`return_items`](WorkQueue::return_items) (timeout/disconnect retry) and
//!   [`reset_above`](WorkQueue::reset_above) move `in_flight → pending`;
//! - [`advance_floor`](WorkQueue::advance_floor) is garbage collection only — the
//!   committed floor never throttles the fetch decision.
//!
//! Internals are a brief `std::sync::Mutex` whose critical sections are tiny map
//! splices held **never across `.await`** (the anti-block rule). `estimated_bytes`
//! on a [`WorkItem`] is the block's size *estimate* (not its worst-case
//! reservation); it exists only to carry the `SizeMismatch` tolerance check
//! through to the reactor's receive path and request budget.

use std::sync::Mutex as StdMutex;

use tokio::sync::Notify;
use zebra_chain::block;

use super::{request::BlockSizeEstimate, state::BlockBudgetLedger};

/// Lower clamp on a body-size estimate.
pub(super) const DEFAULT_BS_SIZE_FLOOR_BYTES: u64 = 1024;

/// Per-height download metadata held in the [`WorkQueue`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct WorkItem {
    /// Expected hash of the block at this height (drives the response match).
    pub(super) hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the
    /// receive-path `SizeMismatch` tolerance check.
    pub(super) estimated_bytes: u64,
    /// Current byte-budget charge owned by this height.
    ///
    /// Pending items normally have `Released`; issued-but-unreceived items have
    /// `Reserved(estimate)`; received bodies held by the commit pipeline have
    /// `Held(actual)`. All terminal release paths go through this ledger so a
    /// stale local owner cannot release a charge that another path already
    /// returned.
    pub(super) budget: BlockBudgetLedger,
}

#[derive(Debug)]
struct WorkQueueInner {
    pending: std::collections::BTreeMap<block::Height, WorkItem>,
    in_flight: std::collections::BTreeMap<block::Height, WorkItem>,
    floor: block::Height,
    /// Floor clamp for size estimates (overridable for tests).
    floor_estimate_bytes: u64,
}

impl WorkQueueInner {
    fn estimate_bytes(&self, estimate: BlockSizeEstimate) -> u64 {
        estimate_bytes_with(estimate, self.floor_estimate_bytes)
    }
}

/// Compute a clamped body-size estimate from a [`BlockSizeEstimate`] hint.
///
/// `Confirmed`/`Advertised` use the hinted size; `Unknown` reserves the
/// per-block worst case. The result is clamped to `[floor, MAX_BLOCK_BYTES]`.
fn estimate_bytes_with(estimate: BlockSizeEstimate, floor: u64) -> u64 {
    let hinted = match estimate {
        BlockSizeEstimate::Confirmed(size) | BlockSizeEstimate::Advertised(size) => u64::from(size),
        BlockSizeEstimate::Unknown => block::MAX_BLOCK_BYTES,
    };
    hinted.max(floor).min(block::MAX_BLOCK_BYTES)
}

/// The shared download work source. See the module docs for the invariants.
#[derive(Debug)]
pub(super) struct WorkQueue {
    inner: StdMutex<WorkQueueInner>,
    available: Notify,
}

impl WorkQueue {
    pub(super) fn new(floor: block::Height) -> Self {
        Self {
            inner: StdMutex::new(WorkQueueInner {
                pending: std::collections::BTreeMap::new(),
                in_flight: std::collections::BTreeMap::new(),
                floor,
                floor_estimate_bytes: DEFAULT_BS_SIZE_FLOOR_BYTES,
            }),
            available: Notify::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn set_estimate_floor_for_tests(&self, floor: u64) {
        let mut inner = self.lock();
        inner.floor_estimate_bytes = floor.max(1);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WorkQueueInner> {
        self.inner
            .lock()
            .expect("work queue mutex is never poisoned")
    }

    /// Add `(height, hash, size)` items to `pending`. Each is inserted iff its
    /// height is `> floor` and not already in `pending` or `in_flight`
    /// (idempotent — already-buffered/fetched heights are never re-queued).
    /// Returns the number of newly-inserted heights and wakes waiters if any.
    pub(super) fn extend(
        &self,
        items: impl IntoIterator<Item = (block::Height, block::Hash, BlockSizeEstimate)>,
    ) -> usize {
        let mut inserted = 0usize;
        {
            let mut inner = self.lock();
            for (height, hash, size) in items {
                if height <= inner.floor
                    || inner.pending.contains_key(&height)
                    || inner.in_flight.contains_key(&height)
                {
                    continue;
                }
                let estimated_bytes = inner.estimate_bytes(size);
                inner.pending.insert(
                    height,
                    WorkItem {
                        hash,
                        estimated_bytes,
                        budget: BlockBudgetLedger::Released,
                    },
                );
                inserted += 1;
            }
        }
        if inserted > 0 {
            self.available.notify_waiters();
        }
        inserted
    }

    /// Move up to `max` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, returned in ascending order.
    ///
    /// "Contiguous-ascending" stops at the first gap, so the returned chunk maps
    /// to a single `BlockRangeRequest`. `high` is the caller's `servable_high`
    /// and is **NOT** clamped to the floor (the committed floor is never an upper
    /// bound on the fetch). Returns empty if nothing is eligible.
    pub(super) fn take_in_range(
        &self,
        low: block::Height,
        high: block::Height,
        max: usize,
    ) -> Vec<(block::Height, WorkItem)> {
        if max == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut next_expected: Option<block::Height> = None;
        for (height, item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }
            taken.push((*height, *item));
            if taken.len() >= max {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        for (height, item) in &taken {
            inner.pending.remove(height);
            inner.in_flight.insert(*height, *item);
        }
        taken
    }

    /// Move up to `max_count` contiguous-ascending `pending` heights within
    /// `low..=high` from `pending` to `in_flight`, also stopping before the
    /// sum of stored size estimates would exceed `max_estimated_bytes`.
    ///
    /// The estimate cap bounds the request's summed byte reservation. To
    /// guarantee progress, the first eligible item is always taken when
    /// `max_count > 0`, even if its estimate alone exceeds the cap.
    pub(super) fn take_in_range_budgeted(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
    ) -> Vec<(block::Height, WorkItem)> {
        if max_count == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut taken: Vec<(block::Height, WorkItem)> = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut next_expected: Option<block::Height> = None;
        for (height, item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }

            let next_estimated_bytes = estimated_bytes.saturating_add(item.estimated_bytes);
            if !taken.is_empty() && next_estimated_bytes > max_estimated_bytes {
                break;
            }

            taken.push((*height, *item));
            estimated_bytes = next_estimated_bytes;
            if taken.len() >= max_count {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        for (height, item) in &taken {
            inner.pending.remove(height);
            inner.in_flight.insert(*height, *item);
        }
        taken
    }

    /// Move each given height `in_flight → pending`, preserving its stored
    /// [`WorkItem`]. Heights not currently `in_flight` are skipped (idempotent).
    /// Wakes waiters if anything moved.
    #[cfg(test)]
    pub(super) fn return_items(&self, heights: impl IntoIterator<Item = block::Height>) {
        let mut moved = false;
        {
            let mut inner = self.lock();
            for height in heights {
                if let Some(item) = inner.in_flight.remove(&height) {
                    inner.pending.insert(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
    }

    /// Like [`return_items`](Self::return_items) but **does not** notify waiters.
    ///
    /// Used by a peer routine to put back a chunk it took but chose not to issue
    /// (e.g. the heights are in its own short retry-avoid window after it just
    /// failed them). Notifying here would re-wake the returning routine's own
    /// freshly-registered `available` future and busy-loop the want-work arm
    /// (a self-wake spin); other peers were already woken by the original failure
    /// `return_items`, so suppressing the notify only affects the caller.
    pub(super) fn return_items_quiet(&self, heights: impl IntoIterator<Item = block::Height>) {
        let mut inner = self.lock();
        for height in heights {
            if let Some(item) = inner.in_flight.remove(&height) {
                inner.pending.insert(height, item);
            }
        }
    }

    /// Mark already-taken heights as owning an estimated byte reservation.
    ///
    /// Returns the sum marked. The caller must have already admitted the same
    /// byte total through [`ByteBudget`](crate::zakura::transport::guard::ByteBudget).
    pub(super) fn mark_reserved(&self, heights: impl IntoIterator<Item = block::Height>) -> u64 {
        let mut marked = 0u64;
        let mut inner = self.lock();
        for height in heights {
            let Some(item) = inner.in_flight.get_mut(&height) else {
                continue;
            };
            if item.budget.current_charge() != 0 {
                continue;
            }
            item.budget = BlockBudgetLedger::reserved(item.estimated_bytes);
            marked = marked.saturating_add(item.estimated_bytes);
        }
        marked
    }

    /// Settle a body only if this height still owns an active request reservation.
    ///
    /// Returns `None` when a central watchdog or local timeout already released
    /// and returned the height. Late bodies from that superseded claim must not
    /// resurrect a second charge.
    #[cfg(test)]
    pub(super) fn settle_active_reserved_height(
        &self,
        height: block::Height,
        actual: u64,
    ) -> Option<i128> {
        let mut inner = self.lock();
        let item = inner.in_flight.get_mut(&height)?;
        item.budget
            .is_reserved()
            .then(|| item.budget.settle(actual))
    }

    /// Settle an active request and count the body as queued for the Sequencer.
    ///
    /// The counter update happens while holding the work queue lock, so audit
    /// snapshots see either `Reserved(estimate)` or queued body bytes, not both.
    pub(super) fn settle_active_reserved_height_and_count(
        &self,
        height: block::Height,
        actual: u64,
        sequencer_input_bytes: &std::sync::atomic::AtomicU64,
    ) -> Option<i128> {
        let mut inner = self.lock();
        let item = inner.in_flight.get_mut(&height)?;
        item.budget.is_reserved().then(|| {
            let delta = item.budget.settle(actual);
            sequencer_input_bytes.fetch_add(actual, std::sync::atomic::Ordering::Relaxed);
            delta
        })
    }

    /// Mark a height as directly held after the caller admitted `actual` bytes.
    ///
    /// Used for unmatched queued bodies, which did not have a prior request
    /// estimate reservation.
    pub(super) fn mark_held_direct(&self, height: block::Height, actual: u64) -> u64 {
        let mut inner = self.lock();
        if let Some(item) = inner.in_flight.get_mut(&height) {
            let previous_charge = item.budget.release();
            item.budget = BlockBudgetLedger::Held(actual);
            return previous_charge;
        }
        if let Some(mut item) = inner.pending.remove(&height) {
            let previous_charge = item.budget.release();
            item.budget = BlockBudgetLedger::Held(actual);
            inner.in_flight.insert(height, item);
            return previous_charge;
        }
        0
    }

    /// Release any live charges for `heights`, exactly once.
    pub(super) fn release_heights(&self, heights: impl IntoIterator<Item = block::Height>) -> u64 {
        let mut released = 0u64;
        let mut inner = self.lock();
        for height in heights {
            if let Some(item) = inner.in_flight.get_mut(&height) {
                released = released.saturating_add(item.budget.release());
            } else if let Some(item) = inner.pending.get_mut(&height) {
                released = released.saturating_add(item.budget.release());
            }
        }
        released
    }

    /// Release and return `in_flight` heights to `pending`.
    pub(super) fn release_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for height in heights {
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    released = released.saturating_add(item.budget.release());
                    inner.pending.insert(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    /// Release and return only still-reserved `in_flight` heights to `pending`.
    ///
    /// A height that has already settled to `Held(actual)` is owned by the body
    /// handoff / Sequencer path. A central watchdog may clear stale peer claims,
    /// but it must not release or requeue those bytes.
    pub(super) fn release_reserved_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for height in heights {
                let Some(item) = inner.in_flight.get(&height) else {
                    continue;
                };
                if !item.budget.is_reserved() {
                    continue;
                }
                let mut item = inner
                    .in_flight
                    .remove(&height)
                    .expect("reserved item exists because it was just checked");
                released = released.saturating_add(item.budget.release());
                inner.pending.insert(height, item);
                moved = true;
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    /// Garbage-collect committed heights: raise the floor to `max(self.floor,
    /// floor)` and drop every `pending`/`in_flight` entry `<= floor`.
    ///
    /// Returns request-estimate bytes that were still reserved for unreceived
    /// heights. Held body bytes are cleared from the ledger here but are not
    /// returned: the Sequencer releases those actual body bytes when it drops
    /// reorder/applying state.
    pub(super) fn advance_floor(&self, floor: block::Height) -> u64 {
        let mut inner = self.lock();
        inner.floor = inner.floor.max(floor);
        let floor = inner.floor;
        let mut released = 0u64;
        for item in inner.pending.range_mut(..=floor).map(|(_, item)| item) {
            released = released.saturating_add(item.budget.release_reserved());
        }
        for item in inner.in_flight.range_mut(..=floor).map(|(_, item)| item) {
            released = released.saturating_add(item.budget.release_reserved());
        }
        inner.pending.retain(|height, _| *height > floor);
        inner.in_flight.retain(|height, _| *height > floor);
        released
    }

    /// Frontier reset: pin the floor and drop every `pending`/`in_flight` entry
    /// `> floor` (their buffers were dropped; the producer re-fills via the next
    /// query).
    ///
    /// Returns request-estimate bytes still reserved for unreceived heights, as
    /// in [`advance_floor`](Self::advance_floor).
    pub(super) fn reset_above(&self, floor: block::Height) -> u64 {
        let mut inner = self.lock();
        inner.floor = floor;
        let mut released = 0u64;
        for item in inner
            .pending
            .range_mut((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
            .map(|(_, item)| item)
        {
            released = released.saturating_add(item.budget.release_reserved());
        }
        for item in inner
            .in_flight
            .range_mut((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
            .map(|(_, item)| item)
        {
            released = released.saturating_add(item.budget.release_reserved());
        }
        inner.pending.retain(|height, _| *height <= floor);
        inner.in_flight.retain(|height, _| *height <= floor);
        released
    }

    /// The "work added" notifier (per-peer routines wake source).
    #[allow(dead_code)]
    pub(super) fn subscribe_available(&self) -> &Notify {
        &self.available
    }

    // ---- diagnostics (trace + late-response classification) ----

    pub(super) fn pending_len(&self) -> usize {
        self.lock().pending.len()
    }

    pub(super) fn in_flight_len(&self) -> usize {
        self.lock().in_flight.len()
    }

    pub(super) fn reserved_above(&self, floor: block::Height) -> (u64, u64) {
        let inner = self.lock();
        inner
            .in_flight
            .range((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
            .fold((0u64, 0u64), |(bytes, count), (_, item)| {
                let charge = item.budget.reserved_charge();
                if charge == 0 {
                    (bytes, count)
                } else {
                    (bytes.saturating_add(charge), count.saturating_add(1))
                }
            })
    }

    pub(super) fn reserved_bytes(&self) -> u64 {
        let inner = self.lock();
        inner
            .pending
            .values()
            .chain(inner.in_flight.values())
            .map(|item| item.budget.reserved_charge())
            .fold(0u64, u64::saturating_add)
    }

    /// Number of contiguous runs across `pending` (the old `queue_len` meaning:
    /// one queued range per maximal contiguous run of heights).
    pub(super) fn pending_run_count(&self) -> usize {
        let inner = self.lock();
        let mut runs = 0usize;
        let mut previous: Option<block::Height> = None;
        for height in inner.pending.keys() {
            let contiguous =
                previous.and_then(|previous| previous.0.checked_add(1)) == Some(height.0);
            if !contiguous {
                runs += 1;
            }
            previous = Some(*height);
        }
        runs
    }

    pub(super) fn min_pending(&self) -> Option<block::Height> {
        self.lock().pending.keys().next().copied()
    }

    pub(super) fn min_in_flight(&self) -> Option<block::Height> {
        self.lock().in_flight.keys().next().copied()
    }

    pub(super) fn first_pending_in_range(
        &self,
        low: block::Height,
        high: block::Height,
    ) -> Option<block::Height> {
        if low > high {
            return None;
        }
        self.lock()
            .pending
            .range(low..=high)
            .next()
            .map(|(height, _)| *height)
    }

    pub(super) fn max_in_flight(&self) -> Option<block::Height> {
        self.lock().in_flight.keys().next_back().copied()
    }

    pub(super) fn max_claimed(&self) -> Option<block::Height> {
        let inner = self.lock();
        inner
            .pending
            .keys()
            .next_back()
            .copied()
            .max(inner.in_flight.keys().next_back().copied())
    }

    /// Expected hash for a height in `pending` or `in_flight` (late-response
    /// recovery; replaces the old `queued_hash_for_height`).
    pub(super) fn hash_for_height(&self, height: block::Height) -> Option<block::Hash> {
        let inner = self.lock();
        inner
            .pending
            .get(&height)
            .or_else(|| inner.in_flight.get(&height))
            .map(|item| item.hash)
    }

    pub(super) fn pending_contains(&self, height: block::Height) -> bool {
        self.lock().pending.contains_key(&height)
    }

    pub(super) fn in_flight_contains(&self, height: block::Height) -> bool {
        self.lock().in_flight.contains_key(&height)
    }
}
