//! Download work source for Zakura block sync.
//!
//! The [`WorkQueue`] is the sole shared download-scheduling primitive: a sorted
//! set of needed block heights the per-peer issuance path pulls from. It replaces
//! the central `BlockRangeScheduler`'s eligibility/dedup/retry roles with a small
//! API the caller drives from its own per-peer state (see the ):
//!
//! - a height is in **exactly one** of `{below-floor (gone), pending, in_flight}`;
//! - [`take_in_range`](WorkQueue::take_in_range) moves a contiguous-ascending run
//!   into `in_flight` and records one active claim for each height (so one taken
//!   chunk maps to one `BlockRangeRequest`), bounded only by the caller's servable
//!   range and a count cap — never by how far above the committed floor the
//!   heights already are;
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

use std::{collections::BTreeSet, sync::Mutex as StdMutex};

use tokio::sync::Notify;
use zebra_chain::block;

use super::{
    request::{BlockSizeEstimate, WorkClaimId},
    state::BlockBudgetLedger,
};

/// Lower clamp on a body-size estimate.
pub(super) const DEFAULT_BS_SIZE_FLOOR_BYTES: u64 = 1024;

/// Per-height download metadata returned to peer routines when they claim work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ClaimedWorkItem {
    /// Expected hash of the block at this height (drives the response match).
    pub(super) hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the
    /// receive-path `SizeMismatch` tolerance check.
    pub(super) estimated_bytes: u64,
    /// The specific claim owned by the peer routine that took this work.
    pub(super) claim_id: WorkClaimId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkClaim {
    id: WorkClaimId,
    budget: BlockBudgetLedger,
}

/// Per-height download metadata held in the [`WorkQueue`].
#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkItem {
    /// Expected hash of the block at this height (drives the response match).
    hash: block::Hash,
    /// The block's size estimate. Used for request budget reservation and the
    /// receive-path `SizeMismatch` tolerance check.
    estimated_bytes: u64,
    /// Current byte-budget charges owned by active claims for this height.
    ///
    /// Pending items normally have no claims; issued-but-unreceived claims have
    /// `Reserved(estimate)`; received bodies held by the commit pipeline have
    /// `Held(actual)`. All terminal release paths go through these ledgers so a
    /// stale local owner cannot release a charge that another path already
    /// returned.
    claims: Vec<WorkClaim>,
}

impl WorkItem {
    fn new(hash: block::Hash, estimated_bytes: u64) -> Self {
        Self {
            hash,
            estimated_bytes,
            claims: Vec::new(),
        }
    }

    fn claim_count(&self) -> usize {
        self.claims.len()
    }

    fn add_claim(&mut self, id: WorkClaimId) -> ClaimedWorkItem {
        self.claims.push(WorkClaim {
            id,
            budget: BlockBudgetLedger::Released,
        });
        ClaimedWorkItem {
            hash: self.hash,
            estimated_bytes: self.estimated_bytes,
            claim_id: id,
        }
    }

    fn reserved_charge(&self) -> u64 {
        self.claims
            .iter()
            .map(|claim| claim.budget.reserved_charge())
            .fold(0u64, u64::saturating_add)
    }

    fn release_one_claim(&mut self) -> u64 {
        if let Some(index) = self
            .claims
            .iter()
            .position(|claim| claim.budget.is_reserved())
        {
            let mut claim = self.claims.remove(index).budget;
            return claim.release();
        }

        self.claims
            .pop()
            .map(|claim| {
                let mut budget = claim.budget;
                budget.release()
            })
            .unwrap_or(0)
    }

    fn release_one_held_claim(&mut self) -> u64 {
        let Some(index) = self.claims.iter().position(|claim| claim.budget.is_held()) else {
            return 0;
        };
        let mut claim = self.claims.remove(index).budget;
        claim.release()
    }

    #[cfg(test)]
    fn release_one_reserved_claim(&mut self) -> u64 {
        let Some(index) = self
            .claims
            .iter()
            .position(|claim| claim.budget.is_reserved())
        else {
            return 0;
        };
        let mut claim = self.claims.remove(index).budget;
        claim.release()
    }

    fn release_claim(&mut self, id: WorkClaimId) -> u64 {
        let Some(index) = self.claims.iter().position(|claim| claim.id == id) else {
            return 0;
        };
        let mut claim = self.claims.remove(index).budget;
        claim.release()
    }

    fn release_reserved_claim(&mut self, id: WorkClaimId) -> u64 {
        let Some(index) = self
            .claims
            .iter()
            .position(|claim| claim.id == id && claim.budget.is_reserved())
        else {
            return 0;
        };
        let mut claim = self.claims.remove(index).budget;
        claim.release()
    }

    fn release_held_claim(&mut self, id: WorkClaimId) -> u64 {
        let Some(index) = self
            .claims
            .iter()
            .position(|claim| claim.id == id && claim.budget.is_held())
        else {
            return 0;
        };
        let mut claim = self.claims.remove(index).budget;
        claim.release()
    }

    fn release_all_reserved(&mut self) -> u64 {
        self.claims
            .iter_mut()
            .map(|claim| claim.budget.release_reserved())
            .fold(0u64, u64::saturating_add)
    }

    fn mark_reserved(&mut self, id: WorkClaimId) -> u64 {
        let Some(claim) = self.claims.iter_mut().find(|claim| claim.id == id) else {
            return 0;
        };
        if claim.budget.current_charge() != 0 {
            return 0;
        }
        claim.budget = BlockBudgetLedger::reserved(self.estimated_bytes);
        self.estimated_bytes
    }

    fn settle_reserved(&mut self, id: WorkClaimId, actual: u64) -> Option<i128> {
        self.claims
            .iter_mut()
            .find(|claim| claim.id == id && claim.budget.is_reserved())
            .map(|claim| claim.budget.settle(actual))
    }

    fn mark_first_held(&mut self, actual: u64, next_claim_id: &mut u64) -> (u64, WorkClaimId) {
        if let Some(claim) = self.claims.iter_mut().next() {
            let previous_charge = claim.budget.release();
            claim.budget = BlockBudgetLedger::Held(actual);
            (previous_charge, claim.id)
        } else {
            let id = WorkClaimId(*next_claim_id);
            *next_claim_id = next_claim_id.saturating_add(1);
            self.claims.push(WorkClaim {
                id,
                budget: BlockBudgetLedger::Held(actual),
            });
            (0, id)
        }
    }

    #[cfg(test)]
    fn claim_id_for_first_unreserved(&self) -> Option<WorkClaimId> {
        self.claims
            .iter()
            .find(|claim| claim.budget.current_charge() == 0)
            .map(|claim| claim.id)
    }

    fn has_claim(&self, id: WorkClaimId) -> bool {
        self.claims.iter().any(|claim| claim.id == id)
    }

    fn has_held_claim(&self) -> bool {
        self.claims.iter().any(|claim| claim.budget.is_held())
    }
}

#[derive(Debug)]
struct WorkQueueInner {
    pending: std::collections::BTreeMap<block::Height, WorkItem>,
    in_flight: std::collections::BTreeMap<block::Height, WorkItem>,
    floor: block::Height,
    next_claim_id: u64,
    /// Floor clamp for size estimates (overridable for tests).
    floor_estimate_bytes: u64,
}

impl WorkQueueInner {
    fn estimate_bytes(&self, estimate: BlockSizeEstimate) -> u64 {
        estimate_bytes_with(estimate, self.floor_estimate_bytes)
    }

    fn retain_returned_claim(&mut self, height: block::Height, item: WorkItem) {
        if height <= self.floor {
            return;
        }

        if item.claim_count() == 0 {
            self.pending.insert(height, item);
        } else {
            self.in_flight.insert(height, item);
        }
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
                next_claim_id: 1,
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
                inner
                    .pending
                    .insert(height, WorkItem::new(hash, estimated_bytes));
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
    ) -> Vec<(block::Height, ClaimedWorkItem)> {
        if max == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut selected: Vec<block::Height> = Vec::new();
        let mut next_expected: Option<block::Height> = None;
        for (height, _item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }
            selected.push(*height);
            if selected.len() >= max {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        let mut taken = Vec::with_capacity(selected.len());
        for height in selected {
            let mut item = inner
                .pending
                .remove(&height)
                .expect("taken item exists because it was just iterated");
            let id = WorkClaimId(inner.next_claim_id);
            inner.next_claim_id = inner.next_claim_id.saturating_add(1);
            let claimed = item.add_claim(id);
            inner.in_flight.insert(height, item);
            taken.push((height, claimed));
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
    #[cfg(test)]
    pub(super) fn take_in_range_budgeted(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
    ) -> Vec<(block::Height, ClaimedWorkItem)> {
        if max_count == 0 || low > high {
            return Vec::new();
        }
        let mut inner = self.lock();
        let mut selected: Vec<block::Height> = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut next_expected: Option<block::Height> = None;
        for (height, item) in inner.pending.range(low..=high) {
            if let Some(expected) = next_expected {
                if *height != expected {
                    break;
                }
            }

            let next_estimated_bytes = estimated_bytes.saturating_add(item.estimated_bytes);
            if !selected.is_empty() && next_estimated_bytes > max_estimated_bytes {
                break;
            }

            selected.push(*height);
            estimated_bytes = next_estimated_bytes;
            if selected.len() >= max_count {
                break;
            }
            // Stop the run at the end of the height space rather than overflowing.
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }
        let mut taken = Vec::with_capacity(selected.len());
        for height in selected {
            let mut item = inner
                .pending
                .remove(&height)
                .expect("taken item exists because it was just iterated");
            let id = WorkClaimId(inner.next_claim_id);
            inner.next_claim_id = inner.next_claim_id.saturating_add(1);
            let claimed = item.add_claim(id);
            inner.in_flight.insert(height, item);
            taken.push((height, claimed));
        }
        taken
    }

    /// Move up to `max_count` claimable contiguous heights within `low..=high`
    /// into `in_flight`, stopping before the summed size estimates would exceed
    /// `max_estimated_bytes`.
    ///
    /// Unlike [`take_in_range_budgeted`](Self::take_in_range_budgeted), this can
    /// add a new claim to an already in-flight height when its active claims are
    /// below the caller-provided fanout policy.
    pub(super) fn take_in_range_budgeted_with_fanout(
        &self,
        low: block::Height,
        high: block::Height,
        max_count: usize,
        max_estimated_bytes: u64,
        desired_fanout: &dyn Fn(block::Height) -> usize,
        excluded: &BTreeSet<block::Height>,
    ) -> Vec<(block::Height, ClaimedWorkItem)> {
        if max_count == 0 || low > high {
            return Vec::new();
        }

        let mut inner = self.lock();
        let mut heights: Vec<_> = inner
            .pending
            .range(low..=high)
            .map(|(height, _)| *height)
            .chain(inner.in_flight.range(low..=high).map(|(height, _)| *height))
            .collect();
        heights.sort_unstable();
        heights.dedup();

        let mut selected: Vec<block::Height> = Vec::new();
        let mut estimated_bytes = 0u64;
        let mut next_expected: Option<block::Height> = None;
        for height in heights {
            if excluded.contains(&height) {
                continue;
            }
            if let Some(expected) = next_expected {
                if height != expected {
                    break;
                }
            }

            let Some(item) = inner
                .pending
                .get(&height)
                .or_else(|| inner.in_flight.get(&height))
            else {
                continue;
            };
            if item.has_held_claim() || item.claim_count() >= desired_fanout(height).max(1) {
                continue;
            }

            let next_estimated_bytes = estimated_bytes.saturating_add(item.estimated_bytes);
            if !selected.is_empty() && next_estimated_bytes > max_estimated_bytes {
                break;
            }

            selected.push(height);
            estimated_bytes = next_estimated_bytes;
            if selected.len() >= max_count {
                break;
            }
            match height.0.checked_add(1) {
                Some(raw) => next_expected = Some(block::Height(raw)),
                None => break,
            }
        }

        let mut taken = Vec::with_capacity(selected.len());
        for height in selected {
            let mut item = inner
                .pending
                .remove(&height)
                .or_else(|| inner.in_flight.remove(&height))
                .expect("taken item exists because it was just selected");
            let id = WorkClaimId(inner.next_claim_id);
            inner.next_claim_id = inner.next_claim_id.saturating_add(1);
            let claimed = item.add_claim(id);
            inner.in_flight.insert(height, item);
            taken.push((height, claimed));
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
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    let _ = item.release_one_claim();
                    inner.retain_returned_claim(height, item);
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
            if let Some(mut item) = inner.in_flight.remove(&height) {
                let _ = item.release_one_claim();
                inner.retain_returned_claim(height, item);
            }
        }
    }

    /// Mark already-taken heights as owning an estimated byte reservation.
    ///
    /// Returns the sum marked. The caller must have already admitted the same
    /// byte total through [`ByteBudget`](crate::zakura::transport::guard::ByteBudget).
    #[cfg(test)]
    pub(super) fn mark_reserved(&self, heights: impl IntoIterator<Item = block::Height>) -> u64 {
        let mut marked = 0u64;
        let mut inner = self.lock();
        for height in heights {
            let Some(item) = inner.in_flight.get_mut(&height) else {
                continue;
            };
            let Some(id) = item.claim_id_for_first_unreserved() else {
                continue;
            };
            marked = marked.saturating_add(item.mark_reserved(id));
        }
        marked
    }

    pub(super) fn mark_reserved_claims(
        &self,
        claims: impl IntoIterator<Item = (block::Height, WorkClaimId)>,
    ) -> u64 {
        let mut marked = 0u64;
        let mut inner = self.lock();
        for (height, claim_id) in claims {
            let Some(item) = inner.in_flight.get_mut(&height) else {
                continue;
            };
            marked = marked.saturating_add(item.mark_reserved(claim_id));
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
        let claim_id = item
            .claims
            .iter()
            .find(|claim| claim.budget.is_reserved())
            .map(|claim| claim.id)?;
        item.settle_reserved(claim_id, actual)
    }

    pub(super) fn settle_active_reserved_claim(
        &self,
        height: block::Height,
        claim_id: WorkClaimId,
        actual: u64,
    ) -> Option<i128> {
        let mut inner = self.lock();
        let item = inner.in_flight.get_mut(&height)?;
        item.settle_reserved(claim_id, actual)
    }

    /// Mark a height as directly held after the caller admitted `actual` bytes.
    ///
    /// Used for unmatched queued bodies, which did not have a prior request
    /// estimate reservation.
    pub(super) fn mark_held_direct(
        &self,
        height: block::Height,
        actual: u64,
    ) -> (u64, WorkClaimId) {
        let mut inner = self.lock();
        if let Some(mut item) = inner.in_flight.remove(&height) {
            let mut next_claim_id = inner.next_claim_id;
            let result = item.mark_first_held(actual, &mut next_claim_id);
            inner.next_claim_id = next_claim_id;
            inner.in_flight.insert(height, item);
            return result;
        }
        if let Some(mut item) = inner.pending.remove(&height) {
            let mut next_claim_id = inner.next_claim_id;
            let (previous_charge, claim_id) = item.mark_first_held(actual, &mut next_claim_id);
            inner.next_claim_id = next_claim_id;
            inner.in_flight.insert(height, item);
            return (previous_charge, claim_id);
        }
        let id = WorkClaimId(inner.next_claim_id);
        inner.next_claim_id = inner.next_claim_id.saturating_add(1);
        (0, id)
    }

    /// Release any live charges for `heights`, exactly once.
    #[cfg(test)]
    pub(super) fn release_heights(&self, heights: impl IntoIterator<Item = block::Height>) -> u64 {
        let mut released = 0u64;
        let mut inner = self.lock();
        for height in heights {
            if let Some(item) = inner.in_flight.get_mut(&height) {
                released = released.saturating_add(item.release_one_claim());
            } else if let Some(item) = inner.pending.get_mut(&height) {
                released = released.saturating_add(item.release_one_claim());
            }
        }
        released
    }

    pub(super) fn release_claims(
        &self,
        claims: impl IntoIterator<Item = (block::Height, WorkClaimId)>,
    ) -> u64 {
        let mut released = 0u64;
        let mut inner = self.lock();
        for (height, claim_id) in claims {
            if let Some(item) = inner.in_flight.get_mut(&height) {
                released = released.saturating_add(item.release_claim(claim_id));
            } else if let Some(item) = inner.pending.get_mut(&height) {
                released = released.saturating_add(item.release_claim(claim_id));
            }
        }
        released
    }

    /// Release and return `in_flight` heights to `pending`.
    #[cfg(test)]
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
                    released = released.saturating_add(item.release_one_claim());
                    inner.retain_returned_claim(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    pub(super) fn release_and_return_claims(
        &self,
        claims: impl IntoIterator<Item = (block::Height, WorkClaimId)>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for (height, claim_id) in claims {
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    released = released.saturating_add(item.release_claim(claim_id));
                    inner.retain_returned_claim(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    /// Release one held body claim and return the height to pending only when no
    /// other claims remain.
    pub(super) fn release_held_and_return_items(
        &self,
        heights: impl IntoIterator<Item = block::Height>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for height in heights {
                if let Some(mut item) = inner.in_flight.remove(&height) {
                    let held_released = item.release_one_held_claim();
                    released = released.saturating_add(if held_released == 0 {
                        item.release_one_claim()
                    } else {
                        held_released
                    });
                    inner.retain_returned_claim(height, item);
                    moved = true;
                }
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    pub(super) fn release_held_claim(&self, height: block::Height, claim_id: WorkClaimId) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            if let Some(mut item) = inner.in_flight.remove(&height) {
                let held_released = item.release_held_claim(claim_id);
                released = released.saturating_add(if held_released == 0 {
                    item.release_claim(claim_id)
                } else {
                    held_released
                });
                inner.retain_returned_claim(height, item);
                moved = true;
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
    #[cfg(test)]
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
                if item.claims.iter().all(|claim| !claim.budget.is_reserved()) {
                    continue;
                }
                let mut item = inner
                    .in_flight
                    .remove(&height)
                    .expect("reserved item exists because it was just checked");
                released = released.saturating_add(item.release_one_reserved_claim());
                inner.retain_returned_claim(height, item);
                moved = true;
            }
        }
        if moved {
            self.available.notify_waiters();
        }
        released
    }

    pub(super) fn release_reserved_and_return_claims(
        &self,
        claims: impl IntoIterator<Item = (block::Height, WorkClaimId)>,
    ) -> u64 {
        let mut moved = false;
        let mut released = 0u64;
        {
            let mut inner = self.lock();
            for (height, claim_id) in claims {
                let Some(item) = inner.in_flight.get(&height) else {
                    continue;
                };
                if !item.has_claim(claim_id) {
                    continue;
                }
                let mut item = inner
                    .in_flight
                    .remove(&height)
                    .expect("claim exists because it was just checked");
                released = released.saturating_add(item.release_reserved_claim(claim_id));
                inner.retain_returned_claim(height, item);
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
            released = released.saturating_add(item.release_all_reserved());
        }
        for item in inner.in_flight.range_mut(..=floor).map(|(_, item)| item) {
            released = released.saturating_add(item.release_all_reserved());
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
            released = released.saturating_add(item.release_all_reserved());
        }
        for item in inner
            .in_flight
            .range_mut((std::ops::Bound::Excluded(floor), std::ops::Bound::Unbounded))
            .map(|(_, item)| item)
        {
            released = released.saturating_add(item.release_all_reserved());
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
                let charge = item.reserved_charge();
                if charge == 0 {
                    (bytes, count)
                } else {
                    (
                        bytes.saturating_add(charge),
                        count.saturating_add(item.claim_count() as u64),
                    )
                }
            })
    }

    pub(super) fn reserved_bytes(&self) -> u64 {
        let inner = self.lock();
        inner
            .pending
            .values()
            .chain(inner.in_flight.values())
            .map(|item| item.reserved_charge())
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

    pub(super) fn first_claimable_in_range(
        &self,
        low: block::Height,
        high: block::Height,
        desired_fanout: &dyn Fn(block::Height) -> usize,
        excluded: &BTreeSet<block::Height>,
    ) -> Option<block::Height> {
        if low > high {
            return None;
        }

        let inner = self.lock();
        let mut heights: Vec<_> = inner
            .pending
            .range(low..=high)
            .map(|(height, _)| *height)
            .chain(inner.in_flight.range(low..=high).map(|(height, _)| *height))
            .collect();
        heights.sort_unstable();
        heights.dedup();
        heights.into_iter().find(|height| {
            !excluded.contains(height)
                && inner
                    .pending
                    .get(height)
                    .or_else(|| inner.in_flight.get(height))
                    .is_some_and(|item| {
                        !item.has_held_claim()
                            && item.claim_count() < desired_fanout(*height).max(1)
                    })
        })
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

    pub(super) fn claim_count(&self, height: block::Height) -> usize {
        self.lock()
            .in_flight
            .get(&height)
            .map(WorkItem::claim_count)
            .unwrap_or(0)
    }
}
