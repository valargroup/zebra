//! The serial commit pipeline for Zakura block sync.
//!
//! The [`Sequencer`] owns the consensus-critical reorder → applying machinery and
//! nothing else. "Applying" now means *drained onto the applyQ and held until
//! durable*: the contiguous reorder prefix is drained out as [`DrainedBlock`]s (the
//! block `Arc` leaves for the applyQ + the node-side `Committer`), and the
//! Sequencer keeps only a bytes-and-metadata ledger entry per drained height for
//! release/reset accounting. The block bytes stay reserved against the byte budget
//! from drain until the durable frontier crosses the height.
//!
//! The Sequencer deliberately never touches download-side state — the byte budget,
//! the work scheduler, peers, emitted actions, or state queries. Two rules keep
//! that boundary clean:
//!
//! - every method that frees reserved bytes *returns* the freed count, so the
//!   reactor/Sequencer task releases it against the shared budget, and
//! - every download-side consequence (mark a height covered, clear covered,
//!   re-query, attribute misbehavior) is expressed as a value the task acts on,
//!   not performed here.

use super::{reorder::*, state::*, *};

/// One drained, hash-verified block leaving the reorder buffer for the applyQ.
///
/// The block `Arc` is carried out so the Sequencer task can build the
/// `ApplyItem`; the Sequencer retains only an [`ApplyingEntry`] for accounting.
#[derive(Clone, Debug)]
pub(super) struct DrainedBlock {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) block: Arc<block::Block>,
    pub(super) bytes: u64,
    pub(super) source_peer: ZakuraPeerId,
    /// Apply generation stamped on this block. Echoed onto the `ApplyItem` and
    /// recorded in the held ledger so a stale [`super::CommitterReset`] (older
    /// epoch) for a re-pushed height is ignored.
    pub(super) epoch: u64,
}

/// The bytes-and-metadata ledger entry the Sequencer keeps for a height that has
/// been drained onto the applyQ and is held until the durable frontier crosses it.
///
/// The block `Arc` is *not* stored here — it lives on the applyQ / in the
/// `Committer` / in the state write queue. Only the accounting metadata the reset
/// and frontier-advance paths need survives.
#[derive(Copy, Clone, Debug)]
struct ApplyingEntry {
    bytes: u64,
    hash: block::Hash,
    prev_hash: block::Hash,
    /// Apply generation this height was drained at (see [`DrainedBlock::epoch`]).
    epoch: u64,
}

/// Outcome of offering a received body to the commit pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AcceptOutcome {
    /// The body was buffered and now owns its byte reservation. The reactor must
    /// mark `covered` covered in the download scheduler so the retry path stops
    /// re-requesting it.
    Buffered { covered: block::Height },
    /// The body was not buffered (already at/below the floor, held elsewhere in
    /// the commit pipeline, or a duplicate). The reactor must release the
    /// `release_bytes` the body still reserved.
    Redundant { release_bytes: u64 },
}

/// Sequencer half of a verified-tip advance (frontier growth/commit).
#[derive(Copy, Clone, Debug)]
pub(super) struct AdvanceOutcome {
    /// Bytes freed from the reorder/applying buffers that the reactor releases.
    pub(super) release_bytes: u64,
    /// Whether the verified tip actually moved. The reactor drops download state
    /// (scheduler/outstanding) and re-drains only when it did.
    pub(super) changed: bool,
    /// Held heights this advance released (durable commits), for throughput.
    pub(super) committed_blocks: u64,
    /// Their byte total, for throughput.
    pub(super) committed_bytes: u64,
}

/// The reorder → applying (drain-to-applyQ, hold-until-durable) commit pipeline.
#[derive(Clone, Debug)]
pub(super) struct Sequencer {
    reorder: ReorderBuffer,
    /// Heights drained onto the applyQ and held until durable (the byte ledger).
    applying: BTreeMap<block::Height, ApplyingEntry>,
    body_download_floor: block::Height,
    verified_block_tip: block::Height,
}

impl Sequencer {
    pub(super) fn new(verified_block_tip: block::Height) -> Self {
        Self {
            reorder: ReorderBuffer::new(),
            applying: BTreeMap::new(),
            body_download_floor: verified_block_tip,
            verified_block_tip,
        }
    }

    // ---- reads (download side queries the commit pipeline through these) ----

    pub(super) fn floor(&self) -> block::Height {
        self.body_download_floor
    }

    pub(super) fn verified_tip(&self) -> block::Height {
        self.verified_block_tip
    }

    #[cfg(test)]
    pub(super) fn reorder_contains(&self, height: block::Height) -> bool {
        self.reorder.contains(height)
    }

    #[cfg(test)]
    pub(super) fn applying_contains(&self, height: block::Height) -> bool {
        self.applying.contains_key(&height)
    }

    pub(super) fn reorder_len(&self) -> usize {
        self.reorder.len()
    }

    pub(super) fn applying_len(&self) -> usize {
        self.applying.len()
    }

    pub(super) fn lowest_applying_height(&self) -> Option<block::Height> {
        self.applying.keys().next().copied()
    }

    pub(super) fn applying_buffered_bytes(&self) -> u64 {
        self.applying
            .values()
            .map(|entry| entry.bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub(super) fn reorder_buffered_bytes(&self) -> u64 {
        self.reorder.buffered_bytes()
    }

    /// Highest buffered reorder height, for shed-for-floor-starvation.
    pub(super) fn reorder_max_height(&self) -> Option<block::Height> {
        self.reorder.max_height()
    }

    /// Whether any reorder/applying body sits at or above `height`, used by the
    /// reactor to decide whether a reset is anchored by active successor work.
    pub(super) fn has_buffered_at_or_above(&self, height: block::Height) -> bool {
        self.reorder.contains_at_or_above(height) || self.applying.range(height..).next().is_some()
    }

    /// `previous_block_hash` of a held `applying` body, for deciding whether a
    /// reset orphans an already-drained successor.
    pub(super) fn applying_previous_block_hash(
        &self,
        height: block::Height,
    ) -> Option<block::Hash> {
        self.applying.get(&height).map(|entry| entry.prev_hash)
    }

    pub(super) fn reorder_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.reorder.hash(height)
    }

    pub(super) fn applying_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.applying.get(&height).map(|entry| entry.hash)
    }

    /// The apply generation a held height was drained at, or `None` if the height
    /// is not held. The Sequencer task compares this against an incoming
    /// [`super::CommitterReset`]'s epoch so a stale reset (from a superseded
    /// generation, or for a height already committed/rolled back) is a no-op.
    pub(super) fn applying_epoch(&self, height: block::Height) -> Option<u64> {
        self.applying.get(&height).map(|entry| entry.epoch)
    }

    // ---- body acceptance ----

    /// Offer a received body to the commit pipeline. Runs the redundancy checks
    /// and (when not redundant) buffers it in the reorder buffer, which takes
    /// ownership of the body's existing `bytes` reservation.
    #[cfg(test)]
    pub(super) fn accept_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> AcceptOutcome {
        self.accept_buffered_body(
            height,
            hash,
            BufferedBlockBody::Decoded(block),
            bytes,
            source_peer,
        )
    }

    pub(super) fn accept_buffered_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        mut body: BufferedBlockBody,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> AcceptOutcome {
        if height <= self.body_download_floor
            || self.reorder.contains(height)
            || self.applying.contains_key(&height)
        {
            return AcceptOutcome::Redundant {
                release_bytes: bytes,
            };
        }

        if next_height(self.body_download_floor) != Some(height) {
            body = body.into_non_contiguous_backlog();
        }

        match self
            .reorder
            .insert_body(height, hash, body, bytes, source_peer)
        {
            ReorderInsertResult::Inserted => AcceptOutcome::Buffered { covered: height },
            ReorderInsertResult::Duplicate => AcceptOutcome::Redundant {
                release_bytes: bytes,
            },
        }
    }

    // ---- drain reorder → applying (onto the applyQ) ----

    /// Drain the contiguous reorder prefix above the floor, advancing the floor and
    /// recording a held ledger entry (stamped with `epoch`) for each height.
    /// Returns the drained blocks — the caller carries each `Arc` out onto the
    /// applyQ — so the Sequencer no longer stores block bodies.
    pub(super) fn drain_ready_into_applying(&mut self, epoch: u64) -> Vec<DrainedBlock> {
        let released = self
            .reorder
            .drain_contiguous_prefix(self.body_download_floor);
        let mut drained = Vec::with_capacity(released.len());
        for (height, block, bytes, source_peer) in released {
            let hash = block.hash();
            let prev_hash = block.header.previous_block_hash;
            self.body_download_floor = height;
            self.applying.insert(
                height,
                ApplyingEntry {
                    bytes,
                    hash,
                    prev_hash,
                    epoch,
                },
            );
            drained.push(DrainedBlock {
                height,
                hash,
                block,
                bytes,
                source_peer,
                epoch,
            });
        }
        drained
    }

    // ---- reject / reset rollback ----

    /// After a rejected/timed-out commit at `height`, roll the download floor back
    /// below it — never below the verified tip — so the height is re-requestable.
    pub(super) fn reset_floor_below(&mut self, height: block::Height) {
        self.body_download_floor = previous_height(height)
            .unwrap_or(block::Height::MIN)
            .max(self.verified_block_tip);
    }

    /// Drop buffered reorder bodies at or above `from`; returns the freed bytes.
    pub(super) fn drop_reorder_from(&mut self, from: block::Height) -> u64 {
        self.reorder.drop_from(from)
    }

    /// Remove held `applying` ledger entries at or above `from`; returns the freed
    /// bytes (the budget reservations to release on a reject rollback).
    pub(super) fn release_applying_blocks_from(&mut self, from: block::Height) -> u64 {
        let heights: Vec<_> = self
            .applying
            .range(from..)
            .map(|(height, _)| *height)
            .collect();
        let mut released = 0u64;
        for height in heights {
            if let Some(entry) = self.applying.remove(&height) {
                released = released.saturating_add(entry.bytes);
            }
        }
        released
    }

    /// Remove durable `applying` ledger entries at or below `tip`; returns freed
    /// bytes (the budget release on a durable frontier advance).
    pub(super) fn release_applied_through(&mut self, tip: block::Height) -> u64 {
        let applied: Vec<_> = self
            .applying
            .range(..=tip)
            .map(|(height, _)| *height)
            .collect();
        let mut released = 0u64;
        for height in applied {
            if let Some(entry) = self.applying.remove(&height) {
                released = released.saturating_add(entry.bytes);
            }
        }
        released
    }

    // ---- frontier advance / reset ----

    /// Advance the verified tip to `new_tip` (frontier growth/commit). Bumps the
    /// floor unconditionally, drops superseded reorder bodies (and, when
    /// `release_applied`, the now-durable held ledger entries), and moves the
    /// verified tip. Returns the freed bytes, whether the tip moved, and the held
    /// heights/bytes the advance made durable (for commit throughput).
    pub(super) fn advance_verified_tip(
        &mut self,
        new_tip: block::Height,
        release_applied: bool,
    ) -> AdvanceOutcome {
        self.body_download_floor = self.body_download_floor.max(new_tip);
        if new_tip == self.verified_block_tip {
            return AdvanceOutcome {
                release_bytes: 0,
                changed: false,
                committed_blocks: 0,
                committed_bytes: 0,
            };
        }
        let mut released = self.reorder.drop_through(new_tip);
        let (committed_blocks, committed_bytes) = if release_applied {
            // All held entries sit above the verified tip, so those at or below the
            // new tip are exactly the heights this advance makes durable.
            let committed_blocks = self.applying.range(..=new_tip).count() as u64;
            let committed_bytes = self.release_applied_through(new_tip);
            released = released.saturating_add(committed_bytes);
            (committed_blocks, committed_bytes)
        } else {
            (0, 0)
        };
        self.verified_block_tip = new_tip;
        AdvanceOutcome {
            release_bytes: released,
            changed: true,
            committed_blocks,
            committed_bytes,
        }
    }

    /// Destructively reset the commit pipeline to `new_tip` (reorg/rollback): clear
    /// the reorder buffer and all held ledger entries, and pin the floor and
    /// verified tip to `new_tip`. Returns the freed bytes.
    pub(super) fn reset_to(&mut self, new_tip: block::Height) -> u64 {
        self.verified_block_tip = new_tip;
        self.body_download_floor = new_tip;
        let mut released = self.reorder.clear();
        released = released.saturating_add(
            self.applying
                .values()
                .map(|entry| entry.bytes)
                .fold(0u64, u64::saturating_add),
        );
        self.applying.clear();
        released
    }
}
