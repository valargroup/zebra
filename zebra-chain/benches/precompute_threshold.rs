//! Benchmarks to find where the precompute's rayon parallelism stops paying off.
//!
//! For a range of per-block note counts, this compares:
//! - `serial`: appending the notes one at a time to a fresh tree (no rayon), the
//!   cost the committer pays inline today; and
//! - `parallel`: `NoteCommitmentTree::precompute_append` (rayon `into_par_iter` +
//!   `rayon::join`), the off-committer precompute.
//!
//! The crossover — the smallest count where `parallel` beats `serial` — is the
//! point below which gating off rayon (hashing serially) avoids paying overhead
//! that does not buy anything. Orchard's Sinsemilla `combine` dominates, so it is
//! the meaningful pool to measure; Sapling is shown as a control.

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use halo2::pasta::pallas;

use zebra_chain::orchard::tree::NoteCommitmentTree as OrchardTree;
use zebra_chain::sapling::tree::NoteCommitmentTree as SaplingTree;

/// Note counts spanning the small-batch region where rayon overhead is expected
/// to dominate, up to sizes where parallelism clearly wins.
const NOTE_COUNTS: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

fn orchard_notes(count: usize) -> Vec<pallas::Base> {
    // Small integers are canonical Pallas field elements.
    (0..count as u64).map(pallas::Base::from).collect()
}

fn sapling_notes(count: usize) -> Vec<sapling_crypto::note::ExtractedNoteCommitment> {
    (0..count as u64)
        .map(|value| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&value.to_le_bytes());
            Option::from(sapling_crypto::note::ExtractedNoteCommitment::from_bytes(
                &bytes,
            ))
            .expect("small little-endian integer is a canonical Jubjub base")
        })
        .collect()
}

fn bench_orchard(c: &mut Criterion) {
    let mut group = c.benchmark_group("orchard_precompute_threshold");

    for &count in NOTE_COUNTS {
        let notes = orchard_notes(count);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("serial", count), &notes, |b, notes| {
            b.iter(|| {
                let mut tree = OrchardTree::default();
                for note in notes {
                    tree.append(*black_box(note)).expect("tree is not full");
                }
                black_box(tree.root());
            })
        });

        group.bench_with_input(BenchmarkId::new("parallel", count), &notes, |b, notes| {
            b.iter(|| black_box(OrchardTree::precompute_then_graft_root(black_box(notes))))
        });
    }

    group.finish();
}

fn bench_sapling(c: &mut Criterion) {
    let mut group = c.benchmark_group("sapling_precompute_threshold");

    for &count in NOTE_COUNTS {
        let notes = sapling_notes(count);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("serial", count), &notes, |b, notes| {
            b.iter(|| {
                let mut tree = SaplingTree::default();
                for note in notes {
                    tree.append(*black_box(note)).expect("tree is not full");
                }
                black_box(tree.root());
            })
        });

        group.bench_with_input(BenchmarkId::new("parallel", count), &notes, |b, notes| {
            b.iter(|| black_box(SaplingTree::precompute_then_graft_root(black_box(notes))))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_orchard, bench_sapling);
criterion_main!(benches);
