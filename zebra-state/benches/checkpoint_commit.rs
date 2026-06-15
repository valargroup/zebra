//! Benchmarks the finalized-state checkpoint commit path with the RocksDB
//! write-ahead log (WAL) enabled vs disabled.
//!
//! Checkpoint-verified blocks are reproducible from the hard-coded checkpoint
//! hashes, so Zebra writes them without a WAL (see
//! `FinalizedState::commit_finalized_direct`). This benchmark isolates the lever
//! that change affects: the per-block commit cost.
//!
//! Note that Zebra does not enable synchronous WAL writes (RocksDB's
//! `WriteOptions::sync` defaults to `false`, and Zebra never sets it), so the
//! WAL does *not* cost a per-block `fsync`. The cost it adds is write
//! amplification: every block's bytes are written once to the WAL and again when
//! the memtable flushes to SST files. Disabling the WAL therefore saves write
//! bandwidth (and some compaction pressure) proportional to block size, rather
//! than removing a sync latency.
//!
//! # Run on real disk
//!
//! The WAL cost is extra disk writes, which are masked by RAM-backed storage.
//! Running this on `tmpfs` (e.g. `/tmp` on many Linux systems) will understate
//! the difference. By default the database is created under
//! `CARGO_TARGET_TMPDIR` (inside the cargo `target/` directory, which is
//! normally on real disk). Override with `ZEBRA_WAL_BENCH_DIR` to point at a
//! specific filesystem (e.g. an NVMe vs spinning-disk comparison).
//!
//! ```sh
//! cargo bench -p zebra-state --features proptest-impl --bench checkpoint_commit
//! ```

// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use proptest::{
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};

use zebra_chain::{block::Block, parameters::Network};
use zebra_state::{CheckpointVerifiedBlock, Config, FinalizedState, PreparedChain};

/// Generates a single deterministic chain of committable blocks, starting at
/// genesis. The same chain is reused for every benchmark variant so that the
/// only difference being measured is the WAL setting.
fn generate_chain() -> (Network, Vec<Arc<Block>>) {
    let mut runner = TestRunner::deterministic();
    let value_tree = PreparedChain::default()
        .new_tree(&mut runner)
        .expect("prepared chain strategy creates a value tree");

    let (chain, count, network, _history_tree) = value_tree.current();

    let blocks = chain
        .iter()
        .take(count)
        .map(|block| block.block.clone())
        .collect();

    (network, blocks)
}

/// Root directory for this run's databases, on real disk.
///
/// Uses `ZEBRA_WAL_BENCH_DIR` if set, otherwise `CARGO_TARGET_TMPDIR`. The
/// process id keeps concurrent runs from colliding.
fn run_root() -> PathBuf {
    let base = std::env::var_os("ZEBRA_WAL_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")));

    base.join(format!("wal-bench-{}", std::process::id()))
}

/// Creates a fresh, empty on-disk finalized state under a unique directory, with
/// checkpoint WAL-skipping set to `skip_wal`.
fn new_state(network: &Network, run_root: &Path, skip_wal: bool) -> FinalizedState {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);

    let cache_dir = run_root.join(format!("db-{unique}"));
    std::fs::create_dir_all(&cache_dir).expect("can create benchmark cache dir on disk");

    // A real on-disk, non-ephemeral database (ephemeral databases live under
    // `std::env::temp_dir()`, which is often tmpfs and would hide the WAL cost).
    let config = Config {
        cache_dir,
        ephemeral: false,
        should_backup_non_finalized_state: false,
        delete_old_database: false,
        ..Config::default()
    };

    let mut state = FinalizedState::new(
        &config,
        network,
        #[cfg(feature = "elasticsearch")]
        false,
    );
    state.set_checkpoint_skip_wal(skip_wal);

    state
}

fn bench_checkpoint_commit(c: &mut Criterion) {
    let (network, blocks) = generate_chain();
    let run_root = run_root();

    let mut group = c.benchmark_group("checkpoint_commit");
    // Report results per committed block.
    group.throughput(Throughput::Elements(blocks.len() as u64));
    // Each sample rebuilds a fresh on-disk database in (untimed) setup, so allow
    // a longer measurement window to get tighter confidence intervals.
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(20));

    for skip_wal in [false, true] {
        let label = if skip_wal {
            "wal_disabled"
        } else {
            "wal_enabled"
        };

        group.bench_function(label, |b| {
            b.iter_batched(
                // Setup (not timed): a fresh empty database.
                || new_state(&network, &run_root, skip_wal),
                // Routine (timed): commit the whole chain through the checkpoint path.
                |mut state| {
                    // Thread the note commitment trees from each commit into the
                    // next, exactly as the real checkpoint path does
                    // (`FinalizedState::commit_finalized`). This avoids a
                    // per-block tip-treestate read from the database, so the
                    // measurement reflects the genuine commit cost rather than an
                    // extra read that would dominate both variants equally.
                    let mut prev_trees = None;
                    for block in &blocks {
                        let checkpoint_verified = CheckpointVerifiedBlock::from(block.clone());
                        let (_hash, trees) = state
                            .commit_finalized_direct(
                                checkpoint_verified.into(),
                                prev_trees.take(),
                                "checkpoint_commit benchmark",
                            )
                            .expect("benchmark block commits");
                        prev_trees = Some(trees);
                    }
                    // Returned so the database is dropped *outside* the timed
                    // section: the WAL-disabled variant should not be charged for
                    // the final flush, which it defers past the commit loop.
                    state
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();

    // Best-effort cleanup of this run's databases.
    let _ = std::fs::remove_dir_all(&run_root);
}

criterion_group!(benches, bench_checkpoint_commit);
criterion_main!(benches);
