//! `zakura-commit-bench` — a standalone benchmark for the block-commit stack
//! *below* Zakura block-sync.
//!
//! It drives **real** mainnet blocks through the **real** checkpoint verifier
//! and **real** finalized state, at production apply concurrency, so the
//! checkpoint/execution/commit path can be measured and profiled in isolation
//! from the networking layer. See `README.md`.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;

mod fetch;
mod metrics_rec;
mod run;
mod snapshot;
mod stats;
mod validate_cache;

#[cfg(feature = "jemalloc-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Benchmark the Zakura block-commit stack (checkpoint verify + state commit).
#[derive(Parser, Debug)]
#[command(name = "zakura-commit-bench", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Download + extract a Zebra state snapshot into ~/.zakura/snapshots.
    Snapshot(snapshot::SnapshotArgs),
    /// Download real blocks from a node's JSON-RPC into a local cache.
    Fetch(fetch::FetchArgs),
    /// Replay cached blocks through the real verifier+state and report throughput.
    Run(run::RunArgs),
    /// Validate a contiguous cached block range before replaying it.
    ValidateCache(validate_cache::ValidateArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("warn,zakura_commit_bench=info")
            }),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Snapshot(args) => snapshot::run(args).await,
        Command::Fetch(args) => fetch::run(args).await,
        Command::Run(args) => run::run(args).await,
        Command::ValidateCache(args) => validate_cache::run(args),
    }
}
