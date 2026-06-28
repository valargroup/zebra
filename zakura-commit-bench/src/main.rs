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
mod mode;
mod range;
mod roots;
mod run;
mod snapshot;
mod state_dir;
mod stats;
mod status;
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
    /// Discover local snapshots, caches, and traces.
    Status(status::StatusArgs),
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
        Command::ValidateCache(args) => validate_cache::run(args).await,
        Command::Status(args) => status::run(args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_disk_peers_default_is_four() {
        let cli = Cli::parse_from(["zakura-commit-bench", "run"]);
        let Command::Run(args) = cli.command else {
            panic!("run command parsed");
        };
        assert_eq!(args.disk_peers, 4);
    }
}
