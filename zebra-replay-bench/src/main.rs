//! `zebra-replay-bench` — replay mainnet blocks through the Zebra state committer
//! in isolation from the network, to benchmark the commit pipeline.
//!
//! Two phases share a flat block cache:
//!   * `index` reads a height window from a snapshot DB (read-only) into the cache;
//!   * `apply` replays the cache onto a writable fork at `start-1`, timing each
//!     `commit_finalized_direct` call and verifying the final tip hash.
//!
//! See `README.md` for the end-to-end harness usage.

// This is an operator-facing benchmark CLI: its results go to stdout/stderr.
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

mod apply;
mod cache;
mod config;
mod index;
mod rollback;
mod roots_cache;
mod stats;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result};
use zebra_chain::parameters::Network;
use zebra_state::{FinalizedState, HashOrHeight};

use config::state_config;

#[derive(Parser)]
#[command(
    name = "zebra-replay-bench",
    about = "Replay mainnet blocks through the state committer, isolated from the network"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print a snapshot's finalized tip height and hash (read-only).
    Info {
        /// Snapshot root containing `state/vN/<network>`.
        #[arg(long)]
        src: PathBuf,
    },
    /// Read blocks `start..=end` from a snapshot into a flat cache file.
    Index {
        /// Snapshot root to read from (opened read-only).
        #[arg(long)]
        src: PathBuf,
        /// Output cache file path.
        #[arg(long)]
        cache: PathBuf,
        /// First height to index (inclusive).
        #[arg(long)]
        start: u32,
        /// Last height to index (inclusive).
        #[arg(long)]
        end: u32,
    },
    /// Read per-height anchor roots (+ the successor of `end`) into a VCT sidecar.
    IndexRoots {
        /// Snapshot root to read from (opened on a writable fork).
        #[arg(long)]
        src: PathBuf,
        /// Output sidecar file path.
        #[arg(long)]
        sidecar: PathBuf,
        /// First height (inclusive); must match the block cache's start.
        #[arg(long)]
        start: u32,
        /// Last height (inclusive); must match the block cache's end.
        #[arg(long)]
        end: u32,
    },
    /// Roll a base fork's finalized tip back to `target` (to manufacture a base
    /// at `start-1` from a higher snapshot).
    Rollback {
        /// Base fork root (opened writable, mutated in place).
        #[arg(long)]
        base: PathBuf,
        /// Height to roll the finalized tip back to.
        #[arg(long)]
        target: u32,
    },
    /// Replay a cache onto a writable base fork (tip must be `start-1`).
    Apply {
        /// Base fork root (opened writable; must be at height start-1).
        #[arg(long)]
        base: PathBuf,
        /// Cache file produced by `index`.
        #[arg(long)]
        cache: PathBuf,
        /// VCT roots sidecar produced by `index-roots`. When set, the committer
        /// runs the VCT fast path (folds supplied roots, skips the recompute);
        /// otherwise the legacy full-recompute path runs.
        #[arg(long)]
        vct_sidecar: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    // The snapshots under test are Mainnet; widen this when a need arises.
    let network = Network::Mainnet;

    match cli.cmd {
        Cmd::Info { src } => {
            let config = state_config(src.clone(), true);
            let state = FinalizedState::new_read_only(&config, &network);
            let tip = state
                .db
                .tip()
                .ok_or_else(|| eyre!("snapshot has no finalized tip"))?;
            let has_body = state.db.block(HashOrHeight::Height(tip.0)).is_some();
            println!(
                "src={}\ntip_height={}\ntip_hash={}\ntip_body_present={}",
                src.display(),
                tip.0 .0,
                tip.1,
                has_body
            );
        }
        Cmd::Index {
            src,
            cache,
            start,
            end,
        } => {
            index::run(&src, &cache, start, end, network)?;
        }
        Cmd::IndexRoots {
            src,
            sidecar,
            start,
            end,
        } => {
            index::run_roots(&src, &sidecar, start, end, network)?;
        }
        Cmd::Rollback { base, target } => {
            rollback::run(&base, target, network)?;
        }
        Cmd::Apply {
            base,
            cache,
            vct_sidecar,
        } => {
            #[cfg(feature = "commit-metrics")]
            let handle = install_metrics();

            apply::run(&base, &cache, vct_sidecar.as_deref(), network)?;

            #[cfg(feature = "commit-metrics")]
            render_metrics(handle);
        }
    }

    Ok(())
}

#[cfg(feature = "commit-metrics")]
fn install_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("installing the prometheus recorder")
}

#[cfg(feature = "commit-metrics")]
fn render_metrics(handle: metrics_exporter_prometheus::PrometheusHandle) {
    let rendered = handle.render();
    println!("--- commit-metrics (zebra_state.* / state_vct.*) ---");
    for line in rendered.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with("zebra_state_") || line.starts_with("state_vct_") {
            println!("{line}");
        }
    }
}
