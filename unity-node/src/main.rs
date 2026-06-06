mod config;
mod regtest_producer;
mod supervisor;

use std::path::PathBuf;

use anyhow::Result;
use clap::ArgAction;
use clap::{Parser, Subcommand, ValueEnum};

use crate::supervisor::{start_stack, status_stack, stop_stack, StartOptions};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum NetworkArg {
    Regtest,
    Testnet,
    MainnetLike,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum RegtestProducerArg {
    Internal,
    External,
}

#[derive(Parser, Debug)]
#[command(name = "unity-node")]
#[command(about = "Single-CLI zebrad + zcashd stack supervisor")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start zebrad primary then zcashd follower
    Start {
        /// Network to run
        #[arg(long, value_enum, default_value = "regtest")]
        network: NetworkArg,
        /// Base state directory for data/config/pids
        #[arg(long, default_value = "/var/lib/unity-node")]
        state_dir: PathBuf,
        /// Manifest file containing pinned binaries and hashes
        #[arg(long, default_value = "unity-node/manifest.toml")]
        manifest: PathBuf,
        /// Seconds to wait for each readiness gate
        #[arg(long, default_value_t = 120)]
        timeout_secs: u64,
        /// Enforce Zebra-authoritative regtest producer + zcashd follower checks
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        canonical_regtest: bool,
        /// Max allowed block lag from zcashd follower behind zebra
        #[arg(long, default_value_t = 20)]
        follower_lag_tolerance: i64,
        /// Regtest producer mode
        #[arg(long, value_enum, default_value = "external")]
        regtest_producer: RegtestProducerArg,
        /// Optional external producer command (`sh -c`)
        #[arg(long)]
        external_producer_cmd: Option<String>,
        /// Built-in producer submission interval seconds
        #[arg(long, default_value_t = 2)]
        producer_interval_secs: u64,
    },
    /// Show process and health status
    Status {
        #[arg(long, value_enum, default_value = "regtest")]
        network: NetworkArg,
        #[arg(long, default_value = "/var/lib/unity-node")]
        state_dir: PathBuf,
        #[arg(long, default_value = "unity-node/manifest.toml")]
        manifest: PathBuf,
    },
    /// Stop zcashd then zebrad
    Stop {
        #[arg(long, value_enum, default_value = "regtest")]
        network: NetworkArg,
        #[arg(long, default_value = "/var/lib/unity-node")]
        state_dir: PathBuf,
    },
    /// Internal use: run built-in regtest producer harness
    #[command(hide = true)]
    ProducerHarness {
        #[arg(long)]
        rpc_addr: String,
        #[arg(long)]
        cookie_path: String,
        #[arg(long, default_value_t = 2)]
        interval_secs: u64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            network,
            state_dir,
            manifest,
            timeout_secs,
            canonical_regtest,
            follower_lag_tolerance,
            regtest_producer,
            external_producer_cmd,
            producer_interval_secs,
        } => start_stack(
            network.into(),
            &state_dir,
            &manifest,
            timeout_secs,
            StartOptions {
                canonical_regtest,
                follower_lag_tolerance,
                regtest_producer_external: matches!(regtest_producer, RegtestProducerArg::External),
                external_producer_cmd,
                producer_interval_secs,
            },
        ),
        Commands::Status {
            network,
            state_dir,
            manifest,
        } => status_stack(network.into(), &state_dir, &manifest),
        Commands::Stop { network, state_dir } => stop_stack(network.into(), &state_dir),
        Commands::ProducerHarness {
            rpc_addr,
            cookie_path,
            interval_secs,
        } => regtest_producer::run_harness(&rpc_addr, &cookie_path, interval_secs),
    }
}

impl From<NetworkArg> for config::Network {
    fn from(value: NetworkArg) -> Self {
        match value {
            NetworkArg::Regtest => config::Network::Regtest,
            NetworkArg::Testnet => config::Network::Testnet,
            NetworkArg::MainnetLike => config::Network::MainnetLike,
        }
    }
}
