//! `fork-mainnet` subcommand - generate config for a local Mainnet fork.

use std::{
    collections::BTreeMap, fs::File, io::Write, net::SocketAddr, path::PathBuf, str::FromStr,
};

use abscissa_core::{Command, Runnable};
use clap::{ArgAction, Parser, Subcommand};
use indexmap::IndexSet;

use zebra_chain::{
    block::{self, Height},
    parameters::{fork, Magic, Network, NetworkUpgrade},
    serialization::BytesInDisplayOrder,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, ParameterDifficulty as _},
};

use crate::config::ZebradConfig;

const DEFAULT_FORK_LISTEN_ADDR: &str = "127.0.0.1:28233";
const DEFAULT_EASY_FORK_TARGET_DIFFICULTY: &str = "037fffff";

/// Generate or manage a local fork of Mainnet.
#[derive(Command, Debug, Parser)]
pub struct ForkMainnetCmd {
    /// The fork-mainnet command to run.
    #[clap(subcommand)]
    command: ForkMainnetSubcommand,
}

#[derive(Debug, Subcommand)]
enum ForkMainnetSubcommand {
    /// Write a TOML config for starting a forked-mainnet node.
    Prepare(ForkMainnetPrepareCmd),
}

#[derive(Debug, Parser)]
struct ForkMainnetPrepareCmd {
    /// Mainnet height where the fork anchors.
    #[clap(long)]
    height: u32,

    /// Mainnet block hash at `--height`, in display-order hex.
    #[clap(long)]
    hash: String,

    /// Human-readable fork name used in logs and cache names.
    #[clap(long)]
    name: String,

    /// Four-byte fork network magic in hex, for example `a1b2c3d4`.
    #[clap(long, value_parser = parse_network_magic)]
    network_magic: [u8; 4],

    /// Post-fork activation as `UPGRADE=HEIGHT`.
    ///
    /// Supported upgrades: NU7, NU6.2, NU6.1, NU6, NU5, Canopy, Heartwood,
    /// Blossom, Sapling, Overwinter.
    #[clap(long = "activation", value_parser = parse_activation)]
    activations: Vec<ActivationHeight>,

    /// Use an easy post-fork DAA starting difficulty suitable for local CPU testing.
    #[clap(long, default_value_t = true, action = ArgAction::Set)]
    easy_difficulty: bool,

    /// Override the post-fork target difficulty limit as compact 8-hex or expanded 64-hex.
    #[clap(long, value_parser = parse_difficulty)]
    target_difficulty_limit: Option<CompactDifficulty>,

    /// Disable proof-of-work validation strictly after the fork height.
    #[clap(long)]
    disable_pow: bool,

    /// Explicit fork peer as `host:port`; repeat for multiple local fork peers.
    #[clap(long = "initial-peer")]
    initial_peers: Vec<String>,

    /// Listen address for this fork node.
    #[clap(long, default_value = DEFAULT_FORK_LISTEN_ADDR)]
    listen_addr: SocketAddr,

    /// The file to write the generated config to. Prints to stdout if unspecified.
    #[clap(long, short)]
    output_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug)]
struct ActivationHeight {
    network_upgrade: NetworkUpgrade,
    height: Height,
}

impl Runnable for ForkMainnetCmd {
    #[allow(clippy::print_stdout)]
    fn run(&self) {
        match &self.command {
            ForkMainnetSubcommand::Prepare(cmd) => {
                let output = match cmd.generated_config() {
                    Ok(output) => output,
                    Err(error) => {
                        eprintln!("failed to generate forked-mainnet config: {error}");
                        std::process::exit(1);
                    }
                };

                match &cmd.output_file {
                    Some(output_file) => {
                        if let Err(error) = File::create(output_file)
                            .and_then(|mut file| file.write_all(output.as_bytes()))
                        {
                            eprintln!(
                                "failed to write forked-mainnet config to {}: {error}",
                                output_file.display()
                            );
                            std::process::exit(1);
                        }
                    }
                    None => println!("{output}"),
                }
            }
        }
    }
}

impl ForkMainnetPrepareCmd {
    fn generated_config(&self) -> Result<String, String> {
        let fork_height = self.fork_height()?;
        let network = Network::new_forked_mainnet(
            fork::Parameters::new(
                &self.name,
                fork_height,
                block::Hash::from_str(&self.hash).map_err(|error| error.to_string())?,
                Magic(self.network_magic),
                self.post_fork_activation_heights(),
                self.target_difficulty_limit()?,
                self.disable_pow,
            )
            .map_err(|error| error.to_string())?,
        );

        let mut config = ZebradConfig::default();
        config.network.network = network;
        config.network.listen_addr = self.listen_addr;
        config.network.initial_mainnet_peers = IndexSet::new();
        config.network.initial_testnet_peers = IndexSet::new();
        config.network.initial_fork_peers = self.initial_peers.iter().cloned().collect();
        let mut output = format!(
            "# Forked Mainnet configuration generated by `zebrad fork-mainnet prepare`.\n\
             # Before starting with this config, stop Zebra and make sure the Mainnet finalized DB tip\n\
             # is exactly height {} with hash {}. Forked Mainnet reuses Mainnet finalized state\n\
             # up to that anchor, never finalizes fork-only blocks, and stores fork-only peer and\n\
             # non-finalized caches separately.\n\
             # To rejoin public Mainnet, stop Zebra, run:\n\
             #   zebrad -c <this-file> reset-non-finalized-state --force\n\
             # then restart with a normal Mainnet config.\n\n",
            fork_height.0, self.hash
        );

        let conf = toml::Value::try_from(config).map_err(|error| error.to_string())?;
        output.push_str(&toml::to_string_pretty(&conf).map_err(|error| error.to_string())?);

        Ok(output)
    }

    fn post_fork_activation_heights(&self) -> BTreeMap<Height, NetworkUpgrade> {
        self.activations
            .iter()
            .map(|activation| (activation.height, activation.network_upgrade))
            .collect()
    }

    fn fork_height(&self) -> Result<Height, String> {
        Height::try_from(self.height).map_err(|error| format!("fork height is invalid: {error}"))
    }

    fn target_difficulty_limit(&self) -> Result<CompactDifficulty, String> {
        if let Some(target_difficulty_limit) = self.target_difficulty_limit {
            return Ok(target_difficulty_limit);
        }

        if self.easy_difficulty {
            parse_difficulty(DEFAULT_EASY_FORK_TARGET_DIFFICULTY)
        } else {
            Ok(Network::Mainnet.target_difficulty_limit().to_compact())
        }
    }
}

fn parse_activation(activation: &str) -> Result<ActivationHeight, String> {
    let (network_upgrade, height) = activation
        .split_once('=')
        .ok_or_else(|| "activation must be formatted as UPGRADE=HEIGHT".to_string())?;

    Ok(ActivationHeight {
        network_upgrade: parse_network_upgrade(network_upgrade)?,
        height: Height::try_from(
            height
                .parse::<u32>()
                .map_err(|error| format!("activation height must be a u32: {error}"))?,
        )
        .map_err(|error| format!("activation height is invalid: {error}"))?,
    })
}

fn parse_network_upgrade(network_upgrade: &str) -> Result<NetworkUpgrade, String> {
    let normalized = network_upgrade
        .chars()
        .filter(|character| !matches!(character, '.' | '_' | '-'))
        .flat_map(char::to_uppercase)
        .collect::<String>();

    match normalized.as_str() {
        "OVERWINTER" => Ok(NetworkUpgrade::Overwinter),
        "SAPLING" => Ok(NetworkUpgrade::Sapling),
        "BLOSSOM" => Ok(NetworkUpgrade::Blossom),
        "HEARTWOOD" => Ok(NetworkUpgrade::Heartwood),
        "CANOPY" => Ok(NetworkUpgrade::Canopy),
        "NU5" => Ok(NetworkUpgrade::Nu5),
        "NU6" => Ok(NetworkUpgrade::Nu6),
        "NU61" => Ok(NetworkUpgrade::Nu6_1),
        "NU62" => Ok(NetworkUpgrade::Nu6_2),
        "NU7" => Ok(NetworkUpgrade::Nu7),
        _ => Err(format!(
            "unsupported network upgrade {network_upgrade:?}; use NU7, NU6.2, NU6.1, NU6, NU5, Canopy, Heartwood, Blossom, Sapling, or Overwinter"
        )),
    }
}

fn parse_network_magic(network_magic: &str) -> Result<[u8; 4], String> {
    let network_magic = network_magic.trim().trim_start_matches("0x");

    parse_hex_bytes(network_magic)
        .map_err(|error| format!("network magic must be exactly four hex bytes: {error}"))
}

fn parse_difficulty(difficulty: &str) -> Result<CompactDifficulty, String> {
    let difficulty = difficulty.trim().trim_start_matches("0x");

    match difficulty.len() {
        8 => CompactDifficulty::from_bytes_in_display_order(&parse_hex_bytes(difficulty)?)
            .map_err(|error| error.to_string()),
        64 => {
            if difficulty.chars().all(|character| character == '0') {
                return Err("zero difficulty values are invalid".to_string());
            }

            Ok(
                ExpandedDifficulty::from_bytes_in_display_order(&parse_hex_bytes(difficulty)?)
                    .to_compact(),
            )
        }
        _ => Err(format!(
            "difficulty values must be compact 8-hex or expanded 64-hex, got {} hex characters",
            difficulty.len()
        )),
    }
}

fn parse_hex_bytes<const BYTE_LEN: usize>(hex: &str) -> Result<[u8; BYTE_LEN], String> {
    if hex.len() != BYTE_LEN * 2 {
        return Err(format!(
            "expected {} hex characters, got {}",
            BYTE_LEN * 2,
            hex.len()
        ));
    }

    let mut bytes = [0; BYTE_LEN];
    for (pair, byte) in hex.as_bytes().chunks_exact(2).zip(bytes.iter_mut()) {
        let pair = std::str::from_utf8(pair)
            .map_err(|error| format!("hex input must be ASCII: {error}"))?;
        let value = u8::from_str_radix(pair, 16)
            .map_err(|error| format!("invalid hex byte {pair:?}: {error}"))?;
        *byte = value;
    }

    Ok(bytes)
}

#[allow(dead_code)]
#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use super::*;
    use crate::commands::{entry_point::EntryPoint, ZebradCmd};

    #[test]
    fn fork_mainnet_prepare_generates_fork_config() {
        let _init_guard = zebra_test::init();

        let args = EntryPoint::process_cli_args(
            [
                "zebrad",
                "fork-mainnet",
                "prepare",
                "--height",
                "3400000",
                "--hash",
                "1111111111111111111111111111111111111111111111111111111111111111",
                "--name",
                "LocalFork",
                "--network-magic",
                "a1b2c3d4",
                "--activation",
                "NU7=3400100",
                "--disable-pow",
                "--initial-peer",
                "127.0.0.1:38233",
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )
        .expect("fork-mainnet prepare args should preprocess");

        let entry_point =
            EntryPoint::try_parse_from(args).expect("fork-mainnet prepare args should parse");
        let ZebradCmd::ForkMainnet(ForkMainnetCmd {
            command: ForkMainnetSubcommand::Prepare(cmd),
        }) = entry_point.cmd()
        else {
            panic!("expected fork-mainnet prepare command");
        };

        let output = cmd
            .generated_config()
            .expect("fork-mainnet prepare config should generate");

        assert!(output.contains("Forked Mainnet configuration"));
        assert!(output.contains("reset-non-finalized-state --force"));
        assert!(!output.contains("debug_max_non_finalized_chain_length"));
        assert!(!output.contains("finalization_depth"));
        assert!(!output.contains("fixed_post_fork_difficulty"));
        assert!(output.contains("initial_fork_peers = [\"127.0.0.1:38233\"]"));
        assert!(output.contains("forked_mainnet"));
        assert!(output.contains("target_difficulty_limit = \"037fffff\""));
        assert!(output.contains("NU7 = 3400100"));

        let config: ZebradConfig =
            toml::from_str(&output).expect("generated fork-mainnet config should parse");
        assert!(config.network.initial_mainnet_peers.is_empty());
        assert!(config.network.initial_testnet_peers.is_empty());
        assert_eq!(
            config.network.initial_fork_peers,
            ["127.0.0.1:38233".to_string()].into()
        );
        assert_eq!(
            config.network.initial_peer_hostnames(),
            ["127.0.0.1:38233".to_string()].into(),
            "forked-mainnet startup should only use explicit fork peers"
        );
        let Network::ForkedMainnet(params) = &config.network.network else {
            panic!("generated config must use ForkedMainnet");
        };

        assert_eq!(
            params.post_fork_target_difficulty_limit(),
            parse_difficulty(DEFAULT_EASY_FORK_TARGET_DIFFICULTY)
                .expect("default easy difficulty should parse")
                .to_expanded()
                .expect("default easy difficulty should expand")
        );
        assert_eq!(
            params.post_fork_activation_heights(),
            &BTreeMap::from([(Height(3_400_100), NetworkUpgrade::Nu7)])
        );
    }

    #[test]
    fn fork_mainnet_prepare_rejects_height_above_max() {
        let cmd = ForkMainnetPrepareCmd {
            height: Height::MAX.0 + 1,
            hash: "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            name: "LocalFork".to_string(),
            network_magic: [0xa1, 0xb2, 0xc3, 0xd4],
            activations: Vec::new(),
            easy_difficulty: true,
            target_difficulty_limit: None,
            disable_pow: true,
            initial_peers: Vec::new(),
            listen_addr: DEFAULT_FORK_LISTEN_ADDR
                .parse()
                .expect("default listen address should parse"),
            output_file: None,
        };

        let error = cmd
            .generated_config()
            .expect_err("out-of-range fork height should fail cleanly");

        assert!(error.contains("fork height is invalid"));
    }

    #[test]
    fn fork_mainnet_prepare_rejects_activation_height_above_max() {
        let error = parse_activation(&format!("NU7={}", Height::MAX.0 + 1))
            .expect_err("out-of-range activation height should fail cleanly");

        assert!(error.contains("activation height is invalid"));
    }

    #[test]
    fn fork_mainnet_prepare_rejects_ambiguous_difficulty_length() {
        let error = parse_difficulty("1000")
            .expect_err("non-compact non-expanded difficulty should fail cleanly");

        assert!(error.contains("8-hex or expanded 64-hex"));
    }
}
