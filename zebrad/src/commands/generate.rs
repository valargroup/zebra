//! `generate` subcommand - generates a default `zebrad.toml` config.

use crate::config::ZebradConfig;
use abscissa_core::{Command, Runnable};
use clap::Parser;

/// Generate a default `zebrad.toml` configuration
#[derive(Command, Debug, Default, Parser)]
pub struct GenerateCmd {
    /// The file to write the generated config to.
    //
    // TODO: use PathBuf here instead, to support non-UTF-8 paths
    #[clap(
        long,
        short,
        help = "The file to write the generated config to (stdout if unspecified)"
    )]
    output_file: Option<String>,
}

impl Runnable for GenerateCmd {
    /// Start the application.
    #[allow(clippy::print_stdout)]
    fn run(&self) {
        let default_config = ZebradConfig::default();
        let mut output = r"# Default configuration for zebrad.
#
# This file can be used as a skeleton for custom configs.
#
# Unspecified fields use default values. Optional fields are Some(field) if the
# field is present and None if it is absent.
#
# This file is generated as an example using zebrad's current defaults.
# You should set only the config options you want to keep, and delete the rest.
# Only a subset of fields are present in the skeleton, since optional values
# whose default is None are omitted.
#
# The config format (including a complete list of sections and fields) is
# documented here:
# https://docs.rs/zebrad/latest/zebrad/config/struct.ZebradConfig.html
#
# CONFIGURATION SOURCES (in order of precedence, highest to lowest):
#
# 1. Environment variables with ZEBRA_ prefix (highest precedence)
#    - Format: ZEBRA_SECTION__KEY (double underscore for nested keys)
#    - Examples:
#      - ZEBRA_NETWORK__NETWORK=Testnet
#      - ZEBRA_RPC__LISTEN_ADDR=127.0.0.1:8232
#      - ZEBRA_STATE__CACHE_DIR=/path/to/cache
#      - ZEBRA_TRACING__FILTER=debug
#      - ZEBRA_METRICS__ENDPOINT_ADDR=0.0.0.0:9999
#
# 2. Configuration file (TOML format)
#    - At the path specified via -c flag, e.g. `zebrad -c myconfig.toml start`, or
#    - At the default path in the user's preference directory (platform-dependent, see below)
#
# 3. Hard-coded defaults (lowest precedence)
#
# The user's preference directory and the default path to the `zebrad` config are platform dependent,
# based on `dirs::preference_dir`, see https://docs.rs/dirs/latest/dirs/fn.preference_dir.html :
#
# | Platform | Value                                 | Example                                        |
# | -------- | ------------------------------------- | ---------------------------------------------- |
# | Linux    | `$XDG_CONFIG_HOME` or `$HOME/.config` | `/home/alice/.config/zebrad.toml`              |
# | macOS    | `$HOME/Library/Preferences`           | `/Users/Alice/Library/Preferences/zebrad.toml` |
# | Windows  | `{FOLDERID_RoamingAppData}`           | `C:\Users\Alice\AppData\Local\zebrad.toml`     |

"
        .to_owned();

        // this avoids a ValueAfterTable error
        // https://github.com/alexcrichton/toml-rs/issues/145
        let conf = toml::Value::try_from(default_config).unwrap();
        let conf = toml::to_string_pretty(&conf).expect("default config should be serializable");
        output += &document_network_p2p_config(&conf);
        match self.output_file {
            Some(ref output_file) => {
                use std::{fs::File, io::Write};
                File::create(output_file)
                    .expect("must be able to open output file")
                    .write_all(output.as_bytes())
                    .expect("must be able to write output");
            }
            None => {
                println!("{output}");
            }
        }
    }
}

fn document_network_p2p_config(config: &str) -> String {
    let had_trailing_newline = config.ends_with('\n');
    let mut lines = config.lines().map(ToString::to_string).collect::<Vec<_>>();

    let Some(network_start) = lines.iter().position(|line| line == "[network]") else {
        return config.to_string();
    };
    let mut network_end = lines
        .iter()
        .enumerate()
        .skip(network_start + 1)
        .find_map(|(index, line)| line.starts_with('[').then_some(index))
        .unwrap_or(lines.len());

    let Some(v2_p2p_index) = lines[network_start + 1..network_end]
        .iter()
        .position(|line| line.starts_with("v2_p2p = "))
        .map(|index| index + network_start + 1)
    else {
        return config.to_string();
    };

    let v2_p2p_line = lines.remove(v2_p2p_index);
    network_end -= 1;

    let Some(legacy_p2p_index) = lines[network_start + 1..network_end]
        .iter()
        .position(|line| line.starts_with("legacy_p2p = "))
        .map(|index| index + network_start + 1)
    else {
        return config.to_string();
    };

    let comments = [
        "# P2P stack selection. `legacy_p2p` controls the legacy TCP stack.",
        "# `v2_p2p` accepts \"default\", true, or false. \"default\" disables",
        "# Zakura P2P v2 on Mainnet and enables it on Testnet, Regtest, and other networks.",
        "# Set `v2_p2p` to true or false to override the selected network's default.",
    ]
    .map(ToString::to_string);
    let comments_len = comments.len();

    lines.splice(legacy_p2p_index..legacy_p2p_index, comments);
    let v2_p2p_insert_index = legacy_p2p_index + comments_len + 1;
    lines.insert(v2_p2p_insert_index, v2_p2p_line);

    let mut output = lines.join("\n");
    if had_trailing_newline {
        output.push('\n');
    }
    output
}
