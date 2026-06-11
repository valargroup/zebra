//! `reset-non-finalized-state` subcommand - delete Zebra's non-finalized state backup cache.

use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use abscissa_core::{Application, Command, Runnable};
use clap::Parser;
use color_eyre::eyre::{eyre, Result};

use zebra_chain::parameters::Network;
use zebra_state::forked_mainnet_marker_contents;

use crate::prelude::APPLICATION;

/// Delete the non-finalized state backup cache for a configured network.
#[derive(Command, Debug, Default, Parser)]
pub struct ResetNonFinalizedStateCmd {
    /// Path to Zebra's cached state.
    ///
    /// Defaults to `state.cache_dir` from the loaded `zebrad.toml`.
    #[clap(long, short, help = "path to directory with the Zebra chain state")]
    cache_dir: Option<PathBuf>,

    /// Network whose non-finalized backup cache should be deleted.
    ///
    /// Defaults to `network.network` from the loaded `zebrad.toml`. Use the
    /// config default for forked-mainnet because forked networks need their
    /// full structured config.
    #[clap(long, short, help = "network backup cache to delete")]
    network: Option<Network>,

    /// Preview the reset without deleting anything.
    #[clap(long, help = "show the reset target without deleting it")]
    dry_run: bool,

    /// Delete the selected non-finalized state backup cache.
    #[clap(long, help = "delete the selected non-finalized backup cache")]
    force: bool,

    /// Confirm deletion of the Mainnet non-finalized backup cache.
    ///
    /// This is required with `--force` when the selected network is Mainnet.
    #[clap(long, help = "confirm deletion when the selected network is Mainnet")]
    confirm_mainnet: bool,
}

impl Runnable for ResetNonFinalizedStateCmd {
    fn run(&self) {
        let config = APPLICATION.config();

        if let Err(error) =
            self.run_with_config(config.state.clone(), config.network.network.clone())
        {
            tracing::error!("Failed to reset non-finalized state: {error}");
            std::process::exit(1);
        }
    }
}

impl ResetNonFinalizedStateCmd {
    /// Runs non-finalized state reset using `state_config` and `configured_network`.
    #[allow(clippy::print_stdout)]
    pub fn run_with_config(
        &self,
        mut state_config: zebra_state::Config,
        configured_network: Network,
    ) -> Result<ResetNonFinalizedStateSummary> {
        if self.dry_run && self.force {
            return Err(eyre!("--dry-run and --force cannot be used together"));
        }

        if !self.dry_run && !self.force {
            return Err(eyre!(
                "refusing to delete non-finalized state without --force; use --dry-run to preview"
            ));
        }

        if let Some(cache_dir) = self.cache_dir.clone() {
            state_config.cache_dir = cache_dir;
        }

        let network = self.network.clone().unwrap_or(configured_network);
        let summary = preview_reset_non_finalized_state(&state_config, &network)?;
        if self.force && summary.exists {
            validate_forked_mainnet_marker(&summary)?;
            validate_mainnet_confirmation(&summary, self.confirm_mainnet)?;
        }

        if self.dry_run {
            print_summary("non-finalized reset plan", &summary);
        } else if summary.exists {
            print_summary("non-finalized reset target", &summary);
            fs::remove_dir_all(&summary.path)?;
            if let Some(marker_path) = &summary.forked_mainnet_marker_path {
                if let Err(error) = fs::remove_file(marker_path) {
                    if error.kind() != ErrorKind::NotFound {
                        return Err(error.into());
                    }
                }
            }
            print_summary("non-finalized reset complete", &summary);
        } else {
            print_summary("non-finalized reset complete", &summary);
        }

        Ok(summary)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetNonFinalizedStateSummary {
    /// Network whose non-finalized backup cache was selected.
    pub network: Network,

    /// Selected backup directory.
    pub path: PathBuf,

    /// Whether the backup directory exists.
    pub exists: bool,

    /// Number of files below the backup directory.
    pub file_count: u64,

    /// Total bytes used by files below the backup directory.
    pub byte_count: u64,

    /// Marker file path for forked-mainnet backup caches.
    pub forked_mainnet_marker_path: Option<PathBuf>,

    /// Whether the marker file currently matches the configured fork.
    pub forked_mainnet_marker_matches: Option<bool>,
}

fn preview_reset_non_finalized_state(
    state_config: &zebra_state::Config,
    network: &Network,
) -> Result<ResetNonFinalizedStateSummary> {
    let Some(path) = state_config.non_finalized_state_backup_dir(network) else {
        return Err(eyre!(
            "non-finalized state backups are disabled for {network}; nothing to reset"
        ));
    };

    validate_backup_dir_path(state_config, &path)?;

    let Some((file_count, byte_count)) = directory_file_count_and_size(&path)? else {
        return Ok(ResetNonFinalizedStateSummary {
            network: network.clone(),
            path,
            exists: false,
            file_count: 0,
            byte_count: 0,
            forked_mainnet_marker_path: state_config.forked_mainnet_marker_path(network),
            forked_mainnet_marker_matches: forked_mainnet_marker_matches(state_config, network)?,
        });
    };

    Ok(ResetNonFinalizedStateSummary {
        network: network.clone(),
        path,
        exists: true,
        file_count,
        byte_count,
        forked_mainnet_marker_path: state_config.forked_mainnet_marker_path(network),
        forked_mainnet_marker_matches: forked_mainnet_marker_matches(state_config, network)?,
    })
}

fn validate_forked_mainnet_marker(summary: &ResetNonFinalizedStateSummary) -> Result<()> {
    let Network::ForkedMainnet(_) = &summary.network else {
        return Ok(());
    };

    if summary.forked_mainnet_marker_matches == Some(true) {
        return Ok(());
    }

    let marker_path = summary
        .forked_mainnet_marker_path
        .as_ref()
        .expect("forked-mainnet summaries include marker paths");

    Err(eyre!(
        "refusing to delete forked-mainnet non-finalized cache because marker {} \
         is missing or does not match the configured fork",
        marker_path.display()
    ))
}

fn validate_mainnet_confirmation(
    summary: &ResetNonFinalizedStateSummary,
    confirm_mainnet: bool,
) -> Result<()> {
    if summary.network == Network::Mainnet && !confirm_mainnet {
        return Err(eyre!(
            "refusing to delete Mainnet non-finalized cache without --confirm-mainnet; \
             use --dry-run to preview the exact target path first"
        ));
    }

    Ok(())
}

fn forked_mainnet_marker_matches(
    state_config: &zebra_state::Config,
    network: &Network,
) -> Result<Option<bool>> {
    let Some(marker_path) = state_config.forked_mainnet_marker_path(network) else {
        return Ok(None);
    };
    let expected_marker = forked_mainnet_marker_contents(network)
        .expect("marker path only exists for forked-mainnet");

    match fs::read_to_string(marker_path) {
        Ok(actual_marker) => Ok(Some(actual_marker == expected_marker)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Some(false)),
        Err(error) => Err(error.into()),
    }
}

fn validate_backup_dir_path(state_config: &zebra_state::Config, path: &Path) -> Result<()> {
    let cache_dir = normalized_path(&state_config.cache_dir)?;
    let non_finalized_root = cache_dir.join("non_finalized_state");

    if path.exists() {
        let canonical_path = path.canonicalize().map_err(|error| {
            eyre!(
                "failed to canonicalize non-finalized backup dir {}: {error}",
                path.display()
            )
        })?;

        if !canonical_path.starts_with(&non_finalized_root) {
            return Err(eyre!(
                "refusing to reset non-finalized backup outside cache dir: {} is not under {}",
                canonical_path.display(),
                non_finalized_root.display()
            ));
        }
    } else if !normalized_path(path)?.starts_with(&non_finalized_root) {
        return Err(eyre!(
            "refusing to reset non-finalized backup outside cache dir: {} is not under {}",
            path.display(),
            non_finalized_root.display()
        ));
    }

    Ok(())
}

fn normalized_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return path
            .canonicalize()
            .map_err(|error| eyre!("failed to canonicalize {}: {error}", path.display()));
    }

    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn directory_file_count_and_size(path: &Path) -> Result<Option<(u64, u64)>> {
    let mut file_count = 0;
    let mut byte_count = 0;
    let mut pending = vec![path.to_path_buf()];

    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                file_count += 1;
                byte_count += metadata.len();
            }
        }
    }

    Ok(Some((file_count, byte_count)))
}

#[allow(clippy::print_stdout)]
fn print_summary(label: &str, summary: &ResetNonFinalizedStateSummary) {
    println!("{label}:");
    println!("  network: {}", summary.network);
    println!("  path: {}", summary.path.display());
    println!("  exists: {}", summary.exists);
    println!("  files: {}", summary.file_count);
    println!("  bytes: {}", summary.byte_count);

    if let Some(marker_path) = &summary.forked_mainnet_marker_path {
        println!("  fork marker: {}", marker_path.display());
        println!(
            "  fork marker matches config: {}",
            summary.forked_mainnet_marker_matches.unwrap_or(false)
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write as _;

    use clap::Parser as _;

    use super::*;
    use crate::commands::{entry_point::EntryPoint, ZebradCmd};
    use zebra_chain::{
        block::{self, Height},
        parameters::{fork, Magic, NetworkUpgrade},
        work::difficulty::ParameterDifficulty,
    };

    fn config_with_temp_cache() -> (tempfile::TempDir, zebra_state::Config) {
        let temp_dir = tempfile::Builder::new()
            .prefix("zebra-non-finalized-reset")
            .tempdir()
            .expect("temporary directory is created successfully");
        let config = zebra_state::Config {
            cache_dir: temp_dir.path().to_path_buf(),
            ..zebra_state::Config::default()
        };

        (temp_dir, config)
    }

    fn forked_mainnet_network() -> Network {
        let fork_height = Height(3_400_000);
        let fork_hash = block::Hash([0x11; 32]);
        let network_magic = Magic([0xab, 0xcd, 0xef, 0x01]);
        let post_fork_height = fork_height
            .next()
            .expect("test fork height is below Height::MAX");
        let post_fork_activation_heights =
            BTreeMap::from([(post_fork_height, NetworkUpgrade::Nu7)]);
        let post_fork_limit = Network::Mainnet.target_difficulty_limit().to_compact();

        Network::new_forked_mainnet(
            fork::Parameters::new(
                "LocalFork",
                fork_height,
                fork_hash,
                network_magic,
                post_fork_activation_heights,
                post_fork_limit,
                true,
            )
            .expect("test fork parameters should be valid"),
        )
    }

    #[test]
    fn reset_non_finalized_state_args_parse() {
        let args = EntryPoint::process_cli_args(
            [
                "zebrad",
                "reset-non-finalized-state",
                "--network",
                "mainnet",
                "--dry-run",
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        )
        .expect("reset args should preprocess");

        let entry_point = EntryPoint::try_parse_from(args).expect("reset args should parse");
        let ZebradCmd::ResetNonFinalizedState(ResetNonFinalizedStateCmd {
            network: Some(Network::Mainnet),
            dry_run: true,
            force: false,
            ..
        }) = entry_point.cmd()
        else {
            panic!("expected reset-non-finalized-state command");
        };
    }

    #[test]
    fn reset_non_finalized_state_requires_force_without_dry_run() {
        let (_temp_dir, config) = config_with_temp_cache();
        let cmd = ResetNonFinalizedStateCmd::default();

        let error = cmd
            .run_with_config(config, Network::Mainnet)
            .expect_err("reset without force should fail");

        assert!(error.to_string().contains("--force"));
    }

    #[test]
    fn reset_non_finalized_state_dry_run_keeps_cache() {
        let (_temp_dir, config) = config_with_temp_cache();
        let backup_dir = config
            .non_finalized_state_backup_dir(&Network::Mainnet)
            .expect("mainnet backups are enabled");
        fs::create_dir_all(&backup_dir).expect("backup directory should be created");

        let mut file = fs::File::create(backup_dir.join("block")).expect("file should be created");
        file.write_all(b"block bytes")
            .expect("file should be written");

        let cmd = ResetNonFinalizedStateCmd {
            dry_run: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let summary = cmd
            .run_with_config(config, Network::Mainnet)
            .expect("dry-run should succeed");

        assert!(summary.exists);
        assert_eq!(summary.file_count, 1);
        assert_eq!(summary.byte_count, 11);
        assert!(backup_dir.exists());
    }

    #[test]
    fn reset_non_finalized_state_force_deletes_cache() {
        let (_temp_dir, config) = config_with_temp_cache();
        let backup_dir = config
            .non_finalized_state_backup_dir(&Network::Mainnet)
            .expect("mainnet backups are enabled");
        fs::create_dir_all(&backup_dir).expect("backup directory should be created");
        fs::write(backup_dir.join("block"), b"block bytes").expect("file should be written");

        let cmd = ResetNonFinalizedStateCmd {
            force: true,
            confirm_mainnet: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let summary = cmd
            .run_with_config(config, Network::Mainnet)
            .expect("forced reset should succeed");

        assert!(summary.exists);
        assert!(!backup_dir.exists());
    }

    #[test]
    fn reset_mainnet_requires_confirmation() {
        let (_temp_dir, config) = config_with_temp_cache();
        let backup_dir = config
            .non_finalized_state_backup_dir(&Network::Mainnet)
            .expect("mainnet backups are enabled");
        fs::create_dir_all(&backup_dir).expect("backup directory should be created");
        fs::write(backup_dir.join("block"), b"block bytes").expect("file should be written");

        let cmd = ResetNonFinalizedStateCmd {
            force: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let error = cmd
            .run_with_config(config, Network::Mainnet)
            .expect_err("forced mainnet reset without confirmation should fail");

        assert!(error.to_string().contains("--confirm-mainnet"));
        assert!(backup_dir.exists());
    }

    #[test]
    fn reset_non_finalized_state_missing_cache_succeeds() {
        let (_temp_dir, config) = config_with_temp_cache();
        let backup_dir = config
            .non_finalized_state_backup_dir(&Network::Mainnet)
            .expect("mainnet backups are enabled");

        let cmd = ResetNonFinalizedStateCmd {
            force: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let summary = cmd
            .run_with_config(config, Network::Mainnet)
            .expect("missing backup directory should be a successful no-op");

        assert!(!summary.exists);
        assert_eq!(summary.path, backup_dir);
    }

    #[test]
    fn reset_forked_mainnet_requires_matching_marker() {
        let (_temp_dir, config) = config_with_temp_cache();
        let forked_mainnet = forked_mainnet_network();
        let backup_dir = config
            .non_finalized_state_backup_dir(&forked_mainnet)
            .expect("forked-mainnet backups are enabled");
        fs::create_dir_all(&backup_dir).expect("backup directory should be created");
        fs::write(backup_dir.join("block"), b"block bytes").expect("file should be written");

        let cmd = ResetNonFinalizedStateCmd {
            force: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let error = cmd
            .run_with_config(config, forked_mainnet)
            .expect_err("forced fork reset without matching marker should fail");

        assert!(error.to_string().contains("marker"));
        assert!(backup_dir.exists());
    }

    #[test]
    fn reset_forked_mainnet_deletes_cache_with_matching_marker() {
        let (_temp_dir, config) = config_with_temp_cache();
        let forked_mainnet = forked_mainnet_network();
        let backup_dir = config
            .non_finalized_state_backup_dir(&forked_mainnet)
            .expect("forked-mainnet backups are enabled");
        let marker_path = config
            .forked_mainnet_marker_path(&forked_mainnet)
            .expect("forked-mainnet marker path should exist");
        fs::create_dir_all(&backup_dir).expect("backup directory should be created");
        fs::write(backup_dir.join("block"), b"block bytes").expect("file should be written");
        fs::write(
            &marker_path,
            forked_mainnet_marker_contents(&forked_mainnet)
                .expect("forked-mainnet marker contents should exist"),
        )
        .expect("marker should be written");

        let cmd = ResetNonFinalizedStateCmd {
            force: true,
            ..ResetNonFinalizedStateCmd::default()
        };
        let summary = cmd
            .run_with_config(config, forked_mainnet)
            .expect("forced fork reset with matching marker should succeed");

        assert!(summary.exists);
        assert_eq!(summary.forked_mainnet_marker_matches, Some(true));
        assert!(!backup_dir.exists());
        assert!(!marker_path.exists());
    }
}
