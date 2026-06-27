use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::{bail, Result, WrapErr};
use tower::ServiceExt;

use zebra_chain::{block, parameters::Network};

use crate::mode::RunMode;

pub const METADATA_FILE: &str = ".zakura-commit-bench.json";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct StateDirMetadata {
    pub snapshot_name: Option<String>,
    pub sha256: Option<String>,
    pub archive_path: Option<PathBuf>,
    pub expected_tip: Option<u32>,
    pub used_by_bench: bool,
    pub used_at_unix_seconds: Option<u64>,
    pub used_mode: Option<String>,
    pub measured_target: Option<u32>,
    pub lookahead_target: Option<u32>,
}

impl StateDirMetadata {
    pub fn new_snapshot(
        snapshot_name: String,
        sha256: String,
        archive_path: PathBuf,
        expected_tip: Option<u32>,
    ) -> Self {
        Self {
            snapshot_name: Some(snapshot_name),
            sha256: Some(sha256),
            archive_path: Some(archive_path),
            expected_tip,
            used_by_bench: false,
            ..Self::default()
        }
    }
}

pub fn metadata_path(state_dir: &Path) -> PathBuf {
    state_dir.join(METADATA_FILE)
}

pub fn read_metadata(state_dir: &Path) -> Result<Option<StateDirMetadata>> {
    let path = metadata_path(state_dir);
    if !path.is_file() {
        return Ok(None);
    }
    let raw = std::fs::read(&path)
        .wrap_err_with(|| format!("reading state metadata {}", path.display()))?;
    serde_json::from_slice(&raw)
        .wrap_err_with(|| format!("parsing state metadata {}", path.display()))
        .map(Some)
}

pub fn write_metadata(state_dir: &Path, metadata: &StateDirMetadata) -> Result<()> {
    let path = metadata_path(state_dir);
    let tmp = path.with_extension("json.tmp");
    let raw = serde_json::to_vec_pretty(metadata)?;
    std::fs::write(&tmp, raw).wrap_err_with(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).wrap_err_with(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

pub fn check_state_dir_not_used(state_dir: &Path, allow_used: bool) -> Result<()> {
    let Some(metadata) = read_metadata(state_dir)? else {
        eprintln!(
            "warning: {} has no {}; this benchmark will mutate the state dir",
            state_dir.display(),
            METADATA_FILE
        );
        return Ok(());
    };

    if metadata.used_by_bench && !allow_used {
        bail!(
            "{} was already used by zakura-commit-bench; pass --allow-used-state-dir to reuse it",
            state_dir.display()
        );
    }

    Ok(())
}

pub fn mark_used(
    state_dir: &Path,
    mode: RunMode,
    measured_target: block::Height,
    lookahead_target: block::Height,
) -> Result<()> {
    let mut metadata = read_metadata(state_dir)?.unwrap_or_default();
    metadata.used_by_bench = true;
    metadata.used_at_unix_seconds = Some(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    );
    metadata.used_mode = Some(mode.as_str().to_string());
    metadata.measured_target = Some(measured_target.0);
    metadata.lookahead_target = Some(lookahead_target.0);
    write_metadata(state_dir, &metadata)
}

pub async fn planning_tip_from_state_dir(
    state_dir: &Path,
    network: &Network,
) -> Result<block::Height> {
    if let Some(metadata) = read_metadata(state_dir)? {
        if let Some(tip) = metadata.expected_tip {
            return Ok(block::Height(tip));
        }
    }

    finalized_tip_from_state_dir(state_dir, network).await
}

async fn finalized_tip_from_state_dir(
    state_dir: &Path,
    network: &Network,
) -> Result<block::Height> {
    let (_, max_checkpoint_height) =
        zebra_consensus::router::init_checkpoint_list(zebra_consensus::Config::default(), network);
    let state_config = zebra_state::Config {
        cache_dir: state_dir.to_path_buf(),
        ephemeral: false,
        ..zebra_state::Config::default()
    };
    let (_state_service, read_state, _latest_chain_tip, _chain_tip_change) =
        zebra_state::init(state_config, network, max_checkpoint_height, 1).await;

    match read_state
        .oneshot(zebra_state::ReadRequest::FinalizedTip)
        .await
    {
        Ok(zebra_state::ReadResponse::FinalizedTip(Some((height, _)))) => Ok(height),
        Ok(zebra_state::ReadResponse::FinalizedTip(None)) => {
            bail!("--state-dir state has no finalized tip (empty snapshot?)")
        }
        other => bail!("unexpected FinalizedTip response: {other:?}"),
    }
}

pub fn expected_tip_from_snapshot_name(name: &str) -> Option<u32> {
    name.rsplit_once('-')?.1.parse().ok()
}

pub fn ensure_empty_or_replace(path: &Path, replace: bool) -> Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)
            .wrap_err_with(|| format!("creating extract dir {}", path.display()))?;
        return Ok(());
    }

    if path.is_dir() && path.read_dir()?.next().is_none() {
        return Ok(());
    }

    if !replace {
        bail!(
            "{} already exists and is not empty; pass --replace to extract over it",
            path.display()
        );
    }

    if path.is_dir() {
        std::fs::remove_dir_all(path)
            .wrap_err_with(|| format!("removing existing extract dir {}", path.display()))?;
    } else {
        std::fs::remove_file(path)
            .wrap_err_with(|| format!("removing existing extract path {}", path.display()))?;
    }
    std::fs::create_dir_all(path)
        .wrap_err_with(|| format!("creating extract dir {}", path.display()))
}

pub fn exact_fetch_command(
    cache_dir: &Path,
    state_dir: Option<&Path>,
    blocks: u32,
    mode: RunMode,
    with_roots: bool,
    network: &str,
) -> String {
    let mut command = format!(
        "cargo xtask zakura-commit-bench -- fetch --rpc-url <RPC_URL> --cache-dir {} --blocks {blocks} --mode {} --network {network}",
        cache_dir.display(),
        mode.as_str()
    );
    if let Some(state_dir) = state_dir {
        command.push_str(&format!(" --state-dir {}", state_dir.display()));
    }
    if with_roots {
        command.push_str(" --with-roots");
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_empty_extract_dir_requires_replace() {
        let temp = tempfile::tempdir().unwrap();
        let extract_dir = temp.path().join("snapshot");
        std::fs::create_dir_all(&extract_dir).unwrap();
        std::fs::write(extract_dir.join("existing"), b"x").unwrap();

        assert!(ensure_empty_or_replace(&extract_dir, false).is_err());
        ensure_empty_or_replace(&extract_dir, true).unwrap();
        assert!(extract_dir.is_dir());
        assert!(extract_dir.read_dir().unwrap().next().is_none());
    }

    #[test]
    fn parses_expected_tip_from_snapshot_name_suffix() {
        assert_eq!(
            expected_tip_from_snapshot_name("zebra-mainnet-20260616T032721Z-1707210"),
            Some(1_707_210)
        );
        assert_eq!(expected_tip_from_snapshot_name("custom"), None);
    }

    #[test]
    fn used_state_dir_requires_explicit_allow() {
        let temp = tempfile::tempdir().unwrap();
        write_metadata(
            temp.path(),
            &StateDirMetadata {
                used_by_bench: true,
                ..StateDirMetadata::default()
            },
        )
        .unwrap();

        assert!(check_state_dir_not_used(temp.path(), false).is_err());
        check_state_dir_not_used(temp.path(), true).unwrap();
    }
}
