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
    pub rollback_target: Option<u32>,
    pub rollback_old_tip: Option<u32>,
    pub rollback_old_hash: Option<String>,
    pub rollback_new_tip: Option<u32>,
    pub rollback_new_hash: Option<String>,
    pub rollback_duration_ms: Option<u64>,
    pub rollback_at_unix_seconds: Option<u64>,
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

pub async fn check_state_dir_not_used(
    state_dir: &Path,
    allow_used: bool,
    network: &Network,
) -> Result<()> {
    let Some(metadata) = read_metadata(state_dir)? else {
        eprintln!(
            "warning: {} has no {}; this benchmark will mutate the state dir",
            state_dir.display(),
            METADATA_FILE
        );
        return Ok(());
    };

    if metadata.used_by_bench && !allow_used {
        let current_tip = match finalized_tip_from_state_dir(state_dir, network).await {
            Ok(tip) => Some(tip),
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "could not read used state dir tip for guard message"
                );
                None
            }
        };
        bail!(
            "{}",
            used_state_dir_message(state_dir, &metadata, current_tip)
        );
    }

    Ok(())
}

fn used_state_dir_message(
    state_dir: &Path,
    metadata: &StateDirMetadata,
    current_tip: Option<block::Height>,
) -> String {
    let base = format!(
        "{} was already used by zakura-commit-bench",
        state_dir.display()
    );

    match (metadata.rollback_target, current_tip) {
        (Some(target), Some(current_tip)) if current_tip.0 == target => format!(
            "{base}; the DB tip is back at the recorded rollback target {target}, but metadata \
             is still marked used. Run `zakura-commit-bench status --state-dir {}` to inspect it, \
             or pass --allow-used-state-dir if this state is intentionally reusable",
            state_dir.display()
        ),
        (Some(target), Some(current_tip)) => format!(
            "{base}; current tip is {}, recorded rollback target is {target}. Let the default \
             rollback complete, run with --rollback-dry-run to inspect the state, or pass \
             --allow-used-state-dir to override",
            current_tip.0
        ),
        _ => format!("{base}; pass --allow-used-state-dir to reuse it"),
    }
}

pub fn mark_used(
    state_dir: &Path,
    mode: RunMode,
    measured_target: block::Height,
    lookahead_target: block::Height,
) -> Result<()> {
    let mut metadata = read_metadata(state_dir)?.unwrap_or_default();
    metadata.used_by_bench = true;
    metadata.used_at_unix_seconds = Some(now_unix_seconds());
    metadata.used_mode = Some(mode.as_str().to_string());
    metadata.measured_target = Some(measured_target.0);
    metadata.lookahead_target = Some(lookahead_target.0);
    write_metadata(state_dir, &metadata)
}

pub fn mark_rollback_success(
    state_dir: &Path,
    target: block::Height,
    old_tip: (block::Height, block::Hash),
    new_tip: (block::Height, block::Hash),
    duration: std::time::Duration,
) -> Result<()> {
    let mut metadata = read_metadata(state_dir)?.unwrap_or_default();
    metadata.used_by_bench = false;
    metadata.rollback_target = Some(target.0);
    metadata.rollback_old_tip = Some(old_tip.0 .0);
    metadata.rollback_old_hash = Some(old_tip.1.to_string());
    metadata.rollback_new_tip = Some(new_tip.0 .0);
    metadata.rollback_new_hash = Some(new_tip.1.to_string());
    metadata.rollback_duration_ms = Some(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    metadata.rollback_at_unix_seconds = Some(now_unix_seconds());
    write_metadata(state_dir, &metadata)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

pub async fn finalized_tip_from_state_dir(
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
        let metadata = StateDirMetadata {
            used_by_bench: true,
            ..StateDirMetadata::default()
        };
        write_metadata(temp.path(), &metadata).unwrap();

        assert!(
            used_state_dir_message(temp.path(), &metadata, None).contains("--allow-used-state-dir")
        );
    }

    #[test]
    fn used_state_dir_message_mentions_recorded_rollback_target() {
        let temp = tempfile::tempdir().unwrap();
        let metadata = StateDirMetadata {
            used_by_bench: true,
            rollback_target: Some(42),
            ..StateDirMetadata::default()
        };

        let message = used_state_dir_message(temp.path(), &metadata, Some(block::Height(42)));

        assert!(message.contains("recorded rollback target 42"));
        assert!(message.contains("status --state-dir"));
    }

    #[test]
    fn rollback_success_marks_state_reusable() {
        let temp = tempfile::tempdir().unwrap();
        mark_used(
            temp.path(),
            RunMode::ApplyQueue,
            block::Height(50),
            block::Height(60),
        )
        .unwrap();

        mark_rollback_success(
            temp.path(),
            block::Height(40),
            (block::Height(60), block::Hash([1; 32])),
            (block::Height(40), block::Hash([2; 32])),
            std::time::Duration::from_millis(123),
        )
        .unwrap();

        let metadata = read_metadata(temp.path()).unwrap().unwrap();
        assert!(!metadata.used_by_bench);
        assert_eq!(metadata.rollback_target, Some(40));
        assert_eq!(metadata.rollback_old_tip, Some(60));
        assert_eq!(metadata.rollback_new_tip, Some(40));
        assert_eq!(metadata.rollback_duration_ms, Some(123));
    }
}
