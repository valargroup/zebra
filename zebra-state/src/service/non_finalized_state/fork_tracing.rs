//! JSONL tracing for non-finalized fork and orphan state transitions.

use std::{path::PathBuf, sync::Arc};

use chrono::{SecondsFormat, Utc};
use serde::{Serialize, Serializer};
use zebra_chain::{
    block::{self, Height},
    parameters::Network,
    work::difficulty::PartialCumulativeWork,
};
use zebra_jsonl_trace::{JsonlTraceSendError, JsonlTracer, JsonlWriteEvent};

use super::NonFinalizedState;

const TRACE_DIR_ENV: &str = "ZEBRA_TRACE_DIR";
const FORK_TRACE_ENABLE_ENV: &str = "ZEBRA_FORK_TRACE_ENABLE";
const SCHEMA_VERSION: &str = "zebra.fork.v1";
const FORK_EVENT_TABLE: &str = "fork_event";
const FORK_EVENT_FILE: &str = "fork_event.jsonl";
const FORK_SNAPSHOT_TABLE: &str = "fork_snapshot";
const FORK_SNAPSHOT_FILE: &str = "fork_snapshot.jsonl";

#[derive(Clone, Debug)]
pub(super) struct ForkTracer {
    network: Arc<str>,
    tracer: JsonlTracer,
}

#[derive(Clone, Debug)]
pub(super) struct ForkTraceSnapshot {
    pub chain_count: usize,
    pub best_tip: Option<ForkChainSnapshot>,
    pub chains: Vec<ForkChainSnapshot>,
}

#[derive(Clone, Debug)]
pub(super) struct ForkChainSnapshot {
    pub tip_hash: block::Hash,
    pub tip_height: Height,
    pub root_hash: block::Hash,
    pub root_height: Height,
    pub recent_fork_height: Option<Height>,
    pub recent_fork_length: Option<u32>,
    pub block_count: usize,
    pub chain_work: PartialCumulativeWork,
    pub is_best: bool,
}

#[derive(Copy, Clone, Debug)]
pub(super) enum ForkTraceCause {
    CommitBlock { committed_tip_hash: block::Hash },
    CommitNewChain { committed_tip_hash: block::Hash },
    Finalize { finalized_tip_hash: block::Hash },
    InvalidateBlock { invalidated_hash: block::Hash },
    ReconsiderBlock { reconsidered_hash: block::Hash },
}

#[derive(Serialize)]
struct ForkChainRecord {
    tip_hash: String,
    tip_height: u32,
    root_hash: String,
    root_height: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_length: Option<u32>,
    block_count: usize,
    #[serde(serialize_with = "serialize_chain_work")]
    chain_work: PartialCumulativeWork,
    is_best: bool,
}

#[derive(Serialize)]
struct ForkSnapshotRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    network: String,
    event: &'static str,
    trigger: &'static str,
    chain_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_height: Option<u32>,
    chains: Vec<ForkChainRecord>,
}

#[derive(Serialize)]
struct ForkCreatedRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    network: String,
    event: &'static str,
    trigger: &'static str,
    chain_count: usize,
    best_tip_hash: String,
    best_tip_height: u32,
    tip_hash: String,
    tip_height: u32,
    root_hash: String,
    root_height: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_length: Option<u32>,
    block_count: usize,
    #[serde(serialize_with = "serialize_chain_work")]
    chain_work: PartialCumulativeWork,
    is_best: bool,
}

#[derive(Serialize)]
struct ForkPrunedRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    network: String,
    event: &'static str,
    trigger: &'static str,
    reason: &'static str,
    chain_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_height: Option<u32>,
    tip_hash: String,
    tip_height: u32,
    root_hash: String,
    root_height: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_length: Option<u32>,
    orphaned_block_count: usize,
    #[serde(serialize_with = "serialize_chain_work")]
    chain_work: PartialCumulativeWork,
}

#[derive(Serialize)]
struct BestChainSwitchedRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    network: String,
    event: &'static str,
    trigger: &'static str,
    chain_count: usize,
    previous_best_tip_hash: String,
    previous_best_tip_height: u32,
    new_best_tip_hash: String,
    new_best_tip_height: u32,
}

#[derive(Serialize)]
struct ManualForkRecord {
    schema: &'static str,
    ts: String,
    node_id: &'static str,
    network: String,
    event: &'static str,
    trigger: &'static str,
    chain_count: usize,
    block_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    best_tip_height: Option<u32>,
}

impl ForkTraceSnapshot {
    pub fn from_state(state: &NonFinalizedState) -> Self {
        let chains: Vec<_> = state
            .chain_iter()
            .enumerate()
            .map(|(index, chain)| ForkChainSnapshot {
                tip_hash: chain.non_finalized_tip_hash(),
                tip_height: chain.non_finalized_tip_height(),
                root_hash: chain.non_finalized_root_hash(),
                root_height: chain.non_finalized_root_height(),
                recent_fork_height: chain.recent_fork_height(),
                recent_fork_length: chain.recent_fork_length(),
                block_count: chain.len(),
                chain_work: chain.partial_cumulative_work,
                is_best: index == 0,
            })
            .collect();

        let best_tip = chains.first().cloned();

        Self {
            chain_count: chains.len(),
            best_tip,
            chains,
        }
    }
}

impl ForkTracer {
    pub(super) fn from_env(network: &Network) -> Self {
        if !env_flag_enabled(FORK_TRACE_ENABLE_ENV) {
            return Self::noop(network);
        }

        let Some(trace_dir) = std::env::var_os(TRACE_DIR_ENV)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
        else {
            tracing::warn!(
                env = TRACE_DIR_ENV,
                "fork tracing enabled but trace directory is not configured"
            );
            return Self::noop(network);
        };

        Self::new(network, JsonlTracer::spawn(trace_dir))
    }

    pub(super) fn new(network: &Network, tracer: JsonlTracer) -> Self {
        Self {
            network: Arc::from(network.lowercase_name()),
            tracer,
        }
    }

    pub(super) fn noop(network: &Network) -> Self {
        Self::new(network, JsonlTracer::noop())
    }

    pub(super) fn trace_state_change(
        &self,
        state: &NonFinalizedState,
        before: &ForkTraceSnapshot,
        cause: ForkTraceCause,
    ) {
        if !self.tracer.is_enabled() {
            return;
        }

        let after = ForkTraceSnapshot::from_state(state);
        let timestamp = timestamp();

        self.trace_manual_event(&after, cause, &timestamp);
        self.trace_created_event(&after, before, cause, &timestamp);
        self.trace_pruned_events(state, &after, before, cause, &timestamp);
        self.trace_best_chain_switched_event(state, &after, before, cause, &timestamp);
        self.trace_snapshot(&after, cause, &timestamp);
    }

    fn trace_manual_event(
        &self,
        after: &ForkTraceSnapshot,
        cause: ForkTraceCause,
        timestamp: &str,
    ) {
        let Some((event, block_hash)) = cause.manual_event() else {
            return;
        };

        let record = ManualForkRecord {
            schema: SCHEMA_VERSION,
            ts: timestamp.to_owned(),
            node_id: zebra_jsonl_trace::node_id(),
            network: self.network.to_string(),
            event,
            trigger: cause.trigger(),
            chain_count: after.chain_count,
            block_hash: block_hash.to_string(),
            best_tip_hash: after
                .best_tip
                .as_ref()
                .map(|best| best.tip_hash.to_string()),
            best_tip_height: after.best_tip.as_ref().map(|best| best.tip_height.0),
        };

        self.emit_event(&record);
    }

    fn trace_created_event(
        &self,
        after: &ForkTraceSnapshot,
        before: &ForkTraceSnapshot,
        cause: ForkTraceCause,
        timestamp: &str,
    ) {
        let Some(committed_tip_hash) = cause.created_tip_hash() else {
            return;
        };

        if after.chain_count <= before.chain_count {
            return;
        }

        let Some(best_tip) = after.best_tip.as_ref() else {
            return;
        };

        let Some(created_chain) = after
            .chains
            .iter()
            .find(|chain| chain.tip_hash == committed_tip_hash)
        else {
            return;
        };

        let record = ForkCreatedRecord {
            schema: SCHEMA_VERSION,
            ts: timestamp.to_owned(),
            node_id: zebra_jsonl_trace::node_id(),
            network: self.network.to_string(),
            event: "fork_created",
            trigger: cause.trigger(),
            chain_count: after.chain_count,
            best_tip_hash: best_tip.tip_hash.to_string(),
            best_tip_height: best_tip.tip_height.0,
            tip_hash: created_chain.tip_hash.to_string(),
            tip_height: created_chain.tip_height.0,
            root_hash: created_chain.root_hash.to_string(),
            root_height: created_chain.root_height.0,
            fork_height: created_chain.recent_fork_height.map(|height| height.0),
            fork_length: created_chain.recent_fork_length,
            block_count: created_chain.block_count,
            chain_work: created_chain.chain_work,
            is_best: created_chain.is_best,
        };

        self.emit_event(&record);
    }

    fn trace_pruned_events(
        &self,
        state: &NonFinalizedState,
        after: &ForkTraceSnapshot,
        before: &ForkTraceSnapshot,
        cause: ForkTraceCause,
        timestamp: &str,
    ) {
        for removed_chain in before.chains.iter().filter(|before_chain| {
            !state
                .chain_iter()
                .any(|after_chain| after_chain.contains_block_hash(before_chain.tip_hash))
        }) {
            if cause.is_finalize()
                && before
                    .best_tip
                    .as_ref()
                    .is_some_and(|best| best.tip_hash == removed_chain.tip_hash)
            {
                continue;
            }

            let record = ForkPrunedRecord {
                schema: SCHEMA_VERSION,
                ts: timestamp.to_owned(),
                node_id: zebra_jsonl_trace::node_id(),
                network: self.network.to_string(),
                event: "fork_pruned",
                trigger: cause.trigger(),
                reason: cause.prune_reason(),
                chain_count: after.chain_count,
                best_tip_hash: after
                    .best_tip
                    .as_ref()
                    .map(|best| best.tip_hash.to_string()),
                best_tip_height: after.best_tip.as_ref().map(|best| best.tip_height.0),
                tip_hash: removed_chain.tip_hash.to_string(),
                tip_height: removed_chain.tip_height.0,
                root_hash: removed_chain.root_hash.to_string(),
                root_height: removed_chain.root_height.0,
                fork_height: removed_chain.recent_fork_height.map(|height| height.0),
                fork_length: removed_chain.recent_fork_length,
                orphaned_block_count: removed_chain.block_count,
                chain_work: removed_chain.chain_work,
            };

            self.emit_event(&record);
        }
    }

    fn trace_best_chain_switched_event(
        &self,
        state: &NonFinalizedState,
        after: &ForkTraceSnapshot,
        before: &ForkTraceSnapshot,
        cause: ForkTraceCause,
        timestamp: &str,
    ) {
        let Some(previous_best) = before.best_tip.as_ref() else {
            return;
        };

        let Some(new_best) = after.best_tip.as_ref() else {
            return;
        };

        if previous_best.tip_hash == new_best.tip_hash {
            return;
        }

        let Some(best_chain) = state.best_chain() else {
            return;
        };

        if best_chain.contains_block_hash(previous_best.tip_hash) {
            return;
        }

        let record = BestChainSwitchedRecord {
            schema: SCHEMA_VERSION,
            ts: timestamp.to_owned(),
            node_id: zebra_jsonl_trace::node_id(),
            network: self.network.to_string(),
            event: "best_chain_switched",
            trigger: cause.trigger(),
            chain_count: after.chain_count,
            previous_best_tip_hash: previous_best.tip_hash.to_string(),
            previous_best_tip_height: previous_best.tip_height.0,
            new_best_tip_hash: new_best.tip_hash.to_string(),
            new_best_tip_height: new_best.tip_height.0,
        };

        self.emit_event(&record);
    }

    fn trace_snapshot(&self, snapshot: &ForkTraceSnapshot, cause: ForkTraceCause, timestamp: &str) {
        let record = ForkSnapshotRecord {
            schema: SCHEMA_VERSION,
            ts: timestamp.to_owned(),
            node_id: zebra_jsonl_trace::node_id(),
            network: self.network.to_string(),
            event: "fork_snapshot",
            trigger: cause.trigger(),
            chain_count: snapshot.chain_count,
            best_tip_hash: snapshot
                .best_tip
                .as_ref()
                .map(|best| best.tip_hash.to_string()),
            best_tip_height: snapshot.best_tip.as_ref().map(|best| best.tip_height.0),
            chains: snapshot
                .chains
                .iter()
                .map(|chain| ForkChainRecord {
                    tip_hash: chain.tip_hash.to_string(),
                    tip_height: chain.tip_height.0,
                    root_hash: chain.root_hash.to_string(),
                    root_height: chain.root_height.0,
                    fork_height: chain.recent_fork_height.map(|height| height.0),
                    fork_length: chain.recent_fork_length,
                    block_count: chain.block_count,
                    chain_work: chain.chain_work,
                    is_best: chain.is_best,
                })
                .collect(),
        };

        self.emit_snapshot(&record);
    }

    fn emit_event<T: Serialize>(&self, record: &T) {
        self.emit_json(FORK_EVENT_TABLE, FORK_EVENT_FILE, record);
    }

    fn emit_snapshot<T: Serialize>(&self, record: &T) {
        self.emit_json(FORK_SNAPSHOT_TABLE, FORK_SNAPSHOT_FILE, record);
    }

    fn emit_json<T: Serialize>(&self, table: &'static str, file_name: &'static str, record: &T) {
        let Ok(line) = serde_json::to_vec(record) else {
            tracing::warn!(table, "failed to serialize fork trace record");
            return;
        };

        let event = JsonlWriteEvent {
            table,
            file_name,
            line,
        };

        match self.tracer.try_send(event) {
            Ok(())
            | Err(JsonlTraceSendError::Disabled(_))
            | Err(JsonlTraceSendError::Closed(_)) => {}
            Err(JsonlTraceSendError::Full(_)) => {}
        }
    }
}

impl ForkTraceCause {
    fn trigger(self) -> &'static str {
        match self {
            Self::CommitBlock { .. } => "commit_block",
            Self::CommitNewChain { .. } => "commit_new_chain",
            Self::Finalize { .. } => "finalize",
            Self::InvalidateBlock { .. } => "invalidate_block",
            Self::ReconsiderBlock { .. } => "reconsider_block",
        }
    }

    fn created_tip_hash(self) -> Option<block::Hash> {
        match self {
            Self::CommitBlock { committed_tip_hash }
            | Self::CommitNewChain { committed_tip_hash } => Some(committed_tip_hash),
            Self::Finalize { .. } | Self::InvalidateBlock { .. } | Self::ReconsiderBlock { .. } => {
                None
            }
        }
    }

    fn prune_reason(self) -> &'static str {
        match self {
            Self::CommitBlock { .. } | Self::CommitNewChain { .. } => "max_chain_limit",
            Self::Finalize { .. } => "finalized_root_mismatch",
            Self::InvalidateBlock { .. } => "block_invalidated",
            Self::ReconsiderBlock { .. } => "reconsidered_branch_replaced",
        }
    }

    fn manual_event(self) -> Option<(&'static str, block::Hash)> {
        match self {
            Self::InvalidateBlock { invalidated_hash } => {
                Some(("block_invalidated", invalidated_hash))
            }
            Self::ReconsiderBlock { reconsidered_hash } => {
                Some(("block_reconsidered", reconsidered_hash))
            }
            Self::CommitBlock { .. } | Self::CommitNewChain { .. } | Self::Finalize { .. } => None,
        }
    }

    fn is_finalize(self) -> bool {
        matches!(self, Self::Finalize { .. })
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn serialize_chain_work<S>(
    chain_work: &PartialCumulativeWork,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:064x}", chain_work.as_u128()))
}
