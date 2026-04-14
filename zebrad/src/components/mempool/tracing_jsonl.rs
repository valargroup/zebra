//! JSONL tracing for mempool lifecycle and chain churn during experiments.

use std::{path::PathBuf, sync::Arc};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_chain::{block, transaction::UnminedTxId};
use zebra_jsonl_trace::{JsonlTraceSendError, JsonlTracer, JsonlWriteEvent};
use zebra_state::TipAction;

const TRACE_DIR_ENV: &str = "ZEBRA_P2P_TRACE_DIR";
const TRACE_DIR_FALLBACK_ENV: &str = "ZEBRA_TRACE_DIR";
const TRACE_SCHEMA: &str = "zebra.mempool.v1";
const MEMPOOL_TX_LIFECYCLE_TABLE: &str = "mempool_tx_lifecycle";
const MEMPOOL_TX_LIFECYCLE_FILE: &str = "mempool_tx_lifecycle.jsonl";
const CHAIN_CHURN_TABLE: &str = "chain_churn";
const CHAIN_CHURN_FILE: &str = "chain_churn.jsonl";

#[derive(Clone, Debug)]
pub(crate) struct MempoolTracer {
    tracer: JsonlTracer,
    component: Arc<str>,
}

#[derive(Serialize)]
struct MempoolTxLifecycleRecord {
    schema: &'static str,
    ts: String,
    component: String,
    txid: String,
    event: &'static str,
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tip_height: Option<u32>,
    mempool_transactions: usize,
    mempool_bytes: usize,
}

#[derive(Serialize)]
struct ChainChurnRecord {
    schema: &'static str,
    ts: String,
    component: String,
    event: &'static str,
    old_tip_hash: String,
    old_tip_height: u32,
    new_tip_hash: String,
    new_tip_height: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reorg_depth: Option<u32>,
    mempool_transactions: usize,
    mempool_bytes: usize,
    retry_transactions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<&'static str>,
}

impl MempoolTracer {
    pub(crate) fn from_env() -> Self {
        let trace_dir = std::env::var_os(TRACE_DIR_ENV)
            .or_else(|| std::env::var_os(TRACE_DIR_FALLBACK_ENV))
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);
        let tracer = trace_dir.map_or_else(JsonlTracer::noop, JsonlTracer::spawn);
        Self {
            tracer,
            component: Arc::from("mempool"),
        }
    }

    pub(crate) fn trace_tx(
        &self,
        txid: UnminedTxId,
        event: &'static str,
        source: &'static str,
        reason_class: Option<&'static str>,
        reason_detail: Option<String>,
        transaction_bytes: Option<usize>,
        tip_height: Option<block::Height>,
        mempool_transactions: usize,
        mempool_bytes: usize,
    ) {
        if !self.tracer.is_enabled() {
            return;
        }

        let record = MempoolTxLifecycleRecord {
            schema: TRACE_SCHEMA,
            ts: timestamp(),
            component: self.component.to_string(),
            txid: txid.to_string(),
            event,
            source,
            reason_class,
            reason_detail,
            transaction_bytes,
            tip_height: tip_height.map(|height| height.0),
            mempool_transactions,
            mempool_bytes,
        };
        self.emit(
            MEMPOOL_TX_LIFECYCLE_TABLE,
            MEMPOOL_TX_LIFECYCLE_FILE,
            &record,
        );
    }

    pub(crate) fn trace_churn(
        &self,
        event: &'static str,
        old_tip_hash: block::Hash,
        old_tip_height: block::Height,
        action: &TipAction,
        mempool_transactions: usize,
        mempool_bytes: usize,
        retry_transactions: usize,
        note: Option<&'static str>,
    ) {
        if !self.tracer.is_enabled() {
            return;
        }

        let new_tip_hash = action.best_tip_hash();
        let new_tip_height = action.best_tip_height();
        let reorg_depth = match action {
            TipAction::Reset { height, .. } if *height <= old_tip_height => {
                Some(old_tip_height.0.saturating_sub(height.0).saturating_add(1))
            }
            _ => None,
        };

        let record = ChainChurnRecord {
            schema: TRACE_SCHEMA,
            ts: timestamp(),
            component: self.component.to_string(),
            event,
            old_tip_hash: old_tip_hash.to_string(),
            old_tip_height: old_tip_height.0,
            new_tip_hash: new_tip_hash.to_string(),
            new_tip_height: new_tip_height.0,
            reorg_depth,
            mempool_transactions,
            mempool_bytes,
            retry_transactions,
            note,
        };
        self.emit(CHAIN_CHURN_TABLE, CHAIN_CHURN_FILE, &record);
    }

    fn emit<T: Serialize>(&self, table: &'static str, file_name: &'static str, record: &T) {
        let Ok(line) = serde_json::to_vec(record) else {
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

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
