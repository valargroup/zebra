//! JSONL tracing for `getblocktemplate` responses and transaction selection.

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use zebra_chain::{block, parameters::Network, transaction::VerifiedUnminedTx};
use zebra_jsonl_trace::{JsonlTraceSendError, JsonlTracer, JsonlWriteEvent};
use zebra_node_services::mempool::TransactionDependencies;

const TRACE_DIR_ENV: &str = "ZEBRA_P2P_TRACE_DIR";
const TRACE_DIR_FALLBACK_ENV: &str = "ZEBRA_TRACE_DIR";
const TRACE_SCHEMA: &str = "zebra.template.v1";
const TEMPLATE_EVENT_TABLE: &str = "template_event";
const TEMPLATE_EVENT_FILE: &str = "template_event.jsonl";
const TEMPLATE_DIFF_TABLE: &str = "template_diff";
const TEMPLATE_DIFF_FILE: &str = "template_diff.jsonl";
const TEMPLATE_TX_DECISION_TABLE: &str = "template_tx_decision";
const TEMPLATE_TX_DECISION_FILE: &str = "template_tx_decision.jsonl";
const MAX_DECISION_RECORDS_PER_TEMPLATE: usize = 256;

#[derive(Clone, Debug)]
pub(crate) struct TemplateTracer {
    network: Arc<str>,
    tracer: JsonlTracer,
    previous: Arc<Mutex<Option<PreviousTemplate>>>,
}

#[derive(Clone, Debug)]
struct PreviousTemplate {
    long_poll_id: String,
    tip_height: u32,
    tip_hash: block::Hash,
    tx_ids: HashSet<String>,
    tx_count: usize,
    tx_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct TemplateSelectionTrace {
    pub tip_height: u32,
    pub tip_hash: block::Hash,
    pub template_prev_hash: block::Hash,
    pub client_long_poll_id: Option<String>,
    pub server_long_poll_id: String,
    pub submit_old: Option<bool>,
    pub mempool_transactions: Vec<VerifiedUnminedTx>,
    pub selected_transactions: Vec<VerifiedUnminedTx>,
    pub transaction_dependencies: TransactionDependencies,
    pub long_poll_wait_ms: u64,
    pub selection_ms: u64,
}

#[derive(Serialize)]
struct TemplateEventRecord {
    schema: &'static str,
    ts: String,
    network: String,
    event: &'static str,
    tip_height: u32,
    tip_hash: String,
    template_prev_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_long_poll_id: Option<String>,
    server_long_poll_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    submit_old: Option<bool>,
    mempool_count: usize,
    mempool_bytes: usize,
    selected_count: usize,
    selected_bytes: usize,
    dependent_count: usize,
    selected_dependent_count: usize,
    conventional_fee_count: usize,
    low_fee_count: usize,
    long_poll_wait_ms: u64,
    selection_ms: u64,
}

#[derive(Serialize)]
struct TemplateDiffRecord {
    schema: &'static str,
    ts: String,
    network: String,
    event: &'static str,
    previous_tip_height: u32,
    previous_tip_hash: String,
    new_tip_height: u32,
    new_tip_hash: String,
    old_long_poll_id: String,
    new_long_poll_id: String,
    reason_class: &'static str,
    old_tx_count: usize,
    new_tx_count: usize,
    old_tx_bytes: usize,
    new_tx_bytes: usize,
    added_count: usize,
    removed_count: usize,
}

#[derive(Serialize)]
struct TemplateTxDecisionRecord {
    schema: &'static str,
    ts: String,
    network: String,
    tip_height: u32,
    tip_hash: String,
    long_poll_id: String,
    txid: String,
    decision: &'static str,
    reason_class: &'static str,
    transaction_bytes: usize,
    fee_weight_ratio: f32,
    pays_conventional_fee: bool,
    has_dependencies: bool,
}

impl TemplateTracer {
    pub(crate) fn from_env(network: &Network) -> Self {
        let trace_dir = std::env::var_os(TRACE_DIR_ENV)
            .or_else(|| std::env::var_os(TRACE_DIR_FALLBACK_ENV))
            .filter(|path| !path.is_empty())
            .map(PathBuf::from);

        let tracer = trace_dir.map_or_else(JsonlTracer::noop, JsonlTracer::spawn);
        Self::new(network, tracer)
    }

    pub(crate) fn new(network: &Network, tracer: JsonlTracer) -> Self {
        Self {
            network: Arc::from(network.lowercase_name()),
            tracer,
            previous: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn trace_template(&self, trace: TemplateSelectionTrace) {
        if !self.tracer.is_enabled() {
            return;
        }

        let ts = timestamp();
        let mut selected_ids = HashSet::with_capacity(trace.selected_transactions.len());
        let mut selected_bytes = 0usize;
        let mut selected_dependent_count = 0usize;
        for tx in &trace.selected_transactions {
            selected_ids.insert(tx.transaction.id.mined_id().to_string());
            selected_bytes += tx.transaction.size;
            if trace
                .transaction_dependencies
                .dependencies()
                .contains_key(&tx.transaction.id.mined_id())
            {
                selected_dependent_count += 1;
            }
        }

        let mut mempool_bytes = 0usize;
        let mut dependent_count = 0usize;
        let mut conventional_fee_count = 0usize;
        let mut low_fee_count = 0usize;
        for tx in &trace.mempool_transactions {
            mempool_bytes += tx.transaction.size;
            if trace
                .transaction_dependencies
                .dependencies()
                .contains_key(&tx.transaction.id.mined_id())
            {
                dependent_count += 1;
            }
            if tx.pays_conventional_fee() {
                conventional_fee_count += 1;
            } else {
                low_fee_count += 1;
            }
        }

        let event = TemplateEventRecord {
            schema: TRACE_SCHEMA,
            ts: ts.clone(),
            network: self.network.to_string(),
            event: "template_built",
            tip_height: trace.tip_height,
            tip_hash: trace.tip_hash.to_string(),
            template_prev_hash: trace.template_prev_hash.to_string(),
            client_long_poll_id: trace.client_long_poll_id.clone(),
            server_long_poll_id: trace.server_long_poll_id.clone(),
            submit_old: trace.submit_old,
            mempool_count: trace.mempool_transactions.len(),
            mempool_bytes,
            selected_count: trace.selected_transactions.len(),
            selected_bytes,
            dependent_count,
            selected_dependent_count,
            conventional_fee_count,
            low_fee_count,
            long_poll_wait_ms: trace.long_poll_wait_ms,
            selection_ms: trace.selection_ms,
        };
        self.emit(TEMPLATE_EVENT_TABLE, TEMPLATE_EVENT_FILE, &event);

        self.trace_diff(
            &ts,
            trace.tip_height,
            trace.tip_hash,
            &trace.server_long_poll_id,
            &selected_ids,
            trace.selected_transactions.len(),
            selected_bytes,
        );
        self.trace_decisions(&ts, &trace, &selected_ids);
    }

    fn trace_diff(
        &self,
        ts: &str,
        tip_height: u32,
        tip_hash: block::Hash,
        long_poll_id: &str,
        selected_ids: &HashSet<String>,
        selected_count: usize,
        selected_bytes: usize,
    ) {
        let Ok(mut previous) = self.previous.lock() else {
            return;
        };
        if let Some(prev) = previous.as_ref() {
            let added_count = selected_ids.difference(&prev.tx_ids).count();
            let removed_count = prev.tx_ids.difference(selected_ids).count();
            let reason_class =
                classify_diff_reason(prev, tip_hash, long_poll_id, added_count, removed_count);
            if prev.long_poll_id != long_poll_id || added_count > 0 || removed_count > 0 {
                let record = TemplateDiffRecord {
                    schema: TRACE_SCHEMA,
                    ts: ts.to_owned(),
                    network: self.network.to_string(),
                    event: "template_changed",
                    previous_tip_height: prev.tip_height,
                    previous_tip_hash: prev.tip_hash.to_string(),
                    new_tip_height: tip_height,
                    new_tip_hash: tip_hash.to_string(),
                    old_long_poll_id: prev.long_poll_id.clone(),
                    new_long_poll_id: long_poll_id.to_owned(),
                    reason_class,
                    old_tx_count: prev.tx_count,
                    new_tx_count: selected_count,
                    old_tx_bytes: prev.tx_bytes,
                    new_tx_bytes: selected_bytes,
                    added_count,
                    removed_count,
                };
                self.emit(TEMPLATE_DIFF_TABLE, TEMPLATE_DIFF_FILE, &record);
            }
        }

        *previous = Some(PreviousTemplate {
            long_poll_id: long_poll_id.to_owned(),
            tip_height,
            tip_hash,
            tx_ids: selected_ids.clone(),
            tx_count: selected_count,
            tx_bytes: selected_bytes,
        });
    }

    fn trace_decisions(
        &self,
        ts: &str,
        trace: &TemplateSelectionTrace,
        selected_ids: &HashSet<String>,
    ) {
        let tx_dependencies = trace.transaction_dependencies.dependencies();
        for tx in trace
            .mempool_transactions
            .iter()
            .take(MAX_DECISION_RECORDS_PER_TEMPLATE)
        {
            let txid = tx.transaction.id.mined_id();
            let txid_string = txid.to_string();
            let included = selected_ids.contains(&txid_string);
            let has_dependencies = tx_dependencies.contains_key(&txid);
            let reason_class = if included {
                "included"
            } else if has_dependencies {
                "dependency_missing"
            } else if tx.pays_conventional_fee() {
                "weighted_out_or_limited"
            } else {
                "low_fee_not_selected"
            };

            let record = TemplateTxDecisionRecord {
                schema: TRACE_SCHEMA,
                ts: ts.to_owned(),
                network: self.network.to_string(),
                tip_height: trace.tip_height,
                tip_hash: trace.tip_hash.to_string(),
                long_poll_id: trace.server_long_poll_id.clone(),
                txid: txid_string,
                decision: if included { "included" } else { "excluded" },
                reason_class,
                transaction_bytes: tx.transaction.size,
                fee_weight_ratio: tx.fee_weight_ratio,
                pays_conventional_fee: tx.pays_conventional_fee(),
                has_dependencies,
            };
            self.emit(
                TEMPLATE_TX_DECISION_TABLE,
                TEMPLATE_TX_DECISION_FILE,
                &record,
            );
        }
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

fn classify_diff_reason(
    previous: &PreviousTemplate,
    new_tip_hash: block::Hash,
    new_long_poll_id: &str,
    added_count: usize,
    removed_count: usize,
) -> &'static str {
    let tip_changed = previous.tip_hash != new_tip_hash;
    let mempool_changed =
        previous.long_poll_id != new_long_poll_id || added_count > 0 || removed_count > 0;
    match (tip_changed, mempool_changed) {
        (true, true) => "both",
        (true, false) => "tip_changed",
        (false, true) => "mempool_changed",
        (false, false) => "unknown",
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
