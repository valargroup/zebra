//! Named block-sync fuzzer scenarios + invariant assertions.
//!
//! Each test drives the real reactor through a distinct adversarial shape and asserts
//! the core invariants (no stall, contiguous/correct commit, bounded in-flight). They
//! emit the standard JSONL; run with `ZAKURA_TEST_TRACE=keep` and point the analysis
//! scripts at `target/zakura-traces/<name>/node-00` to inspect a run.

use std::time::Duration;

use zebra_chain::block;

use super::{
    assert_core_invariants, fuzz_config, invariant_report, run_scenario, run_trace, FuzzOutcome,
    IdleGap, InvariantReport, LatencyDist, PeerSpec, Scenario, ServeProfile, TipEvent,
    TipEventKind,
};
use crate::zakura::ZakuraBlockSyncConfig;

/// Run a scenario, flush its trace, assert the core invariants, and return the outcome
/// + report. `outstanding_slack` absorbs brief over-counts at request boundaries.
async fn run_checked(
    name: &str,
    scenario: Scenario,
    outstanding_slack: u64,
) -> (FuzzOutcome, InvariantReport) {
    let (mut capture, trace) = run_trace(name).expect("trace capture opens");
    let outcome = run_scenario(&scenario, trace)
        .await
        .expect("scenario runs without harness error");
    capture.flush().await;
    let reader = capture
        .reader()
        .expect("trace reader loads the flushed run");
    let report = invariant_report(&reader);
    tracing::info!(
        scenario = name,
        committed = outcome.committed_tip.0,
        target = outcome.target.0,
        state_samples = report.state_samples,
        max_outstanding = report.max_outstanding,
        peak_budget_reserved = report.peak_budget_reserved,
        final_budget_reserved = report.final_budget_reserved,
        protocol_rejects = report.protocol_rejects,
        floor_bypass_requests = report.floor_bypass_requests,
        "blocksync fuzz scenario complete",
    );
    assert_core_invariants(&scenario, &outcome, &report, outstanding_slack);
    capture.finish().await.expect("capture discards cleanly");
    (outcome, report)
}

/// A config with a short request timeout, for scenarios that rely on re-requesting
/// around a slow/withholding/dropping peer within the deadline.
fn retry_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        request_timeout: Duration::from_secs(2),
        ..fuzz_config()
    }
}

fn target(blocks: u32) -> block::Height {
    block::Height(blocks)
}

/// Steady state: several fast, full-range peers. Baseline throughput + invariants.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_steady() {
    let blocks = 300;
    let scenario = Scenario::new(
        blocks,
        0x57ea_0001,
        fuzz_config(),
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
            PeerSpec::fast(3, target(blocks)),
        ],
    );
    run_checked("fuzz_steady", scenario, 32).await;
}

/// Steady state under the experimental byte cwnd unit: the controller budgets in-flight
/// work by reserved body bytes instead of request count. End-to-end seam check — the
/// byte-denominated `available_slots` gate must still drive the real reactor to the tip
/// without stalling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_steady_bytes_unit() {
    let blocks = 300;
    let config = ZakuraBlockSyncConfig {
        bbr_cwnd_unit: crate::zakura::CwndUnit::Bytes,
        ..fuzz_config()
    };
    let scenario = Scenario::new(
        blocks,
        0x57ea_0008,
        config,
        vec![
            PeerSpec::fast(1, target(blocks)),
            PeerSpec::fast(2, target(blocks)),
            PeerSpec::fast(3, target(blocks)),
        ],
    );
    run_checked("fuzz_steady_bytes_unit", scenario, 32).await;
}

/// Head-of-line: one slow, high-latency peer alongside fast peers. The trace-proven
/// regime the BBR work targets. For now we only require convergence; once BBR lands we
/// compare the per-peer queue depth / HoL latency in the report across controllers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_one_slow_peer_hol() {
    let blocks = 300;
    let slow = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile::slow(Duration::from_millis(20), Duration::from_millis(5)),
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0002,
        fuzz_config(),
        vec![
            slow,
            PeerSpec::fast(2, target(blocks)),
            PeerSpec::fast(3, target(blocks)),
        ],
    );
    scenario.deadline = Duration::from_secs(60);
    run_checked("fuzz_one_slow_peer_hol", scenario, 32).await;
}

/// Reorg: a mid-sync verified-tip reset, then sync resumes to the target. Stretched
/// with a per-block serve latency so the reset lands while download is in flight.
///
/// Ignored in Phase 1: a faithful *mid-sync* `VerifiedReset` needs the real
/// `Committer`'s epoch / lowest-reset-wins rollback semantics (re-verifying every
/// height above the reset). `MockApplyFrontier` only mirrors part of that, so the
/// re-sync stalls. The header-reanchor "large → small" path through the same
/// `handle_chain_tip_reset` IS covered by `fuzz_large_to_small`. This scenario is the
/// validation target for the high-fidelity `Committer<MockVerifier>` tier.
#[ignore = "needs high-fidelity Committer<MockVerifier> for mid-sync reorg epoch/reset semantics"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_reorg() {
    let blocks = 600;
    let peer = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile::slow(Duration::from_millis(0), Duration::from_millis(2)),
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0003,
        retry_config(),
        vec![peer, PeerSpec::fast(2, target(blocks))],
    );
    // Reset to a low height the node has already committed past by 300 ms (per-block
    // 2 ms ⇒ ~150 committed), so it is a true rollback, then it re-syncs to the tip.
    scenario.timeline = vec![TipEvent {
        at: Duration::from_millis(300),
        kind: TipEventKind::VerifiedReset(block::Height(50)),
    }];
    scenario.deadline = Duration::from_secs(30);
    run_checked("fuzz_reorg", scenario, 32).await;
}

/// Idle/withholding: one peer is missing a height window (answers `RangeUnavailable`);
/// a covering peer serves it. The node must route around the gap. Deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_idle_peers() {
    let blocks = 300;
    let withholder = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            withhold: Some((block::Height(150), block::Height(180))),
            ..ServeProfile::fast()
        },
    );
    let scenario = Scenario::new(
        blocks,
        0x57ea_0004,
        retry_config(),
        vec![withholder, PeerSpec::fast(2, target(blocks))],
    );
    run_checked("fuzz_idle_peers", scenario, 32).await;
}

/// Churn storm: a stable peer plus several peers connecting and disconnecting on a
/// staggered schedule. Progress must continue across the churn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_churn_storm() {
    let blocks = 300;
    let stable = PeerSpec::fast(1, target(blocks));
    let mut churn_a = PeerSpec::fast(2, target(blocks));
    churn_a.disconnect_at = Some(Duration::from_millis(100));
    let mut churn_b = PeerSpec::fast(3, target(blocks));
    churn_b.connect_at = Duration::from_millis(50);
    churn_b.disconnect_at = Some(Duration::from_millis(150));
    let mut churn_c = PeerSpec::fast(4, target(blocks));
    churn_c.connect_at = Duration::from_millis(100);
    churn_c.disconnect_at = Some(Duration::from_millis(200));

    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0005,
        fuzz_config(),
        vec![stable, churn_a, churn_b, churn_c],
    );
    scenario.deadline = Duration::from_secs(60);
    run_checked("fuzz_churn_storm", scenario, 32).await;
}

/// Large → small: the header target grows in steps, reanchors down below the current
/// verified tip, then grows again to the full chain. Exercises header advance/reanchor
/// handling and uniform serve jitter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_large_to_small() {
    let blocks = 1000;
    let jittery = PeerSpec::with_serve(
        1,
        target(blocks),
        ServeProfile {
            per_block_latency: LatencyDist::Uniform {
                low: Duration::ZERO,
                high: Duration::from_millis(1),
            },
            idle_gap: Some(IdleGap {
                every_responses: 25,
                duration: Duration::from_millis(5),
            }),
            ..ServeProfile::fast()
        },
    );
    let mut scenario = Scenario::new(
        blocks,
        0x57ea_0006,
        fuzz_config(),
        vec![jittery, PeerSpec::fast(2, target(blocks))],
    );
    scenario.initial_best_header = block::Height(100);
    scenario.timeline = vec![
        TipEvent {
            at: Duration::from_millis(60),
            kind: TipEventKind::GrowTo(block::Height(300)),
        },
        TipEvent {
            at: Duration::from_millis(140),
            kind: TipEventKind::GrowTo(block::Height(700)),
        },
        TipEvent {
            at: Duration::from_millis(200),
            kind: TipEventKind::HeaderReanchor(block::Height(500)),
        },
        TipEvent {
            at: Duration::from_millis(280),
            kind: TipEventKind::GrowTo(block::Height(1000)),
        },
    ];
    scenario.deadline = Duration::from_secs(60);
    run_checked("fuzz_large_to_small", scenario, 32).await;
}
