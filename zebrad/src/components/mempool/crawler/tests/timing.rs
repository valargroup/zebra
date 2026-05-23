//! Timing tests for the mempool crawler.

use zebra_chain::parameters::POST_NU7_POW_TARGET_SPACING;

use crate::components::mempool::crawler::RATE_LIMIT_DELAY;

#[test]
fn ensure_timing_consistent() {
    assert!(
        RATE_LIMIT_DELAY.as_secs() < POST_NU7_POW_TARGET_SPACING.into(),
        "a mempool crawl should complete before most new blocks"
    );
}
