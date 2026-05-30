//! Timing tests for the mempool crawler.

use std::time::Duration;

use crate::components::mempool::crawler::{MIN_CRAWL_GAP, RATE_LIMIT_DELAY};

#[test]
fn ensure_timing_consistent() {
    // The rate-limit floor must be positive; otherwise tip-change storms
    // bypass rate-limiting entirely.
    assert!(
        MIN_CRAWL_GAP > Duration::ZERO,
        "MIN_CRAWL_GAP must be positive to rate-limit tip-change storms"
    );

    // The rate-limit floor must leave room for the periodic backstop to
    // fire; otherwise the backstop never wakes the crawl loop on quiet
    // chains.
    assert!(
        MIN_CRAWL_GAP < RATE_LIMIT_DELAY,
        "MIN_CRAWL_GAP must be smaller than RATE_LIMIT_DELAY so the backstop can fire"
    );
}
