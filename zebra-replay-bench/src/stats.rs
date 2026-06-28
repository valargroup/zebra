//! Throughput and latency rollup for an `apply` run.

use std::time::Duration;

/// Accumulates per-block commit latencies and byte counts.
#[derive(Default)]
pub struct Stats {
    /// Per-block commit latency in microseconds.
    latencies_us: Vec<u64>,
    total_bytes: u64,
}

impl Stats {
    /// Records one committed block.
    pub fn record(&mut self, bytes: usize, latency: Duration) {
        self.latencies_us
            .push(u64::try_from(latency.as_micros()).unwrap_or(u64::MAX));
        self.total_bytes += bytes as u64;
    }

    /// Number of blocks recorded.
    pub fn count(&self) -> usize {
        self.latencies_us.len()
    }

    fn percentile_us(&mut self, p: f64) -> u64 {
        if self.latencies_us.is_empty() {
            return 0;
        }
        self.latencies_us.sort_unstable();
        // Nearest-rank percentile; clamp the index into bounds.
        let rank = ((p / 100.0) * self.latencies_us.len() as f64).ceil() as usize;
        let idx = rank.saturating_sub(1).min(self.latencies_us.len() - 1);
        self.latencies_us[idx]
    }

    /// Renders a human-readable multi-line report.
    pub fn report(&mut self, wall: Duration) -> String {
        let n = self.count();
        let secs = wall.as_secs_f64().max(f64::MIN_POSITIVE);
        let blk_s = n as f64 / secs;
        let mib = self.total_bytes as f64 / (1024.0 * 1024.0);
        let mib_s = mib / secs;

        let p50 = self.percentile_us(50.0) as f64 / 1000.0;
        let p90 = self.percentile_us(90.0) as f64 / 1000.0;
        let p99 = self.percentile_us(99.0) as f64 / 1000.0;
        let max = self.latencies_us.last().copied().unwrap_or(0) as f64 / 1000.0;

        format!(
            "blocks={n}  bytes={mib:.1} MiB  wall={secs:.2}s\n\
             throughput: {blk_s:.1} blk/s  {mib_s:.2} MiB/s\n\
             commit latency (ms): p50={p50:.2}  p90={p90:.2}  p99={p99:.2}  max={max:.2}"
        )
    }
}
