//! Throughput + latency accounting for the commit benchmark.

use std::time::Duration;

/// Accumulates per-block commit/read latencies and totals.
#[derive(Default)]
pub struct Stats {
    pub committed_blocks: u64,
    pub committed_bytes: u64,
    pub errored_blocks: u64,
    commit_latencies_us: Vec<u64>,
    read_latencies_us: Vec<u64>,
}

impl Stats {
    pub fn record_commit(
        &mut self,
        bytes: u64,
        commit_latency: Duration,
        read_latency: Option<Duration>,
    ) {
        self.committed_blocks += 1;
        self.committed_bytes = self.committed_bytes.saturating_add(bytes);
        self.commit_latencies_us
            .push(u64::try_from(commit_latency.as_micros()).unwrap_or(u64::MAX));
        if let Some(read) = read_latency {
            self.read_latencies_us
                .push(u64::try_from(read.as_micros()).unwrap_or(u64::MAX));
        }
    }

    pub fn record_error(&mut self) {
        self.errored_blocks += 1;
    }

    pub fn summary(&self, elapsed: Duration) -> Summary {
        Summary {
            elapsed,
            committed_blocks: self.committed_blocks,
            committed_bytes: self.committed_bytes,
            errored_blocks: self.errored_blocks,
            commit_us_p50: percentile(&self.commit_latencies_us, 50),
            commit_us_p95: percentile(&self.commit_latencies_us, 95),
            commit_us_p99: percentile(&self.commit_latencies_us, 99),
            read_us_p50: percentile(&self.read_latencies_us, 50),
            read_us_p95: percentile(&self.read_latencies_us, 95),
            reads: self.read_latencies_us.len() as u64,
        }
    }
}

#[derive(Debug)]
pub struct Summary {
    pub elapsed: Duration,
    pub committed_blocks: u64,
    pub committed_bytes: u64,
    pub errored_blocks: u64,
    pub commit_us_p50: u64,
    pub commit_us_p95: u64,
    pub commit_us_p99: u64,
    pub read_us_p50: u64,
    pub read_us_p95: u64,
    pub reads: u64,
}

impl Summary {
    pub fn print(&self) {
        let secs = self.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
        let blk_s = self.committed_blocks as f64 / secs;
        let mib_s = (self.committed_bytes as f64 / (1024.0 * 1024.0)) / secs;
        println!("\n=== zakura-commit-bench summary ===");
        println!("elapsed:            {:.3}s", secs);
        println!("committed blocks:   {}", self.committed_blocks);
        println!(
            "committed bytes:    {} ({:.1} MiB)",
            self.committed_bytes,
            self.committed_bytes as f64 / (1024.0 * 1024.0)
        );
        println!("errored blocks:     {}", self.errored_blocks);
        println!(
            "throughput:         {:.1} blk/s   {:.2} MiB/s",
            blk_s, mib_s
        );
        println!(
            "commit latency:     p50 {:.2}ms  p95 {:.2}ms  p99 {:.2}ms",
            self.commit_us_p50 as f64 / 1000.0,
            self.commit_us_p95 as f64 / 1000.0,
            self.commit_us_p99 as f64 / 1000.0,
        );
        if self.reads > 0 {
            println!(
                "frontier reads:     {} reads   p50 {:.2}ms  p95 {:.2}ms",
                self.reads,
                self.read_us_p50 as f64 / 1000.0,
                self.read_us_p95 as f64 / 1000.0,
            );
        } else {
            println!("frontier reads:     none (per-block read disabled)");
        }
    }
}

/// Nearest-rank percentile over an unsorted sample (clones + sorts; bench-only).
fn percentile(samples: &[u64], pct: u8) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    // nearest-rank: index = ceil(pct/100 * n) - 1
    let rank = ((pct as usize) * sorted.len()).div_ceil(100).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)]
}
