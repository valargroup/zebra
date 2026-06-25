//! A minimal in-process `metrics` recorder that captures counters, so the bench
//! can report `state.vct.fast_path.hit`/`.miss` — i.e. whether the tree-aux roots
//! we fed actually engaged the verified-commitment-trees fast path.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

#[derive(Clone, Default)]
pub struct BenchRecorder {
    counters: Arc<Mutex<BTreeMap<String, Arc<AtomicU64>>>>,
}

impl BenchRecorder {
    fn handle(&self, name: &str) -> Arc<AtomicU64> {
        self.counters
            .lock()
            .expect("metrics map not poisoned")
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// Current value of a counter (0 if never touched).
    pub fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .expect("metrics map not poisoned")
            .get(name)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

struct AtomicCounter(Arc<AtomicU64>);

impl CounterFn for AtomicCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

impl Recorder for BenchRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(AtomicCounter(self.handle(key.name()))))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

/// Install the recorder globally (idempotent across the process). Returns a
/// handle to read counters from. If a recorder is already installed, returns the
/// supplied one anyway (its counters just won't receive updates).
pub fn install() -> BenchRecorder {
    let recorder = BenchRecorder::default();
    let _ = metrics::set_global_recorder(recorder.clone());
    recorder
}
