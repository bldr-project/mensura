//! The queue, the batching worker and the shipped backends, driven through the
//! real global registration path.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use mensura::backends::{Composite, Stdout};
use mensura::{Backend, BackendError, Config, DataPoint, Value, count, measure};

/// Registration is process-wide, so these tests take turns.
static SERIAL: Mutex<()> = Mutex::new(());

/// Registers a backend and deregisters it when the test ends.
struct Registered {
    _guard: MutexGuard<'static, ()>,
}

impl Registered {
    fn new(backend: impl Backend, config: Config) -> Self {
        let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        mensura::register_global_backend_with_config(backend, config)
            .expect("no backend should be registered");
        Registered { _guard: guard }
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        mensura::deregister_global_backend();
    }
}

/// A backend that records the batches it is given, not just the points.
#[derive(Clone, Default)]
struct Recorder {
    batches: Arc<Mutex<Vec<Vec<DataPoint>>>>,
    flushes: Arc<AtomicUsize>,
    delay: Option<Duration>,
    fail: bool,
}

impl Recorder {
    fn batches(&self) -> Vec<Vec<DataPoint>> {
        self.batches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn points(&self) -> Vec<DataPoint> {
        self.batches().into_iter().flatten().collect()
    }
}

#[async_trait]
impl Backend for Recorder {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        self.batches
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(points.clone());
        if self.fail {
            return Err(BackendError::write("recorder was asked to fail"));
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), BackendError> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "recorder"
    }
}

fn fast() -> Config {
    Config::default()
        .with_flush_interval(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_secs(5))
}

#[test]
fn points_reach_the_backend_in_batches() {
    let recorder = Recorder::default();
    let _registered = Registered::new(recorder.clone(), fast().with_batch_size(4));

    for i in 0..8u32 {
        count!("indexed", index = i);
    }
    assert!(mensura::flush(), "flush should be acknowledged");

    let batches = recorder.batches();
    assert_eq!(batches.len(), 2, "8 points at batch_size 4: {batches:?}");
    assert!(batches.iter().all(|batch| batch.len() == 4));
    assert_eq!(recorder.points().len(), 8);
}

#[test]
fn a_partial_batch_is_flushed_by_the_timer() {
    let recorder = Recorder::default();
    let _registered = Registered::new(
        recorder.clone(),
        fast()
            .with_batch_size(1000)
            .with_flush_interval(Duration::from_millis(20)),
    );

    count!("slow", route = "/slow");

    // No explicit flush: the interval has to push the partial batch out.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while recorder.points().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(recorder.points().len(), 1, "the flush interval never fired");
}

#[test]
fn deregistering_drains_what_is_still_queued() {
    let recorder = Recorder::default();
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    mensura::register_global_backend_with_config(
        recorder.clone(),
        // Large batch and a long interval: only the shutdown drain can deliver.
        Config::default()
            .with_batch_size(10_000)
            .with_flush_interval(Duration::from_secs(3600)),
    )
    .expect("no backend should be registered");

    for _ in 0..5 {
        count!("late", route = "/late");
    }

    assert!(mensura::deregister_global_backend(), "shutdown timed out");
    assert_eq!(recorder.points().len(), 5);
    assert!(
        recorder.flushes.load(Ordering::Relaxed) >= 1,
        "the backend should be flushed on shutdown"
    );
    assert!(!mensura::is_registered());
    drop(guard);
}

#[test]
fn a_held_queue_handle_does_not_wedge_shutdown() {
    let recorder = Recorder::default();
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    mensura::register_global_backend_with_config(recorder.clone(), fast().with_batch_size(1000))
        .expect("no backend should be registered");

    // Reading counters after shutdown means holding a handle — and therefore a
    // clone of the sender — past deregistration.
    let queue = mensura::queue_handle().expect("registered");
    count!("hits", route = "/a");

    let started = std::time::Instant::now();
    assert!(
        mensura::deregister_global_backend(),
        "shutdown must not wait for every handle to be dropped"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shutdown stalled"
    );
    assert_eq!(recorder.points().len(), 1);
    assert_eq!(queue.stats().sent, 1, "counters outlive the registration");
    drop(guard);
}

#[test]
fn a_full_queue_drops_points_instead_of_blocking() {
    let recorder = Recorder {
        // Wedge the worker so the queue cannot drain.
        delay: Some(Duration::from_millis(300)),
        ..Recorder::default()
    };
    let _registered = Registered::new(
        recorder.clone(),
        fast().with_queue_capacity(4).with_batch_size(1),
    );

    let started = std::time::Instant::now();
    for i in 0..1_000u32 {
        count!("indexed", index = i);
    }
    let elapsed = started.elapsed();

    let stats = mensura::stats().expect("registered");
    assert!(
        stats.dropped > 0,
        "a wedged worker must cause drops: {stats:?}"
    );
    assert_eq!(stats.sent + stats.dropped, 1_000);
    assert!(
        elapsed < Duration::from_secs(1),
        "recording must not block on a wedged backend, took {elapsed:?}"
    );
}

#[test]
fn backend_errors_are_counted_and_do_not_stop_the_worker() {
    let recorder = Recorder {
        fail: true,
        ..Recorder::default()
    };
    let _registered = Registered::new(
        recorder.clone(),
        fast().with_batch_size(1).with_error_logging(false),
    );

    count!("attempts", attempt = 1);
    assert!(mensura::flush());
    count!("attempts", attempt = 2);
    assert!(mensura::flush());

    let stats = mensura::stats().expect("registered");
    assert_eq!(stats.batches, 2);
    assert_eq!(stats.errors, 2);
    assert_eq!(recorder.points().len(), 2, "the worker kept going");
}

#[test]
fn registering_twice_is_refused() {
    let _registered = Registered::new(Recorder::default(), fast());

    let error = mensura::register_global_backend(Stdout::new()).unwrap_err();
    assert_eq!(error.to_string(), "a mensura backend is already registered");
}

#[test]
fn deregistering_nothing_is_harmless() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    assert!(!mensura::deregister_global_backend());
    assert!(!mensura::flush(), "there is nothing to flush");
    assert!(mensura::stats().is_none());
}

#[test]
fn a_composite_reaches_every_backend() {
    let first = Recorder::default();
    let second = Recorder::default();
    let _registered = Registered::new(
        Composite::new()
            .with(first.clone())
            .with(second.clone())
            .with(Stdout::line_protocol().to_stderr()),
        fast().with_batch_size(2),
    );

    measure!("values", host = "a", ?value = 1i64);
    measure!("values", host = "b", ?value = 2i64);
    assert!(mensura::flush());

    for recorder in [&first, &second] {
        let points = recorder.points();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].tag("host"), Some(&Value::StaticStr("a")));
        assert_eq!(points[1].field("value"), Some(&Value::I64(2)));
    }
}

#[test]
fn several_writers_publish_concurrently() {
    // Each write sleeps; with one writer the batches would be serialized, so
    // finishing in well under the serial time is the evidence of concurrency.
    let recorder = Recorder {
        delay: Some(Duration::from_millis(200)),
        ..Recorder::default()
    };
    let _registered = Registered::new(recorder.clone(), fast().with_batch_size(1).with_writers(4));

    let started = std::time::Instant::now();
    for i in 0..4u32 {
        count!("indexed", index = i);
    }
    assert!(mensura::flush(), "flush should wait for every writer");
    let elapsed = started.elapsed();

    assert_eq!(recorder.batches().len(), 4);
    assert!(
        elapsed < Duration::from_millis(600),
        "4 batches x 200ms should overlap, took {elapsed:?}"
    );
}

#[test]
fn batching_takes_priority_over_spreading_work() {
    // The queue is read by one dispatcher however many writers there are, so
    // points that arrive together go out together instead of being raced into
    // one-point requests by each idle writer.
    let recorder = Recorder::default();
    let _registered = Registered::new(recorder.clone(), fast().with_batch_size(10).with_writers(8));

    for i in 0..10u32 {
        count!("indexed", index = i);
    }
    assert!(mensura::flush());

    let batches = recorder.batches();
    assert_eq!(batches.len(), 1, "expected one batch, got {batches:?}");
    assert_eq!(batches[0].len(), 10);
}

#[test]
fn concurrent_producers_all_get_through() {
    let recorder = Recorder::default();
    let _registered = Registered::new(recorder.clone(), fast().with_batch_size(16));

    let workers: Vec<_> = (0..8)
        .map(|id| {
            std::thread::spawn(move || {
                for _ in 0..100 {
                    count!("workers", worker = id);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(mensura::flush());

    let points = recorder.points();
    assert_eq!(points.len(), 800);
    let stats = mensura::stats().expect("registered");
    assert_eq!(
        stats.dropped, 0,
        "the default queue should absorb 800 points"
    );
}

// The serialization lock is deliberately held across awaits: it is guarding
// process-wide registration for the whole test, not a short critical section.
#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread")]
async fn the_worker_runs_on_an_ambient_runtime() {
    let recorder = Recorder::default();
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    mensura::register_global_backend_with_config(recorder.clone(), fast().with_batch_size(2))
        .expect("no backend should be registered");

    count!("sources", source = "async");
    tokio::spawn(async { count!("sources", source = "spawned") })
        .await
        .unwrap();

    assert!(mensura::flush_async().await);
    assert_eq!(recorder.points().len(), 2);

    // Deregistration blocks, so it belongs on a blocking thread inside a runtime.
    let drained = tokio::task::spawn_blocking(mensura::deregister_global_backend)
        .await
        .unwrap();
    assert!(drained);
    drop(guard);
}
