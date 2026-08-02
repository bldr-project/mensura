//! Capturing and asserting on measurements from tests.
//!
//! ```
//! use mensura::{count, measure};
//!
//! let metrics = mensura::test_backend();
//!
//! measure!("health", route = "/health", ?latency_ms = 4.0);
//! count!("health", route = "/health");
//!
//! assert_eq!(metrics.query().measurement("health").count(), 2);
//! assert_eq!(metrics.query().with_field("latency_ms").sum("latency_ms"), 4.0);
//! ```

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;

use crate::backend::{Backend, BackendError};
use crate::datapoint::DataPoint;
use crate::queue::{Config, Stats};
use crate::value::{MeasurementField, MeasurementTag, Value, ValueType};

/// Serializes tests that install a test backend.
///
/// The queue is process-wide, so two test threads capturing at once would see
/// each other's measurements. Holding this lock for the lifetime of the handle
/// makes `test_backend()` block instead of interleave.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// How long [`TestBackend::flush`] waits for the queue to drain.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Installs a capturing backend for the duration of a test.
///
/// The returned handle deregisters the backend when it is dropped. While it is
/// alive it holds a process-wide lock, so tests using it run one at a time and
/// never observe each other's measurements — including tests that record from
/// spawned threads.
///
/// Any backend registered beforehand is deregistered and *not* restored.
///
/// Do not hold two handles at once on the same thread: the second call waits
/// for the first handle to be dropped, which never happens.
pub fn test_backend() -> TestBackend {
    TestBackend::with_config(
        Config::default()
            .with_batch_size(64)
            .with_flush_interval(Duration::from_millis(20))
            .with_shutdown_timeout(Duration::from_secs(2))
            // So that the blocking `flush` below works the same in a plain
            // `#[test]` and in a single-threaded `#[tokio::test]`.
            .with_dedicated_thread(true),
    )
}

/// A handle to the measurements captured during a test.
///
/// See [`test_backend`].
#[derive(Debug)]
pub struct TestBackend {
    store: Arc<Store>,
    guard: Option<MutexGuard<'static, ()>>,
}

#[derive(Debug, Default)]
struct Store {
    points: Mutex<Vec<DataPoint>>,
}

impl Store {
    fn lock(&self) -> MutexGuard<'_, Vec<DataPoint>> {
        self.points
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The [`Backend`] half of a [`TestBackend`].
#[derive(Debug)]
struct Recorder {
    store: Arc<Store>,
}

#[async_trait]
impl Backend for Recorder {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        self.store.lock().extend(points.iter().cloned());
        Ok(())
    }

    fn name(&self) -> &'static str {
        "test"
    }
}

impl TestBackend {
    /// [`test_backend`] with an explicit queue [`Config`].
    pub fn with_config(config: Config) -> Self {
        let guard = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // A previous test's backend may still be installed if its handle leaked;
        // clear the way so registration below cannot fail.
        crate::queue::deregister_global_backend();

        let store = Arc::new(Store::default());
        let recorder = Recorder {
            store: Arc::clone(&store),
        };
        crate::queue::register_global_backend_with_config(recorder, config)
            .expect("mensura: a backend was registered concurrently with test_backend()");

        TestBackend {
            store,
            guard: Some(guard),
        }
    }

    /// Waits for every measurement recorded so far to reach the store, then
    /// returns a query over them.
    pub fn query(&self) -> Query {
        self.flush();
        Query {
            points: self.store.lock().clone(),
        }
    }

    /// All captured points, oldest first.
    pub fn points(&self) -> Vec<DataPoint> {
        self.flush();
        self.store.lock().clone()
    }

    /// Number of captured points.
    pub fn len(&self) -> usize {
        self.flush();
        self.store.lock().len()
    }

    /// True if nothing has been captured.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops everything captured so far.
    pub fn clear(&self) {
        self.flush();
        self.store.lock().clear();
    }

    /// Blocks until the queue has handed everything to the recorder.
    ///
    /// # Panics
    ///
    /// Panics if the queue does not drain within a few seconds. Returning
    /// quietly would hand the test a stale view of the store, and the
    /// assertion that then failed would say nothing about why.
    pub fn flush(&self) {
        assert!(
            crate::queue::flush_with_timeout(FLUSH_TIMEOUT),
            "mensura: the test backend did not drain within {FLUSH_TIMEOUT:?};              is a backend blocked, or was the handle dropped?"
        );
    }

    /// The queue counters for this test.
    pub fn stats(&self) -> Stats {
        crate::queue::stats().unwrap_or_default()
    }
}

impl Drop for TestBackend {
    fn drop(&mut self) {
        crate::queue::deregister_global_backend();
        // Release the process-wide lock only after the backend is gone, so the
        // next test cannot register on top of this one.
        self.guard.take();
    }
}

/// A filtered view over captured measurements.
///
/// Filters are applied as they are chained; every method keeps the points that
/// match. Values compare by content, so `.tag("host", "a")` matches whether the
/// macro stored `a` as a static or an owned string.
#[derive(Clone, Debug, Default)]
pub struct Query {
    points: Vec<DataPoint>,
}

impl Query {
    /// A query over an explicit set of points, for testing the query itself.
    pub fn over(points: Vec<DataPoint>) -> Self {
        Query { points }
    }

    /// Keeps points recorded under `measurement`.
    ///
    /// This is the first argument of the macro that recorded them.
    pub fn measurement(self, measurement: &str) -> Self {
        self.retain(|point| point.measurement == measurement)
    }

    /// Keeps points whose `key` tag equals `value`.
    pub fn tag(self, key: &str, value: impl MeasurementTag) -> Self {
        let expected = value.into_tag_value();
        self.retain(|point| matches(point, ValueType::Tag, key, Some(&expected)))
    }

    /// Keeps points whose `key` field equals `value`.
    pub fn field(self, key: &str, value: impl MeasurementField) -> Self {
        let expected = value.into_field_value();
        self.retain(|point| matches(point, ValueType::Field, key, Some(&expected)))
    }

    /// Keeps points carrying a `key` tag, whatever its value.
    pub fn with_tag(self, key: &str) -> Self {
        self.retain(|point| matches(point, ValueType::Tag, key, None))
    }

    /// Keeps points carrying a `key` field, whatever its value.
    pub fn with_field(self, key: &str) -> Self {
        self.retain(|point| matches(point, ValueType::Field, key, None))
    }

    /// Keeps points *not* carrying a `key` tag.
    pub fn without_tag(self, key: &str) -> Self {
        self.retain(|point| !matches(point, ValueType::Tag, key, None))
    }

    /// Keeps points *not* carrying a `key` field.
    pub fn without_field(self, key: &str) -> Self {
        self.retain(|point| !matches(point, ValueType::Field, key, None))
    }

    /// Keeps points whose doc comment is exactly `docstring`.
    pub fn docstring(self, docstring: &str) -> Self {
        self.retain(|point| point.docstring == Some(docstring))
    }

    /// Keeps points satisfying an arbitrary predicate.
    pub fn matching(self, predicate: impl FnMut(&DataPoint) -> bool) -> Self {
        self.retain(predicate)
    }

    fn retain(mut self, mut predicate: impl FnMut(&DataPoint) -> bool) -> Self {
        self.points.retain(|point| predicate(point));
        self
    }

    /// The matching points.
    pub fn points(&self) -> &[DataPoint] {
        &self.points
    }

    /// How many points matched.
    pub fn count(&self) -> usize {
        self.points.len()
    }

    /// True if no point matched.
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Every value recorded for `field`, in order.
    pub fn values(&self, field: &str) -> Vec<Value> {
        self.points
            .iter()
            .filter_map(|point| point.field(field).cloned())
            .collect()
    }

    /// Every value recorded for `field` that is string shaped.
    pub fn string_values(&self, field: &str) -> Vec<String> {
        self.points
            .iter()
            .filter_map(|point| point.field(field)?.as_str().map(str::to_string))
            .collect()
    }

    /// Every value recorded for `field` that is numeric, as `f64`.
    ///
    /// Durations come through as nanoseconds.
    pub fn numeric_values(&self, field: &str) -> Vec<f64> {
        self.points
            .iter()
            .filter_map(|point| point.field(field)?.as_f64())
            .collect()
    }

    /// How many matching points carry a numeric `field`.
    pub fn sample_count(&self, field: &str) -> usize {
        self.numeric_values(field).len()
    }

    /// Sum of `field` across matching points. `0.0` if there are none.
    pub fn sum(&self, field: &str) -> f64 {
        self.numeric_values(field).iter().sum()
    }

    /// [`Query::sum`], named for symmetry with [`Query::sum_i64`].
    pub fn sum_f64(&self, field: &str) -> f64 {
        self.sum(field)
    }

    /// Sum of `field` as integers; floats are truncated.
    pub fn sum_i64(&self, field: &str) -> i64 {
        self.points
            .iter()
            .filter_map(|point| point.field(field)?.as_i64())
            .sum()
    }

    /// Smallest value of `field`, or `None` if nothing numeric matched.
    pub fn min(&self, field: &str) -> Option<f64> {
        self.numeric_values(field).into_iter().reduce(f64::min)
    }

    /// Largest value of `field`, or `None` if nothing numeric matched.
    pub fn max(&self, field: &str) -> Option<f64> {
        self.numeric_values(field).into_iter().reduce(f64::max)
    }

    /// Arithmetic mean of `field`, or `None` if nothing numeric matched.
    pub fn mean(&self, field: &str) -> Option<f64> {
        let values = self.numeric_values(field);
        if values.is_empty() {
            return None;
        }
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }

    /// Linearly interpolated percentile of `field`.
    ///
    /// `percentile` is given in `0..=100`, so `p("time", 99.0)` is the p99.
    /// Returns `None` if nothing numeric matched.
    pub fn p(&self, field: &str, percentile: f64) -> Option<f64> {
        let mut values = self.numeric_values(field);
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let rank = percentile.clamp(0.0, 100.0) / 100.0 * (values.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        if lower == upper {
            return Some(values[lower]);
        }
        let weight = rank - lower as f64;
        Some(values[lower] * (1.0 - weight) + values[upper] * weight)
    }
}

fn matches(point: &DataPoint, kind: ValueType, key: &str, expected: Option<&Value>) -> bool {
    point
        .values
        .iter()
        .filter(|(k, name, _)| *k == kind && *name == key)
        .any(|(_, _, value)| match expected {
            Some(expected) => value.loose_eq(expected),
            None => true,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<DataPoint> {
        vec![
            DataPoint::new("reads")
                .with_tag("host", "a")
                .with_field("latency", 10.0f64)
                .with_field("kind", "read"),
            DataPoint::new("writes")
                .with_tag("host", "b")
                .with_field("latency", 20.0f64)
                .with_field("kind", "write"),
            DataPoint::new("reads")
                .with_tag("host", "a")
                .with_field("latency", 30.0f64)
                .with_field("errors", 1i64),
        ]
    }

    #[test]
    fn filters_compose() {
        let query = Query::over(sample());
        assert_eq!(query.clone().tag("host", "a").count(), 2);
        assert_eq!(
            query.clone().tag("host", "a").with_field("errors").count(),
            1
        );
        assert_eq!(query.clone().field("kind", "write").count(), 1);
        assert_eq!(query.clone().without_field("kind").count(), 1);
        assert_eq!(query.clone().with_tag("nope").count(), 0);
        assert_eq!(query.clone().measurement("reads").count(), 2);
        assert_eq!(query.clone().measurement("nope").count(), 0);
        assert_eq!(query.matching(|p| p.tags().count() == 1).count(), 3);
    }

    #[test]
    fn aggregations() {
        let query = Query::over(sample());
        assert_eq!(query.sum("latency"), 60.0);
        assert_eq!(query.sum_f64("latency"), 60.0);
        assert_eq!(query.sum_i64("latency"), 60);
        assert_eq!(query.min("latency"), Some(10.0));
        assert_eq!(query.max("latency"), Some(30.0));
        assert_eq!(query.mean("latency"), Some(20.0));
        assert_eq!(query.sample_count("latency"), 3);
        assert_eq!(query.sample_count("kind"), 0, "strings are not samples");
        assert_eq!(query.string_values("kind"), vec!["read", "write"]);
        assert_eq!(query.values("errors"), vec![Value::I64(1)]);
    }

    #[test]
    fn empty_aggregations_are_neutral() {
        let query = Query::default();
        assert_eq!(query.sum("x"), 0.0);
        assert_eq!(query.min("x"), None);
        assert_eq!(query.mean("x"), None);
        assert_eq!(query.p("x", 50.0), None);
        assert!(query.is_empty());
    }

    #[test]
    fn percentiles_interpolate() {
        let points = (1..=10)
            .map(|i| DataPoint::new("values").with_field("v", f64::from(i)))
            .collect();
        let query = Query::over(points);
        assert_eq!(query.p("v", 0.0), Some(1.0));
        assert_eq!(query.p("v", 100.0), Some(10.0));
        assert_eq!(query.p("v", 50.0), Some(5.5));
        // Clamped rather than panicking.
        assert_eq!(query.p("v", 1000.0), Some(10.0));
    }

    #[test]
    fn a_tag_filter_never_matches_a_field() {
        let points = vec![DataPoint::new("m").with_field("host", "a")];
        assert_eq!(Query::over(points.clone()).tag("host", "a").count(), 0);
        assert_eq!(Query::over(points).field("host", "a").count(), 1);
    }
}
