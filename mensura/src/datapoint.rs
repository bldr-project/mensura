//! The unit of data that travels from a macro invocation to a backend.

use std::fmt;
use std::time::{Instant, SystemTime};

use tinyvec::ArrayVec;

use crate::value::{MeasurementField, MeasurementTag, Value, ValueType};

/// Maximum number of tags + fields a single measurement can carry.
///
/// The values live inline in the [`DataPoint`], so recording a measurement never
/// allocates. Exceeding this is a compile error at the macro call site.
pub const MAX_VALUES: usize = 16;

// `mensura-macros` hardcodes this bound to report an over-long measurement as a
// compile error rather than a runtime panic.
const _: () = assert!(MAX_VALUES == 16);

/// A single tag or field: its kind, its key, and its value.
pub type Entry = (ValueType, &'static str, Value);

/// The inline vector of tags and fields carried by a [`DataPoint`].
pub type ValueVec = ArrayVec<[Entry; MAX_VALUES]>;

/// One recorded measurement.
///
/// Produced by the [`measure!`](crate::measure), [`count!`](crate::count) and
/// [`timer!`](crate::timer) macros and handed to the registered backend in
/// batches.
#[derive(Clone, Debug)]
pub struct DataPoint {
    /// What is being measured — `"call_latency"`, `"http_requests"`.
    ///
    /// This is the InfluxDB measurement, the name the series is stored under.
    /// The macros take it as their first argument, so it is always a
    /// compile-time constant.
    pub measurement: &'static str,
    /// When the measurement was taken, on the monotonic clock.
    ///
    /// Backends that need wall-clock time convert this with
    /// [`DataPoint::system_time`], which is monotonic-safe.
    pub timestamp: Instant,
    /// The doc comment written inside the macro invocation, if any.
    pub docstring: Option<&'static str>,
    /// The tags and fields of this measurement, in source order.
    pub values: ValueVec,
}

impl DataPoint {
    /// Creates an empty data point stamped with the current instant.
    pub fn new(measurement: &'static str) -> Self {
        DataPoint {
            measurement,
            timestamp: Instant::now(),
            docstring: None,
            values: ValueVec::default(),
        }
    }

    /// Adds a tag. Panics if the point already holds [`MAX_VALUES`] entries.
    pub fn with_tag(mut self, key: &'static str, value: impl MeasurementTag) -> Self {
        self.values
            .push((ValueType::Tag, key, value.into_tag_value()));
        self
    }

    /// Adds a field. Panics if the point already holds [`MAX_VALUES`] entries.
    pub fn with_field(mut self, key: &'static str, value: impl MeasurementField) -> Self {
        self.values
            .push((ValueType::Field, key, value.into_field_value()));
        self
    }

    /// Iterates over the tags.
    pub fn tags(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(ValueType::Tag)
    }

    /// Iterates over the fields.
    pub fn fields(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        self.entries(ValueType::Field)
    }

    fn entries(&self, kind: ValueType) -> impl Iterator<Item = (&'static str, &Value)> {
        self.values
            .iter()
            .filter(move |(k, _, _)| *k == kind)
            .map(|(_, key, value)| (*key, value))
    }

    /// Looks up the first tag with the given key.
    pub fn tag(&self, key: &str) -> Option<&Value> {
        self.lookup(ValueType::Tag, key)
    }

    /// Looks up the first field with the given key.
    pub fn field(&self, key: &str) -> Option<&Value> {
        self.lookup(ValueType::Field, key)
    }

    fn lookup(&self, kind: ValueType, key: &str) -> Option<&Value> {
        self.values
            .iter()
            .find(|(k, name, _)| *k == kind && *name == key)
            .map(|(_, _, value)| value)
    }

    /// True if the point carries no fields.
    ///
    /// InfluxDB rejects such points, so backends drop them.
    pub fn has_no_fields(&self) -> bool {
        self.fields().next().is_none()
    }

    /// Converts the monotonic [`Self::timestamp`] to wall-clock time.
    ///
    /// The conversion is anchored on a `(SystemTime, Instant)` pair captured
    /// once at startup, so a wall-clock jump (NTP step, DST) cannot reorder
    /// points relative to each other.
    pub fn system_time(&self) -> SystemTime {
        let (epoch_system, epoch_instant) = clock_epoch();
        if self.timestamp >= epoch_instant {
            epoch_system + self.timestamp.duration_since(epoch_instant)
        } else {
            epoch_system - epoch_instant.duration_since(self.timestamp)
        }
    }

    /// Nanoseconds since the Unix epoch, the precision InfluxDB writes use.
    pub fn unix_timestamp_nanos(&self) -> i128 {
        match self.system_time().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as i128,
            Err(e) => -(e.duration().as_nanos() as i128),
        }
    }
}

impl fmt::Display for DataPoint {
    /// The measurement and its values, the way a call site would write them:
    /// `call_latency host=web-1 ?ms=12.5`.
    ///
    /// This is what the stdout backend prints and what shows up in a failed
    /// test assertion. The docstring is left out; `{:?}` includes it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.measurement)?;
        for (kind, key, value) in self.values.iter() {
            let prefix = if *kind == ValueType::Field { "?" } else { "" };
            write!(f, " {prefix}{key}={value}")?;
        }
        Ok(())
    }
}

/// Returns the process-wide `(SystemTime, Instant)` anchor pair.
fn clock_epoch() -> (SystemTime, Instant) {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<(SystemTime, Instant)> = OnceLock::new();
    *EPOCH.get_or_init(|| (SystemTime::now(), Instant::now()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn builders_and_lookups() {
        let point = DataPoint::new("requests")
            .with_tag("host", "web-1")
            .with_field("count", 1i64);

        assert_eq!(point.measurement, "requests");
        assert_eq!(point.tags().count(), 1);
        assert_eq!(point.fields().count(), 1);
        // Asserted by content: the builder takes the same `MeasurementTag` and
        // `MeasurementField` route as the macros, so which `Value` variant
        // comes out is the trait's business, not the caller's.
        assert_eq!(point.tag("host").and_then(Value::as_str), Some("web-1"));
        assert_eq!(point.field("count"), Some(&Value::I64(1)));
        assert_eq!(point.tag("count"), None, "tag lookup must not see fields");
        assert!(!point.has_no_fields());
        assert!(DataPoint::new("requests").has_no_fields());
    }

    #[test]
    fn wall_clock_conversion_tracks_the_monotonic_clock() {
        let earlier = DataPoint::new("m");
        std::thread::sleep(Duration::from_millis(5));
        let later = DataPoint::new("m");

        assert!(later.system_time() > earlier.system_time());
        let delta = later.unix_timestamp_nanos() - earlier.unix_timestamp_nanos();
        assert!(delta >= 5_000_000, "expected >= 5ms gap, got {delta}ns");
        // Sanity check that we are somewhere in the 21st century.
        assert!(later.unix_timestamp_nanos() > 1_700_000_000_000_000_000);
    }

    #[test]
    fn display_marks_fields_with_a_question_mark() {
        let point = DataPoint::new("requests")
            .with_tag("host", "web-1")
            .with_field("count", 1i64);
        assert_eq!(point.to_string(), "requests host=web-1 ?count=1");
    }
}
