//! RAII timing, behind the [`timer!`](crate::timer) macro.

use std::time::{Duration, Instant};

use crate::datapoint::{DataPoint, MAX_VALUES, ValueVec};
use crate::queue::GlobalQueue;
use crate::value::{MeasurementField, MeasurementTag, Value, ValueType};

/// Name of the field a [`Timer`] adds when it reports.
pub const TIMER_FIELD: &str = "duration";

/// Measures the time between its construction and its drop.
///
/// Created by [`timer!`](crate::timer); on drop it records the tags and fields
/// captured at construction plus a `duration` field holding the elapsed time.
///
/// ```
/// # use mensura::timer;
/// # let _metrics = mensura::test_backend();
/// {
///     let _timer = timer!("load", operation = "read");
///     // … work …
/// } // the measurement is recorded here
/// ```
#[derive(Debug)]
pub struct Timer {
    measurement: &'static str,
    start: Instant,
    docstring: Option<&'static str>,
    /// `None` once the timer has reported (or been discarded), which is what
    /// makes reporting exactly-once.
    values: Option<ValueVec>,
}

impl Timer {
    /// Starts a timer carrying the given tags and fields.
    ///
    /// Called by [`timer!`](crate::timer); use the macro instead.
    #[doc(hidden)]
    pub fn new(
        measurement: &'static str,
        docstring: Option<&'static str>,
        values: ValueVec,
    ) -> Self {
        Timer {
            measurement,
            start: Instant::now(),
            docstring,
            values: Some(values),
        }
    }

    /// Time elapsed since the timer started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Attaches another tag to the measurement before it is reported.
    ///
    /// Ignored if the point is already at [`MAX_VALUES`] entries.
    pub fn tag(&mut self, key: &'static str, value: impl MeasurementTag) -> &mut Self {
        self.push(ValueType::Tag, key, value.into_tag_value());
        self
    }

    /// Attaches another field to the measurement before it is reported.
    ///
    /// Useful for outcomes that are only known at the end:
    ///
    /// ```
    /// # use mensura::timer;
    /// # let _metrics = mensura::test_backend();
    /// let mut timer = timer!("load", operation = "read");
    /// timer.field("rows", 42u32);
    /// ```
    pub fn field(&mut self, key: &'static str, value: impl MeasurementField) -> &mut Self {
        self.push(ValueType::Field, key, value.into_field_value());
        self
    }

    /// Reports the measurement now and returns the elapsed time.
    pub fn stop(mut self) -> Duration {
        let elapsed = self.elapsed();
        self.report();
        elapsed
    }

    /// Throws the measurement away — nothing is recorded on drop.
    pub fn discard(mut self) {
        self.values = None;
    }

    fn push(&mut self, kind: ValueType, key: &'static str, value: Value) {
        if let Some(values) = &mut self.values
            && values.len() < MAX_VALUES
        {
            values.push((kind, key, value));
        }
    }

    fn report(&mut self) {
        let Some(mut values) = self.values.take() else {
            return;
        };
        let queue = GlobalQueue.get();
        let Some(queue) = queue.as_ref() else {
            return;
        };
        if values.len() < MAX_VALUES {
            values.push((
                ValueType::Field,
                TIMER_FIELD,
                Value::Duration(self.start.elapsed()),
            ));
        }
        queue.send(DataPoint {
            measurement: self.measurement,
            timestamp: Instant::now(),
            docstring: self.docstring,
            values,
        });
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.report();
    }
}
