//! Serialization of [`DataPoint`]s into the InfluxDB line protocol.
//!
//! ```text
//! measurement,tag=value,tag2=value2 field="a",field2=3i 1699999999000000000
//! ```

use std::borrow::Cow;
use std::fmt::Write as _;

use crate::datapoint::DataPoint;
use crate::value::{MeasurementField, MeasurementTag, Value};

/// The tags and fields a backend adds to every point it writes.
///
/// ```
/// use mensura::line_protocol::Options;
///
/// let options = Options::new()
///     .with_global_tag("host", "web-1")
///     .with_global_field("version", env!("CARGO_PKG_VERSION"));
/// ```
///
/// Global values fill in gaps rather than overwrite: a point that already
/// carries `host` keeps its own, since the specific measurement knows better
/// than the process-wide default.
#[derive(Clone, Debug, Default)]
pub struct Options {
    global_tags: Vec<(String, Value)>,
    global_fields: Vec<(String, Value)>,
}

impl Options {
    /// Options with no global values.
    pub fn new() -> Self {
        Options::default()
    }

    /// Adds a tag to every point that does not already carry `key`.
    pub fn with_global_tag(mut self, key: impl Into<String>, value: impl MeasurementTag) -> Self {
        self.set_global_tag(key, value);
        self
    }

    /// Adds a field to every point that does not already carry `key`.
    pub fn with_global_field(
        mut self,
        key: impl Into<String>,
        value: impl MeasurementField,
    ) -> Self {
        self.set_global_field(key, value);
        self
    }

    /// Adds a global tag in place, replacing any previous value for `key`.
    pub fn set_global_tag(&mut self, key: impl Into<String>, value: impl MeasurementTag) {
        set(&mut self.global_tags, key.into(), value.into_tag_value());
    }

    /// Adds a global field in place, replacing any previous value for `key`.
    pub fn set_global_field(&mut self, key: impl Into<String>, value: impl MeasurementField) {
        set(
            &mut self.global_fields,
            key.into(),
            value.into_field_value(),
        );
    }

    /// The tags added to every point.
    pub fn global_tags(&self) -> &[(String, Value)] {
        &self.global_tags
    }

    /// The fields added to every point.
    pub fn global_fields(&self) -> &[(String, Value)] {
        &self.global_fields
    }
}

/// The one key InfluxDB refuses as a tag or field key: it names the point's
/// timestamp. A field keyed `time` is dropped on ingest, and a point left with
/// no other field fails with `422 Unprocessable Entity`, so the encoder leaves
/// such keys out rather than send them.
pub const RESERVED_KEY: &str = "time";

fn set(entries: &mut Vec<(String, Value)>, key: String, value: Value) {
    match entries.iter_mut().find(|(name, _)| *name == key) {
        Some(entry) => entry.1 = value,
        None => entries.push((key, value)),
    }
}

/// Appends one point to `out` and returns `true` if anything was written.
///
/// A point with no name, or with no field that has a line-protocol spelling, is
/// skipped and leaves `out` untouched: InfluxDB rejects such points, and one
/// malformed line fails the whole batch. A tag or field keyed [`RESERVED_KEY`]
/// is left out of the line, so a point whose only field is `time` is skipped.
pub fn write_point(out: &mut String, point: &DataPoint, options: &Options) -> bool {
    // An unnamed series cannot be stored; the macros make this unreachable.
    if point.measurement.is_empty() {
        return false;
    }

    let start = out.len();
    escape_measurement(out, point.measurement);

    for (key, value) in point.tags() {
        write_tag(out, key, value);
    }
    for (key, value) in &options.global_tags {
        if point.tag(key).is_none() {
            write_tag(out, key, value);
        }
    }

    let mut wrote_field = false;
    for (key, value) in point.fields() {
        wrote_field |= write_field(out, key, value, !wrote_field);
    }
    for (key, value) in &options.global_fields {
        if point.field(key).is_none() {
            wrote_field |= write_field(out, key, value, !wrote_field);
        }
    }

    if !wrote_field {
        out.truncate(start);
        return false;
    }

    let _ = writeln!(out, " {}", point.unix_timestamp_nanos());
    true
}

/// Serializes a whole batch. Returns the payload and the number of points that
/// had to be skipped.
pub fn write_batch(points: &[DataPoint], options: &Options) -> (String, usize) {
    let mut out = String::with_capacity(points.len() * 128);
    let mut skipped = 0;
    for point in points {
        if !write_point(&mut out, point, options) {
            skipped += 1;
        }
    }
    (out, skipped)
}

fn write_tag(out: &mut String, key: &str, value: &Value) {
    if key == RESERVED_KEY {
        return;
    }
    // Tags are usually strings already, so borrow rather than render.
    let rendered = match value.as_str() {
        Some(text) => Cow::Borrowed(text),
        None => Cow::Owned(value.to_string()),
    };
    // InfluxDB has no concept of an empty tag value; it drops such tags on
    // ingest, so leaving them out keeps what we send honest.
    if rendered.is_empty() {
        return;
    }
    out.push(',');
    escape_key(out, key);
    out.push('=');
    escape_key(out, &rendered);
}

/// Writes `key=value`, prefixed by a space if it opens the field set and a
/// comma otherwise. Returns `false` if the key is [`RESERVED_KEY`] or the value
/// has no line-protocol spelling.
fn write_field(out: &mut String, key: &str, value: &Value, first: bool) -> bool {
    if key == RESERVED_KEY {
        return false;
    }
    let mut rendered = String::new();
    if !write_field_value(&mut rendered, value) {
        return false;
    }
    out.push(if first { ' ' } else { ',' });
    escape_key(out, key);
    out.push('=');
    out.push_str(&rendered);
    true
}

/// Writes a field value, returning `false` if it cannot be represented.
fn write_field_value(out: &mut String, value: &Value) -> bool {
    // Strings are the one shape that needs quoting.
    if let Some(text) = value.as_str() {
        out.push('"');
        escape_string_field(out, text);
        out.push('"');
        return true;
    }

    // Everything else is a number with a type suffix. Writing into a `String`
    // cannot fail, so the `fmt::Result` is discarded.
    let _ = match value {
        Value::I64(v) => write!(out, "{v}i"),
        Value::U64(v) => write!(out, "{v}u"),
        // NaN and the infinities have no line-protocol spelling; sending them
        // would make InfluxDB reject the entire batch.
        Value::F64(v) if !v.is_finite() => return false,
        // `{:?}` keeps the decimal point, so `1.0` stays a float.
        Value::F64(v) => write!(out, "{v:?}"),
        // Boolean fields are recorded as 0/1 integers.
        Value::Bool(v) => write!(out, "{}i", i32::from(*v)),
        Value::Duration(d) => write!(out, "{}i", d.as_nanos().min(i64::MAX as u128)),
        // Handled above, where `as_str` matched.
        Value::StaticStr(_) | Value::String(_) | Value::SharedStr(_) => Ok(()),
    };
    true
}

fn escape_measurement(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            ',' | ' ' => {
                out.push('\\');
                out.push(ch);
            }
            '\n' | '\r' => out.push('_'),
            _ => out.push(ch),
        }
    }
}

/// Escapes a tag key, tag value or field key.
fn escape_key(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            ',' | ' ' | '=' => {
                out.push('\\');
                out.push(ch);
            }
            // Line protocol is newline delimited and offers no escape for a
            // newline outside of quoted strings.
            '\n' | '\r' => out.push('_'),
            _ => out.push(ch),
        }
    }
}

fn escape_string_field(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '"' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn point() -> DataPoint {
        DataPoint::new("mensura")
    }

    fn line(point: &DataPoint) -> String {
        line_with(point, &Options::new())
    }

    fn line_with(point: &DataPoint, options: &Options) -> String {
        let mut out = String::new();
        assert!(write_point(&mut out, point, options));
        // Drop the timestamp, which changes on every run.
        let line = out.trim_end().to_string();
        line.rsplit_once(' ').unwrap().0.to_string()
    }

    #[test]
    fn renders_tags_and_typed_fields() {
        let p = point()
            .with_tag("host", "web-1")
            .with_field("count", 1i64)
            .with_field("ratio", 0.5f64)
            .with_field("name", "abc");
        assert_eq!(
            line(&p),
            r#"mensura,host=web-1 count=1i,ratio=0.5,name="abc""#
        );
    }

    #[test]
    fn unsigned_and_duration_fields() {
        let p = point()
            .with_field("bytes", 7u64)
            .with_field("duration", Duration::from_micros(3));
        assert_eq!(line(&p), "mensura bytes=7u,duration=3000i");
    }

    #[test]
    fn a_time_key_is_left_out() {
        let p = point()
            .with_tag("time", "noon")
            .with_field("time", Duration::from_micros(3))
            .with_field("bytes", 7u64);
        assert_eq!(line(&p), "mensura bytes=7u");
    }

    #[test]
    fn a_point_whose_only_field_is_time_is_skipped() {
        let mut out = String::new();
        let p = DataPoint::new("dns_lookup").with_field("time", Duration::from_micros(3));
        assert!(!write_point(&mut out, &p, &Options::new()));
        assert!(out.is_empty());

        let options = Options::new().with_global_field("time", 1i64);
        let p = DataPoint::new("dns_lookup").with_field("n", 1i64);
        assert!(write_point(&mut out, &p, &options));
        assert_eq!(out.rsplit_once(' ').unwrap().0, "dns_lookup n=1i");
    }

    #[test]
    fn each_point_names_its_own_series() {
        let p = DataPoint::new("http")
            .with_tag("route", "/a")
            .with_field("count", 1i64);
        assert_eq!(line(&p), "http,route=/a count=1i");
    }

    #[test]
    fn an_unnamed_point_is_skipped() {
        let mut out = String::new();
        let p = DataPoint::new("").with_field("count", 1i64);
        assert!(!write_point(&mut out, &p, &Options::new()));
        assert!(out.is_empty());
    }

    #[test]
    fn a_measurement_name_is_escaped() {
        let p = DataPoint::new("odd name,with=chars").with_field("count", 1i64);
        assert_eq!(line(&p), r"odd\ name\,with=chars count=1i");
    }

    #[test]
    fn special_characters_are_escaped() {
        let p = point()
            .with_tag("odd key", "a,b c=d")
            .with_field("text", "say \"hi\"\\")
            .with_tag("multi\nline", "x");
        assert_eq!(
            line(&p),
            r#"mensura,odd\ key=a\,b\ c\=d,multi_line=x text="say \"hi\"\\""#
        );
    }

    #[test]
    fn points_without_fields_are_skipped() {
        let mut out = String::new();
        assert!(!write_point(
            &mut out,
            &point().with_tag("host", "a"),
            &Options::new()
        ));
        assert!(out.is_empty(), "a skipped point must leave no residue");
    }

    #[test]
    fn non_finite_floats_are_dropped() {
        let mut out = String::new();
        let p = point()
            .with_field("bad", f64::NAN)
            .with_field("good", 1.0f64);
        assert!(write_point(&mut out, &p, &Options::new()));
        assert!(out.starts_with("mensura good=1.0 "), "got {out}");

        // A point whose only field is unrepresentable is skipped entirely.
        let mut out = String::new();
        assert!(!write_point(
            &mut out,
            &point().with_field("bad", f64::INFINITY),
            &Options::new()
        ));
    }

    #[test]
    fn empty_tag_values_are_omitted() {
        let p = point()
            .with_tag("empty", "")
            .with_tag("set", "x")
            .with_field("count", 1i64);
        assert_eq!(line(&p), "mensura,set=x count=1i");
    }

    #[test]
    fn global_values_are_added_to_every_point() {
        let options = Options::new()
            .with_global_tag("host", "web-1")
            .with_global_tag("env", "prod")
            .with_global_field("version", "1.2.3");

        let p = point().with_tag("route", "/a").with_field("count", 1i64);
        assert_eq!(
            line_with(&p, &options),
            r#"mensura,route=/a,host=web-1,env=prod count=1i,version="1.2.3""#
        );

        // Globals are per-backend configuration, so they apply to every point.
        let q = point().with_field("count", 2i64);
        assert_eq!(
            line_with(&q, &options),
            r#"mensura,host=web-1,env=prod count=2i,version="1.2.3""#
        );
    }

    #[test]
    fn a_point_overrides_a_global_of_the_same_name() {
        let options = Options::new()
            .with_global_tag("host", "default")
            .with_global_field("version", "1.2.3");
        let p = point()
            .with_tag("host", "specific")
            .with_field("version", "9.9.9");

        assert_eq!(
            line_with(&p, &options),
            r#"mensura,host=specific version="9.9.9""#,
            "the global must not be written a second time"
        );
    }

    #[test]
    fn a_global_field_makes_a_tags_only_point_writable() {
        let options = Options::new().with_global_field("up", 1i64);
        let p = point().with_tag("host", "web-1");
        assert_eq!(line_with(&p, &options), "mensura,host=web-1 up=1i");
    }

    #[test]
    fn globals_follow_the_same_rules_as_point_values() {
        let options = Options::new()
            .with_global_tag("empty", "")
            .with_global_tag("odd key", "a,b")
            .with_global_field("bad", f64::NAN)
            .with_global_field("ok", 1i64);
        let p = point().with_field("count", 1i64);

        assert_eq!(
            line_with(&p, &options),
            r"mensura,odd\ key=a\,b count=1i,ok=1i",
            "empty tag values and non-finite floats are dropped as usual"
        );
    }

    #[test]
    fn setting_a_global_twice_replaces_it() {
        let options = Options::new()
            .with_global_tag("host", "first")
            .with_global_tag("host", "second");
        assert_eq!(options.global_tags().len(), 1);
        assert_eq!(
            line_with(&point().with_field("count", 1i64), &options),
            "mensura,host=second count=1i"
        );
    }

    #[test]
    fn batches_count_skipped_points() {
        let points = vec![point().with_field("a", 1i64), point().with_tag("t", "v")];
        let (payload, skipped) = write_batch(&points, &Options::new());
        assert_eq!(skipped, 1);
        assert_eq!(payload.lines().count(), 1);
    }
}
