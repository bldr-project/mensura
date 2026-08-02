//! The dynamic value model shared by the macros, the queue and the backends.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Whether a value is an InfluxDB *tag* (indexed, string typed) or a *field*
/// (not indexed, typed).
///
/// The macros pick this based on the `?` prefix: `measure!(host = "a", ?ms = 3)`
/// records `host` as a tag and `ms` as a field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueType {
    /// An indexed, low-cardinality dimension. Always serialized as a string.
    #[default]
    Tag,
    /// A typed measurement value.
    Field,
}

impl ValueType {
    /// Returns `"tag"` or `"field"`, for diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            ValueType::Tag => "tag",
            ValueType::Field => "field",
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single tag or field value.
///
/// [`Value::StaticStr`] exists so that the overwhelmingly common case — a string
/// literal written directly in a `measure!` invocation — costs no allocation.
#[derive(Clone, Debug)]
pub enum Value {
    /// A borrowed string known at compile time.
    StaticStr(&'static str),
    /// An owned string.
    String(String),
    /// A shared string, cheap to clone.
    SharedStr(Arc<str>),
    /// A signed integer.
    I64(i64),
    /// An unsigned integer.
    U64(u64),
    /// A floating point number.
    F64(f64),
    /// A boolean. Only produced for tags; boolean *fields* are converted to
    /// `0`/`1` integers by [`MeasurementField`].
    Bool(bool),
    /// A duration, serialized as a whole number of nanoseconds.
    Duration(Duration),
}

impl Default for Value {
    fn default() -> Self {
        Value::StaticStr("")
    }
}

impl Value {
    /// Returns the string contents of this value, if it is string shaped.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::StaticStr(s) => Some(s),
            Value::String(s) => Some(s.as_str()),
            Value::SharedStr(s) => Some(s),
            _ => None,
        }
    }

    /// Returns this value as an `f64`, if it is numeric.
    ///
    /// Booleans count as `0.0`/`1.0` and durations as their nanosecond count,
    /// so that aggregations work uniformly over anything number-like.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::I64(v) => Some(*v as f64),
            Value::U64(v) => Some(*v as f64),
            Value::F64(v) => Some(*v),
            Value::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::Duration(d) => Some(d.as_nanos() as f64),
            _ => None,
        }
    }

    /// Returns this value as an `i64`, if it is numeric. Floats are truncated
    /// and out-of-range values saturate.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::U64(v) => Some((*v).min(i64::MAX as u64) as i64),
            Value::F64(v) => Some(*v as i64),
            Value::Bool(v) => Some(i64::from(*v)),
            Value::Duration(d) => Some(d.as_nanos().min(i64::MAX as u128) as i64),
            _ => None,
        }
    }

    /// True if this value is numeric (including booleans and durations).
    pub fn is_numeric(&self) -> bool {
        self.as_f64().is_some()
    }

    /// Compares two values by content rather than by representation.
    ///
    /// This is what the test-query filters use: a value recorded as
    /// `StaticStr("a")` matches a `String("a")` filter, and `I64(1)` matches
    /// `F64(1.0)` or `Bool(true)`. Without it every assertion in a test would
    /// have to know which `Value` variant the macro happened to pick.
    pub fn loose_eq(&self, other: &Value) -> bool {
        match (self.as_str(), other.as_str()) {
            (Some(a), Some(b)) => return a == b,
            // A string never compares equal to a number.
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => {}
        }
        match (self, other) {
            // Compare integers exactly; `as f64` would make large i64s collide.
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::Duration(a), Value::Duration(b)) => a == b,
            _ => match (self.as_f64(), other.as_f64()) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            },
        }
    }
}

impl PartialEq for Value {
    /// Structural equality: `StaticStr("a") != String("a")`.
    ///
    /// Use [`Value::loose_eq`] to compare by content instead.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::StaticStr(a), Value::StaticStr(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::SharedStr(a), Value::SharedStr(b)) => a == b,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::F64(a), Value::F64(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Duration(a), Value::Duration(b)) => a == b,
            _ => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::StaticStr(v) => f.write_str(v),
            Value::String(v) => f.write_str(v),
            Value::SharedStr(v) => f.write_str(v),
            Value::I64(v) => write!(f, "{v}"),
            Value::U64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Duration(v) => write!(f, "{}ns", v.as_nanos()),
        }
    }
}

/// A type that can be recorded as a measurement *tag*.
///
/// Tags are indexed dimensions, so this is deliberately implemented only for
/// string-like types, booleans, characters and integers — never floats, whose
/// cardinality would blow up the index. Also implemented for `Option<T>`, whose
/// `None` records nothing, and for [`Value`], which passes through untouched.
///
/// This is the single conversion used for tags: by the macros, by
/// [`DataPoint::with_tag`](crate::DataPoint::with_tag), by the test query
/// filters and by a backend's global tags.
pub trait MeasurementTag {
    /// Converts `self` into a tag value.
    fn into_tag_value(self) -> Value;
}

/// A type that can be recorded as a measurement *field*.
///
/// Implemented for every primitive number, string-like types, [`bool`] (as
/// `0`/`1`), [`Duration`] (as nanoseconds), and [`Value`], which passes through
/// untouched.
///
/// This is the single conversion used for fields, in the same four places as
/// [`MeasurementTag`].
pub trait MeasurementField {
    /// Converts `self` into a field value.
    fn into_field_value(self) -> Value;
}

macro_rules! impl_str_like {
    ($trait:ident, $method:ident) => {
        impl $trait for &str {
            fn $method(self) -> Value {
                Value::String(self.to_string())
            }
        }

        impl $trait for String {
            fn $method(self) -> Value {
                Value::String(self)
            }
        }

        impl $trait for &String {
            fn $method(self) -> Value {
                Value::String(self.clone())
            }
        }

        impl $trait for Arc<str> {
            fn $method(self) -> Value {
                Value::SharedStr(self)
            }
        }

        impl $trait for &Arc<str> {
            fn $method(self) -> Value {
                Value::SharedStr(Arc::clone(self))
            }
        }

        impl $trait for Cow<'_, str> {
            fn $method(self) -> Value {
                Value::String(self.into_owned())
            }
        }

        impl $trait for char {
            fn $method(self) -> Value {
                Value::String(self.to_string())
            }
        }
    };
}

impl_str_like!(MeasurementTag, into_tag_value);
impl_str_like!(MeasurementField, into_field_value);

impl MeasurementTag for bool {
    fn into_tag_value(self) -> Value {
        Value::StaticStr(if self { "true" } else { "false" })
    }
}

impl MeasurementField for bool {
    /// Booleans are recorded as `0`/`1` integers.
    fn into_field_value(self) -> Value {
        Value::I64(i64::from(self))
    }
}

impl MeasurementField for Duration {
    /// Durations are recorded as a whole number of nanoseconds.
    fn into_field_value(self) -> Value {
        Value::Duration(self)
    }
}

impl MeasurementTag for Value {
    fn into_tag_value(self) -> Value {
        self
    }
}

impl MeasurementField for Value {
    fn into_field_value(self) -> Value {
        self
    }
}

macro_rules! impl_signed {
    ($($ty:ty),*) => {$(
        impl MeasurementTag for $ty {
            fn into_tag_value(self) -> Value {
                Value::String(self.to_string())
            }
        }

        impl MeasurementField for $ty {
            fn into_field_value(self) -> Value {
                Value::I64(self as i64)
            }
        }
    )*};
}

macro_rules! impl_unsigned {
    ($($ty:ty),*) => {$(
        impl MeasurementTag for $ty {
            fn into_tag_value(self) -> Value {
                Value::String(self.to_string())
            }
        }

        impl MeasurementField for $ty {
            fn into_field_value(self) -> Value {
                Value::U64(self as u64)
            }
        }
    )*};
}

impl_signed!(i8, i16, i32, i64, isize);
impl_unsigned!(u8, u16, u32, u64, usize);

impl MeasurementField for f32 {
    fn into_field_value(self) -> Value {
        Value::F64(self as f64)
    }
}

impl MeasurementField for f64 {
    fn into_field_value(self) -> Value {
        Value::F64(self)
    }
}

/// `i128`/`u128` saturate into the 64-bit line-protocol types.
impl MeasurementField for i128 {
    fn into_field_value(self) -> Value {
        Value::I64(self.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }
}

impl MeasurementField for u128 {
    fn into_field_value(self) -> Value {
        Value::U64(self.min(u64::MAX as u128) as u64)
    }
}

/// An absent tag, spelled as the empty value that backends leave out.
///
/// There is no matching [`MeasurementField`] impl: a field has no "absent"
/// spelling, and writing `0` for a missing measurement would be a lie.
impl<T: MeasurementTag> MeasurementTag for Option<T> {
    fn into_tag_value(self) -> Value {
        match self {
            Some(value) => value.into_tag_value(),
            None => Value::StaticStr(""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_stringly_typed() {
        assert_eq!(true.into_tag_value(), Value::StaticStr("true"));
        assert_eq!(42u8.into_tag_value(), Value::String("42".into()));
        assert_eq!("x".into_tag_value(), Value::String("x".into()));
    }

    #[test]
    fn fields_keep_their_type() {
        assert_eq!(true.into_field_value(), Value::I64(1));
        assert_eq!(false.into_field_value(), Value::I64(0));
        assert_eq!((-3i32).into_field_value(), Value::I64(-3));
        assert_eq!(3u32.into_field_value(), Value::U64(3));
        assert_eq!(1.5f32.into_field_value(), Value::F64(1.5));
        assert_eq!(
            Duration::from_millis(2).into_field_value(),
            Value::Duration(Duration::from_millis(2))
        );
    }

    #[test]
    fn an_absent_tag_is_empty() {
        assert_eq!(Some("a").into_tag_value(), Value::String("a".into()));
        assert_eq!(None::<&str>.into_tag_value(), Value::StaticStr(""));
    }

    #[test]
    fn numeric_views() {
        assert_eq!(
            Value::Duration(Duration::from_micros(3)).as_f64(),
            Some(3000.0)
        );
        assert_eq!(
            Value::Duration(Duration::from_micros(3)).as_i64(),
            Some(3000)
        );
        assert_eq!(Value::StaticStr("7").as_f64(), None);
        assert_eq!(Value::U64(u64::MAX).as_i64(), Some(i64::MAX));
    }

    #[test]
    fn loose_eq_ignores_representation() {
        assert!(Value::StaticStr("a").loose_eq(&Value::String("a".into())));
        assert!(Value::I64(1).loose_eq(&Value::F64(1.0)));
        assert!(Value::I64(1).loose_eq(&Value::Bool(true)));
        assert!(Value::Duration(Duration::from_nanos(5)).loose_eq(&Value::I64(5)));
        assert!(!Value::StaticStr("1").loose_eq(&Value::I64(1)));
        assert!(!Value::I64(2).loose_eq(&Value::I64(3)));
    }

    #[test]
    fn strict_eq_is_structural() {
        assert_ne!(Value::StaticStr("a"), Value::String("a".into()));
        assert_eq!(Value::F64(1.5), Value::F64(1.5));
    }
}
