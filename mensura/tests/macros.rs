//! What the `measure!`, `count!` and `timer!` macros record.

use std::sync::Arc;
use std::time::Duration;

use mensura::{DataPoint, Value, ValueType, count, measure, timer};

/// The single value recorded by a one-point test.
fn only(points: &[DataPoint]) -> &DataPoint {
    assert_eq!(
        points.len(),
        1,
        "expected exactly one point, got {points:?}"
    );
    &points[0]
}

#[test]
fn a_question_mark_makes_a_field() {
    let metrics = mensura::test_backend();

    measure!("readings", tag1 = "value1", tag2 = 123, tag3 = true, ?field1 = "value1", ?field2 = 2.0, ?field3 = false);

    let points = metrics.points();
    let point = only(&points);

    assert_eq!(point.measurement, "readings");
    assert_eq!(
        point
            .values
            .iter()
            .map(|(kind, ..)| *kind)
            .collect::<Vec<_>>(),
        vec![
            ValueType::Tag,
            ValueType::Tag,
            ValueType::Tag,
            ValueType::Field,
            ValueType::Field,
            ValueType::Field,
        ]
    );
    assert_eq!(point.tag("tag1"), Some(&Value::StaticStr("value1")));
    assert_eq!(point.tag("tag2"), Some(&Value::String("123".into())));
    assert_eq!(point.tag("tag3"), Some(&Value::StaticStr("true")));
    assert_eq!(point.field("field1"), Some(&Value::StaticStr("value1")));
    assert_eq!(point.field("field2"), Some(&Value::F64(2.0)));
    assert_eq!(
        point.field("field3"),
        Some(&Value::I64(0)),
        "boolean fields are recorded as 0/1"
    );
}

#[test]
fn string_literals_avoid_an_allocation_but_expressions_do_not() {
    let metrics = mensura::test_backend();
    let owned = String::from("owned");
    let shared: Arc<str> = Arc::from("shared");

    measure!(
        "strings",
        literal = "literal",
        owned,
        shared,
        ?borrowed = "abc".to_string().as_str(),
    );

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.tag("literal"), Some(&Value::StaticStr("literal")));
    assert_eq!(point.tag("owned"), Some(&Value::String("owned".into())));
    assert_eq!(
        point.tag("shared"),
        Some(&Value::SharedStr(Arc::from("shared")))
    );
    assert_eq!(point.field("borrowed"), Some(&Value::String("abc".into())));
}

#[test]
fn every_number_shape_is_a_field() {
    let metrics = mensura::test_backend();

    measure!(
        "numbers",
        ?signed = -7i16,
        ?unsigned = 7u32,
        ?float = 1.25f32,
        ?big = i128::MAX,
        ?duration = Duration::from_millis(3),
    );

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.field("signed"), Some(&Value::I64(-7)));
    assert_eq!(point.field("unsigned"), Some(&Value::U64(7)));
    assert_eq!(point.field("float"), Some(&Value::F64(1.25)));
    assert_eq!(
        point.field("big"),
        Some(&Value::I64(i64::MAX)),
        "i128 saturates"
    );
    assert_eq!(
        point.field("duration"),
        Some(&Value::Duration(Duration::from_millis(3)))
    );
}

#[test]
fn values_may_be_arbitrary_expressions() {
    let metrics = mensura::test_backend();
    let base = 20u32;
    let name = |suffix: &str| format!("web-{suffix}");

    measure!("exprs", host = name("1"), ?total = base + 22, ?nested = (1..=4).sum::<u32>());

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.tag("host"), Some(&Value::String("web-1".into())));
    assert_eq!(point.field("total"), Some(&Value::U64(42)));
    assert_eq!(point.field("nested"), Some(&Value::U64(10)));
}

#[test]
fn keys_may_be_keywords_or_string_literals() {
    let metrics = mensura::test_backend();

    measure!("keys", r#type = "raw", fn = "keyword", "dotted.key" = "quoted", ?count = 1);

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.tag("type"), Some(&Value::StaticStr("raw")));
    assert_eq!(point.tag("fn"), Some(&Value::StaticStr("keyword")));
    assert_eq!(point.tag("dotted.key"), Some(&Value::StaticStr("quoted")));
}

#[test]
fn a_doc_comment_inside_the_invocation_is_attached() {
    let metrics = mensura::test_backend();

    measure!(
        /// How long the upstream took to answer.
        /// Second line.
        "documented",
        route = "/search",
        ?latency = 3.0,
    );
    count!("plain", route = "/plain");

    let points = metrics.points();
    assert_eq!(
        points[0].docstring,
        Some("How long the upstream took to answer.\nSecond line.")
    );
    assert_eq!(points[1].docstring, None);
    assert_eq!(
        metrics
            .query()
            .docstring("How long the upstream took to answer.\nSecond line.")
            .count(),
        1
    );
}

#[test]
fn count_implies_a_count_field() {
    let metrics = mensura::test_backend();

    count!("hits", route = "/search");
    count!("hits", route = "/search", ?count = 5i64);
    count!("hits", route = "/search", ?count = 3u64);
    count!("hits", route = "/search", ?count = u64::MAX);

    assert_eq!(
        metrics.query().tag("route", "/search").sum("count"),
        9.0 + i64::MAX as f64
    );
    assert!(
        metrics
            .query()
            .measurement("hits")
            .points()
            .iter()
            .all(|point| matches!(point.field("count"), Some(Value::I64(_)))),
        "every `count` is an i64"
    );
    assert_eq!(
        metrics.query().field("count", 1).count(),
        1,
        "the implied field is exactly 1"
    );
}

#[test]
fn a_timer_reports_elapsed_time_on_drop() {
    let metrics = mensura::test_backend();

    {
        let _timer = timer!(
            /// Time spent in the block.
            "load_time",
            operation = "load",
            ?attempt = 1u8,
        );
        assert!(metrics.query().is_empty(), "nothing is recorded until drop");
        std::thread::sleep(Duration::from_millis(10));
    }

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.measurement, "load_time");
    assert_eq!(point.tag("operation"), Some(&Value::StaticStr("load")));
    assert_eq!(point.field("attempt"), Some(&Value::U64(1)));
    assert_eq!(point.docstring, Some("Time spent in the block."));

    let elapsed = point
        .field("duration")
        .and_then(Value::as_f64)
        .expect("time field");
    assert!(
        elapsed >= 10_000_000.0,
        "expected >= 10ms in ns, got {elapsed}"
    );
}

#[test]
fn a_timer_can_be_extended_stopped_or_discarded() {
    let metrics = mensura::test_backend();

    let mut timer = timer!("timings", operation = "annotated");
    timer.tag("outcome", "ok").field("rows", 42u32);
    drop(timer);

    let elapsed = timer!("timings", operation = "stopped").stop();
    assert!(elapsed < Duration::from_secs(1));

    timer!("timings", operation = "discarded").discard();

    assert_eq!(metrics.query().count(), 2);
    let annotated = metrics.query().tag("operation", "annotated");
    assert_eq!(annotated.count(), 1);
    assert_eq!(annotated.clone().tag("outcome", "ok").count(), 1);
    assert_eq!(annotated.sum("rows"), 42.0);
    assert_eq!(metrics.query().tag("operation", "discarded").count(), 0);
}

#[test]
fn macros_accept_no_arguments_and_trailing_commas() {
    let metrics = mensura::test_backend();

    // `measure!` needs a field of its own; `count!` and `timer!` supply theirs.
    measure!("a", ?value = 1u8,);
    measure!("b", host = "a", ?value = 1u8,);
    count!("c");
    drop(timer!("d"));

    assert_eq!(metrics.query().count(), 4);
    assert_eq!(metrics.query().with_field("count").count(), 1);
    assert_eq!(metrics.query().with_field("duration").count(), 1);
}

#[test]
fn a_bare_name_records_the_variable_of_that_name() {
    let metrics = mensura::test_backend();

    let host = "web-1";
    let region = String::from("eu");
    let latency = 12.5f64;
    let ok = true;
    let r#type = "raw";

    measure!("shorthand", host, region, r#type, ?latency, ?ok);

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.tag("host"), Some(&Value::String("web-1".into())));
    assert_eq!(point.tag("region"), Some(&Value::String("eu".into())));
    // A shorthand value is a variable read, so it takes the owned path even
    // when the variable happens to hold a `&'static str`.
    assert_eq!(point.tag("type"), Some(&Value::String("raw".into())));
    assert_eq!(point.field("latency"), Some(&Value::F64(12.5)));
    assert_eq!(point.field("ok"), Some(&Value::I64(1)));

    // The shorthand mixes freely with explicit values, in `count!` and `timer!`
    // just as much as in `measure!`.
    count!("shorthand", host, ?latency);
    drop(timer!("shorthand", host, region = "us"));

    assert_eq!(metrics.query().tag("host", "web-1").count(), 3);
    assert_eq!(
        metrics
            .query()
            .tag("region", "us")
            .with_field("duration")
            .count(),
        1
    );
    assert_eq!(metrics.query().with_field("count").sum("latency"), 12.5);
}

#[test]
fn the_measurement_name_may_be_a_const() {
    const LATENCY: &str = "call_latency";
    let metrics = mensura::test_backend();

    measure!(LATENCY, ?ms = 5.0);
    count!(LATENCY);
    drop(timer!(LATENCY));

    assert_eq!(metrics.query().count(), 3);
    assert!(
        metrics
            .points()
            .iter()
            .all(|point| point.measurement == "call_latency")
    );
}

#[test]
fn different_macros_can_share_a_measurement() {
    let metrics = mensura::test_backend();

    measure!("requests", route = "/a", ?bytes = 10u32);
    count!("requests", route = "/a");
    drop(timer!("requests", route = "/a"));

    let query = metrics.query().measurement("requests");
    assert_eq!(query.count(), 3);
    assert_eq!(query.clone().with_field("bytes").sum("bytes"), 10.0);
    assert_eq!(metrics.query().measurement("other").count(), 0);
}

#[test]
fn a_shorthand_name_reads_the_variable_at_the_call_site() {
    let metrics = mensura::test_backend();

    // Whatever the variable holds when the macro runs is what gets recorded,
    // including a value that shadows an earlier one.
    let status = 200u16;
    count!("statuses", status);
    let status = 500u16;
    count!("statuses", status);

    assert_eq!(metrics.query().tag("status", 200).count(), 1);
    assert_eq!(metrics.query().tag("status", 500).count(), 1);
}

#[test]
fn values_may_be_macro_calls_and_other_complex_expressions() {
    let metrics = mensura::test_backend();
    let baz = 7u32;
    let words = ["a", "b", "c"];

    measure!(
        "complex",
        tag = format!("foobar-{baz}"),
        other = format!("{}-{}", "x", baz + 1),
        ?joined = words.join(","),
        ?len = { words.iter().map(|w| w.len()).sum::<usize>() },
        ?ternary = if baz > 3 { 1.0 } else { 0.0 },
        ?nested = vec![1u8, 2, 3].len(),
    );

    let points = metrics.points();
    let point = only(&points);
    assert_eq!(point.tag("tag"), Some(&Value::String("foobar-7".into())));
    assert_eq!(point.tag("other"), Some(&Value::String("x-8".into())));
    assert_eq!(point.field("joined"), Some(&Value::String("a,b,c".into())));
    assert_eq!(point.field("len"), Some(&Value::U64(3)));
    assert_eq!(point.field("ternary"), Some(&Value::F64(1.0)));
    assert_eq!(point.field("nested"), Some(&Value::U64(3)));
}

#[test]
fn measurements_from_other_threads_are_captured() {
    let metrics = mensura::test_backend();

    let workers: Vec<_> = (0..4)
        .map(|id| {
            std::thread::spawn(move || {
                for _ in 0..25 {
                    count!("threaded", worker = id);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(metrics.query().count(), 100);
    assert_eq!(metrics.query().tag("worker", 2).sum("count"), 25.0);
}
