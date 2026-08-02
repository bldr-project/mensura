//! The test-facing capture and query API.

use std::time::Duration;

use mensura::{Value, count, measure, timer};

#[test]
fn filters_and_aggregations_compose() {
    let metrics_helper = mensura::test_backend();

    measure!("m", tag1 = "value", tag2 = "x", ?field1 = "value", ?field2 = 1u8, ?field3 = 4.0);
    measure!("m", tag1 = "value", tag2 = "y", ?field1 = "value", ?field2 = 2u8, ?field3 = 6.0);
    // Filtered out: wrong tag1, missing tag2, wrong field1, missing field2.
    measure!("m", tag1 = "other", tag2 = "x", ?field1 = "value", ?field2 = 3u8, ?field3 = 99.0);
    measure!("m", tag1 = "value", ?field1 = "value", ?field2 = 4u8, ?field3 = 99.0);
    measure!("m", tag1 = "value", tag2 = "z", ?field1 = "nope", ?field2 = 5u8, ?field3 = 99.0);
    measure!("m", tag1 = "value", tag2 = "z", ?field1 = "value", ?field3 = 99.0);

    assert_eq!(
        metrics_helper
            .query()
            .tag("tag1", "value")
            .field("field1", "value")
            .with_tag("tag2")
            .with_field("field2")
            .sum("field3"),
        10.0
    );
}

#[test]
fn aggregations_cover_the_recorded_types() {
    let metrics = mensura::test_backend();

    for (index, latency) in [10.0f64, 20.0, 30.0, 40.0].into_iter().enumerate() {
        measure!(
            "requests",
            route = "/search",
            ?latency,
            ?index,
            ?status = if index % 2 == 0 { "ok" } else { "error" },
            ?took = Duration::from_millis(index as u64),
        );
    }

    let query = || metrics.query().tag("route", "/search");

    assert_eq!(query().count(), 4);
    assert_eq!(query().sum("latency"), 100.0);
    assert_eq!(query().sum_f64("latency"), 100.0);
    assert_eq!(query().sum_i64("index"), 6);
    assert_eq!(query().min("latency"), Some(10.0));
    assert_eq!(query().max("latency"), Some(40.0));
    assert_eq!(query().mean("latency"), Some(25.0));
    assert_eq!(query().sample_count("latency"), 4);
    assert_eq!(query().p("latency", 50.0), Some(25.0));
    assert_eq!(query().p("latency", 100.0), Some(40.0));
    assert_eq!(
        query().string_values("status"),
        vec!["ok", "error", "ok", "error"]
    );
    assert_eq!(
        query().values("index"),
        vec![Value::U64(0), Value::U64(1), Value::U64(2), Value::U64(3)]
    );
    assert_eq!(
        query().numeric_values("took"),
        vec![0.0, 1e6, 2e6, 3e6],
        "durations aggregate as nanoseconds"
    );
}

#[test]
fn values_match_regardless_of_how_they_were_recorded() {
    let metrics = mensura::test_backend();
    let owned = String::from("web-1");

    measure!("m", literal = "web-1", ?number = 1u8);
    measure!("m", owned, ?number = 1.0f64);

    // The tag was stored as a `StaticStr` in one point and a `String` in the
    // other; both are matched by the same filter value.
    assert_eq!(metrics.query().tag("literal", "web-1").count(), 1);
    assert_eq!(metrics.query().tag("owned", "web-1").count(), 1);
    assert_eq!(
        metrics.query().tag("owned", String::from("web-1")).count(),
        1
    );
    assert_eq!(metrics.query().field("number", 1).count(), 2);
    assert_eq!(metrics.query().field("number", 1.0).count(), 2);
    assert_eq!(metrics.query().field("number", true).count(), 2);
}

#[test]
fn negative_filters_and_predicates() {
    let metrics = mensura::test_backend();

    count!("hits", route = "/a", status = 200);
    count!("hits", route = "/b");
    measure!("latencies", route = "/c", ?latency = 5.0);

    assert_eq!(metrics.query().measurement("hits").count(), 2);
    assert_eq!(metrics.query().measurement("latencies").count(), 1);
    assert_eq!(metrics.query().without_tag("status").count(), 2);
    assert_eq!(metrics.query().without_field("count").count(), 1);
    assert_eq!(metrics.query().with_field("count").count(), 2);
    assert_eq!(
        metrics
            .query()
            .matching(|point| point.tags().count() > 1)
            .count(),
        1
    );
    assert!(metrics.query().tag("route", "/missing").is_empty());
    assert_eq!(metrics.query().tag("route", "/missing").sum("count"), 0.0);
}

#[test]
fn the_store_can_be_inspected_and_cleared() {
    let metrics = mensura::test_backend();
    assert!(metrics.is_empty());

    count!("hits", route = "/a");
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics.points().len(), 1);
    assert_eq!(metrics.stats().sent, 1);
    assert_eq!(metrics.stats().dropped, 0);

    metrics.clear();
    assert!(metrics.is_empty());

    count!("hits", route = "/b");
    assert_eq!(metrics.query().tag("route", "/b").count(), 1);
    assert_eq!(metrics.query().tag("route", "/a").count(), 0);
}

#[test]
fn a_timer_is_visible_to_the_query_api() {
    let metrics = mensura::test_backend();

    {
        let _timer = timer!("load_time", operation = "load");
        std::thread::sleep(Duration::from_millis(5));
    }

    let query = metrics.query().tag("operation", "load");
    assert_eq!(query.sample_count("time"), 1);
    assert!(query.min("time").unwrap() >= 5_000_000.0);
    assert_eq!(query.p("time", 99.0), query.max("time"));
}

/// Dropping the handle must restore the previous state, or the next test in
/// this binary would see leftovers.
#[test]
fn the_handle_deregisters_on_drop() {
    {
        let metrics = mensura::test_backend();
        assert!(mensura::is_registered());
        count!("hits", route = "/a");
        assert_eq!(metrics.query().count(), 1);
    }
    assert!(!mensura::is_registered());

    // Recording without a backend is a no-op, and the next handle starts empty.
    count!("hits", route = "/lost");
    let metrics = mensura::test_backend();
    assert!(metrics.query().is_empty());
}
