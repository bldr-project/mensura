//! The InfluxDB backend reports on its own writes — without reporting on its
//! reports for ever.

#![cfg(feature = "influxdb")]

use std::time::Duration;

use mensura::backends::{InfluxDb, SELF_MEASUREMENT_PREFIX, WRITE_MEASUREMENT};
use mensura::{Backend, DataPoint};

/// A backend pointed at a port nothing listens on: every write fails at once,
/// which is all these tests need. Retries are off so failures stay quick.
fn offline() -> InfluxDb {
    InfluxDb::v2("http://127.0.0.1:1")
        .with_org("myorganization")
        .with_bucket("metrics")
        .with_retries(0)
}

fn app_points() -> Vec<DataPoint> {
    vec![
        DataPoint::new("call_latency").with_field("ms", 12.5),
        DataPoint::new("call_latency").with_field("ms", 8.0),
    ]
}

#[tokio::test]
async fn a_write_is_reported_as_a_measurement() {
    let metrics = mensura::test_backend();

    let error = offline().write(&app_points()).await.unwrap_err();
    assert!(
        error.to_string().contains("127.0.0.1:1"),
        "the failure should name what it could not reach: {error}"
    );

    let report = metrics.query().measurement(WRITE_MEASUREMENT);
    assert_eq!(report.count(), 1, "one request, one report");
    assert_eq!(report.clone().tag("outcome", "failure").count(), 1);
    assert_eq!(report.sum("points"), 2.0, "both points were in the request");
    assert_eq!(report.sum("retries"), 0.0);
    assert_eq!(
        report.sample_count("time"),
        1,
        "the request should be timed even when it fails"
    );
}

#[tokio::test]
async fn retries_are_counted() {
    let metrics = mensura::test_backend();

    let backend = offline()
        .with_retries(2)
        .with_retry_delay(Duration::from_millis(1));
    let _ = backend.write(&app_points()).await;

    let report = metrics.query().measurement(WRITE_MEASUREMENT);
    assert_eq!(report.sum("retries"), 2.0, "both retries should be counted");
}

#[tokio::test]
async fn reporting_does_not_report_on_itself() {
    let metrics = mensura::test_backend();
    let backend = offline();

    // A batch of nothing but mensura's own measurements is the batch that would
    // start an endless chain, so it produces no report.
    let own = vec![DataPoint::new(WRITE_MEASUREMENT).with_field("points", 1u32)];
    let _ = backend.write(&own).await;
    assert!(metrics.query().measurement(WRITE_MEASUREMENT).is_empty());

    // A batch carrying application data does produce one, even alongside
    // mensura's own points.
    let mut mixed = own.clone();
    mixed.extend(app_points());
    let _ = backend.write(&mixed).await;
    assert_eq!(metrics.query().measurement(WRITE_MEASUREMENT).count(), 1);

    // And that report, coming back around on its own, ends the chain.
    let echo = metrics.points();
    let _ = backend.write(&echo).await;
    assert_eq!(
        metrics.query().measurement(WRITE_MEASUREMENT).count(),
        1,
        "a report about a report would never terminate"
    );
}

#[tokio::test]
async fn self_metrics_can_be_turned_off() {
    let metrics = mensura::test_backend();

    let _ = offline()
        .with_self_metrics(false)
        .write(&app_points())
        .await;

    assert!(metrics.query().is_empty());
}

#[tokio::test]
async fn every_self_measurement_carries_the_prefix() {
    let metrics = mensura::test_backend();

    let _ = offline().write(&app_points()).await;

    let points = metrics.points();
    assert!(!points.is_empty());
    assert!(
        points
            .iter()
            .all(|point| point.measurement.starts_with(SELF_MEASUREMENT_PREFIX)),
        "mensura's own measurements must be recognisable by prefix: {points:?}"
    );
}
