//! The InfluxDB v2 backend: batches are POSTed as line protocol.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::{Client, StatusCode};

use crate::backend::{Backend, BackendError};
use crate::datapoint::DataPoint;
use crate::line_protocol::{self, Options};
use crate::measure;
use crate::value::{MeasurementField, MeasurementTag};

/// Measurement prefix used by this backend's reports about itself.
///
/// Points whose name starts with this are mensura's own; a batch made up
/// entirely of them produces no further report, which is what stops the
/// reporting from feeding itself.
pub const SELF_MEASUREMENT_PREFIX: &str = "mensura.influxdb.v2.";

/// Measurement recording the outcome of one write request.
///
/// Tagged `outcome` (`success` or `failure`), with fields `points` (how many
/// measurements the request carried), `duration` (how long it took) and `retries`.
///
/// The report travels through the queue like any other measurement, so the very
/// last write of a process — the one that drains the queue on shutdown — has
/// nowhere left to report to, and is not counted.
pub const WRITE_MEASUREMENT: &str = "mensura.influxdb.v2.write";

/// Writes batches to an InfluxDB `/api/v2/write` endpoint.
///
/// The API version is named at construction rather than assumed, so a call site
/// says which protocol it speaks:
///
/// ```no_run
/// use mensura::backends::InfluxDb;
///
/// let backend = InfluxDb::v2("https://influxdb.example.com")
///     .with_auth_token(std::env::var("INFLUXDB_TOKEN").unwrap())
///     .with_org("myorganization")
///     .with_bucket("metrics");
///
/// mensura::register_global_backend(backend).expect("backend is already registered");
/// ```
///
/// Each point names its own measurement — the first argument of the macro that
/// recorded it — so the backend only decides where the points go, not what they
/// are called.
///
/// Values that describe the process rather than the event — host, service,
/// version, environment — belong on the backend rather than on every call site:
///
/// ```no_run
/// # use mensura::backends::InfluxDb;
/// let backend = InfluxDb::v2("https://influxdb.example.com")
///     .with_global_tag("host", hostname())
///     .with_global_tag("env", "production")
///     .with_global_field("version", env!("CARGO_PKG_VERSION"));
/// # fn hostname() -> String { String::new() }
/// ```
pub struct InfluxDb {
    url: String,
    token: Option<String>,
    org: String,
    bucket: String,
    options: Options,
    timeout: Duration,
    retries: usize,
    retry_delay: Duration,
    /// Built on first use, so that `timeout` can still be configured after
    /// construction; primed directly by [`InfluxDb::with_client`].
    client: OnceLock<Client>,
    self_metrics: bool,
}

impl InfluxDb {
    /// Creates a backend speaking the InfluxDB 2.x write API against a base
    /// URL, e.g. `https://influxdb.example.com`.
    ///
    /// This is the `/api/v2/write` endpoint, addressed by org and bucket and
    /// authenticated with `Authorization: Token …`. InfluxDB 3 servers accept
    /// it too, as their v2 compatibility endpoint.
    pub fn v2(url: impl Into<String>) -> Self {
        InfluxDb {
            url: url.into().trim_end_matches('/').to_string(),
            token: None,
            org: String::new(),
            bucket: String::new(),
            options: Options::new(),
            timeout: Duration::from_secs(10),
            retries: 2,
            retry_delay: Duration::from_millis(250),
            client: OnceLock::new(),
            self_metrics: true,
        }
    }

    /// Sets the `Authorization: Token …` credential.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets the destination organization.
    pub fn with_org(mut self, org: impl Into<String>) -> Self {
        self.org = org.into();
        self
    }

    /// Sets the destination bucket.
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    /// Adds a tag to every point this backend writes.
    ///
    /// A point that already carries `key` keeps its own value.
    pub fn with_global_tag(mut self, key: impl Into<String>, value: impl MeasurementTag) -> Self {
        self.options.set_global_tag(key, value);
        self
    }

    /// Adds a field to every point this backend writes.
    ///
    /// A point that already carries `key` keeps its own value.
    pub fn with_global_field(
        mut self,
        key: impl Into<String>,
        value: impl MeasurementField,
    ) -> Self {
        self.options.set_global_field(key, value);
        self
    }

    /// Adds several global tags at once.
    ///
    /// ```
    /// # use mensura::backends::InfluxDb;
    /// let backend = InfluxDb::v2("https://influxdb.example.com")
    ///     .with_global_tags([("host", "web-1"), ("region", "eu")]);
    /// ```
    pub fn with_global_tags<K, V>(mut self, tags: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: MeasurementTag,
    {
        for (key, value) in tags {
            self.options.set_global_tag(key, value);
        }
        self
    }

    /// Adds several global fields at once.
    pub fn with_global_fields<K, V>(mut self, fields: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: MeasurementField,
    {
        for (key, value) in fields {
            self.options.set_global_field(key, value);
        }
        self
    }

    /// Replaces the global tags and fields wholesale.
    pub fn with_options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// The global values this backend writes with.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Renders a batch exactly as it would be sent, with the count of points
    /// that had to be skipped. Useful for tests and for debugging what a
    /// backend is actually shipping.
    pub fn payload(&self, points: &[DataPoint]) -> (String, usize) {
        line_protocol::write_batch(points, &self.options)
    }

    /// Sets the per-request timeout. Ignored if a client is supplied with
    /// [`InfluxDb::with_client`].
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets how many times a failed write is retried. Defaults to 2.
    ///
    /// Only transport errors and 5xx responses are retried; a batch InfluxDB
    /// considers malformed will never succeed, so it is dropped immediately.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    /// Sets the delay between retries.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Controls whether the backend records [`WRITE_MEASUREMENT`] for each
    /// request it makes. On by default.
    pub fn with_self_metrics(mut self, enabled: bool) -> Self {
        self.self_metrics = enabled;
        self
    }

    /// Supplies a pre-configured HTTP client (proxies, custom roots, …).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = OnceLock::from(client);
        self
    }

    /// The URL a write is POSTed to.
    pub fn write_url(&self) -> String {
        format!(
            "{}/api/v2/write?org={}&bucket={}&precision=ns",
            self.url,
            encode(&self.org),
            encode(&self.bucket),
        )
    }

    /// Checks that the server is reachable and ready.
    pub async fn ping(&self) -> Result<String, BackendError> {
        let response = self
            .client()
            .get(format!("{}/health", self.url))
            .send()
            .await
            .map_err(BackendError::write)?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(body)
        } else {
            Err(BackendError::Write(format!(
                "health check failed: {status}: {body}"
            )))
        }
    }

    /// The error for `skipped` points the encoder left out of a batch, naming
    /// their measurements.
    fn skipped_error(&self, points: &[DataPoint], skipped: usize) -> BackendError {
        let mut names: Vec<&str> = Vec::new();
        let mut scratch = String::new();
        for point in points {
            if !line_protocol::write_point(&mut scratch, point, &self.options)
                && !names.contains(&point.measurement)
            {
                names.push(point.measurement);
            }
            scratch.clear();
        }
        BackendError::partial(
            skipped,
            format!(
                "{skipped} point(s) were not written ({}): a point needs a name and at \
                 least one representable field not named `{}`",
                names.join(", "),
                line_protocol::RESERVED_KEY,
            ),
        )
    }

    fn client(&self) -> &Client {
        self.client.get_or_init(|| {
            Client::builder()
                .timeout(self.timeout)
                .build()
                .unwrap_or_else(|_| Client::new())
        })
    }

    async fn post(&self, payload: String) -> WriteOutcome {
        let url = self.write_url();
        let mut retries = 0;
        loop {
            let failure = match self.send_once(&url, &payload).await {
                Ok(()) => return WriteOutcome::succeeded(retries),
                Err(failure) => failure,
            };
            if !failure.worth_retrying || retries >= self.retries {
                return WriteOutcome::failed(retries, failure.error);
            }
            retries += 1;
            tokio::time::sleep(self.retry_delay).await;
        }
    }

    /// Records how the request went, as a measurement like any other.
    ///
    /// Skipped when the batch held nothing but mensura's own reports: a report
    /// about a report would keep the queue busy forever.
    fn report_write(
        &self,
        batch: &[DataPoint],
        points: usize,
        elapsed: Duration,
        outcome: &WriteOutcome,
    ) {
        if !self.self_metrics {
            return;
        }
        // The report itself travels through the queue and comes back here in a
        // later batch. Reporting only on batches that carried something the
        // application asked for is what makes that terminate.
        let carried_user_data = batch
            .iter()
            .any(|point| !point.measurement.starts_with(SELF_MEASUREMENT_PREFIX));
        if !carried_user_data {
            return;
        }

        let retries = outcome.retries;
        let outcome = if outcome.result.is_ok() {
            "success"
        } else {
            "failure"
        };
        measure!(
            /// One write request made by the InfluxDB backend.
            WRITE_MEASUREMENT,
            outcome,
            ?points,
            ?duration = elapsed,
            ?retries,
        );
    }

    /// One write attempt.
    async fn send_once(&self, url: &str, payload: &str) -> Result<(), FailedWrite> {
        let mut request = self
            .client()
            .post(url)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(payload.to_string());
        if let Some(token) = &self.token {
            request = request.header("Authorization", format!("Token {token}"));
        }

        let response = request.send().await.map_err(|error| FailedWrite {
            error: BackendError::from_source_chain(&error),
            worth_retrying: true,
        })?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        let message = format!("influxdb returned {status}: {}", body.trim());
        let error = match partial_write_dropped(status, &body) {
            Some(dropped) => BackendError::partial(dropped, message),
            None => BackendError::Write(message),
        };
        Err(FailedWrite {
            error,
            worth_retrying: is_retriable(status),
        })
    }
}

/// The number of points InfluxDB refused from a batch it otherwise stored.
///
/// InfluxDB answers a batch with some unwritable points with `422`, a message
/// starting `partial write:` that names the first refused point, and
/// `dropped=<n>` at its end; every other point of the batch is stored.
fn partial_write_dropped(status: StatusCode, body: &str) -> Option<usize> {
    if status != StatusCode::UNPROCESSABLE_ENTITY || !body.contains("partial write") {
        return None;
    }
    let (_, after) = body.rsplit_once("dropped=")?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// A write attempt that did not succeed, and whether repeating it might.
struct FailedWrite {
    error: BackendError,
    worth_retrying: bool,
}

/// How a write request ended, including the retries it took to get there.
struct WriteOutcome {
    retries: usize,
    result: Result<(), BackendError>,
}

impl WriteOutcome {
    fn succeeded(retries: usize) -> Self {
        WriteOutcome {
            retries,
            result: Ok(()),
        }
    }

    fn failed(retries: usize, error: BackendError) -> Self {
        WriteOutcome {
            retries,
            result: Err(error),
        }
    }
}

/// A 4xx means the payload or the credentials are wrong; repeating the request
/// cannot turn that into a success. Everything else might be transient.
fn is_retriable(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

impl std::fmt::Debug for InfluxDb {
    /// Never prints the auth token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfluxDb")
            .field("url", &self.url)
            .field("org", &self.org)
            .field("bucket", &self.bucket)
            .field("options", &self.options)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[async_trait]
impl Backend for InfluxDb {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        for (what, value) in [("org", &self.org), ("bucket", &self.bucket)] {
            if value.is_empty() {
                return Err(BackendError::Write(format!(
                    "no {what} configured: call `InfluxDb::with_{what}` before registering"
                )));
            }
        }

        let (payload, skipped) = self.payload(points);
        if payload.is_empty() {
            // Nothing to send, so there is no request to report on either.
            return Err(self.skipped_error(points, skipped));
        }

        let started = Instant::now();
        let outcome = self.post(payload).await;
        self.report_write(points, points.len() - skipped, started.elapsed(), &outcome);

        match (outcome.result, skipped) {
            (Ok(()), 0) => Ok(()),
            (Ok(()), _) => Err(self.skipped_error(points, skipped)),
            (Err(error), 0) => Err(error),
            (Err(error), _) => Err(match error.points_lost() {
                Some(dropped) => BackendError::partial(
                    dropped + skipped,
                    format!("{error}; {}", self.skipped_error(points, skipped)),
                ),
                // The request failed outright, so the whole batch is lost.
                None => error,
            }),
        }
    }

    fn name(&self) -> &'static str {
        "influxdb"
    }
}

/// Percent-encodes a query parameter value.
fn encode(value: &str) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_url_is_encoded_and_slash_normalized() {
        let backend = InfluxDb::v2("https://example.com/")
            .with_org("my org")
            .with_bucket("büc/ket");
        assert_eq!(
            backend.write_url(),
            "https://example.com/api/v2/write?org=my%20org&bucket=b%C3%BCc%2Fket&precision=ns"
        );
    }

    #[test]
    fn debug_never_leaks_the_token() {
        let backend = InfluxDb::v2("https://example.com").with_auth_token("s3cret");
        let debug = format!("{backend:?}");
        assert!(!debug.contains("s3cret"), "got {debug}");
        assert!(debug.contains("<redacted>"));
    }

    /// A backend that would fail loudly if it ever reached the network.
    fn offline() -> InfluxDb {
        InfluxDb::v2("http://127.0.0.1:1")
            .with_org("myorganization")
            .with_bucket("metrics")
            .with_retries(0)
    }

    #[tokio::test]
    async fn a_batch_of_unwritable_points_never_hits_the_network() {
        // No representable fields, so nothing to send: this must fail locally
        // rather than POST an empty body to a URL that does not exist.
        let points = vec![
            DataPoint::new("m").with_tag("host", "a"),
            DataPoint::new("dns_lookup").with_field("time", Duration::from_millis(1)),
        ];
        let error = offline().write(&points).await.unwrap_err();
        assert_eq!(error.points_lost(), Some(2));
        let error = error.to_string();
        assert!(
            error.contains("2 point(s) were not written (m, dns_lookup)"),
            "got {error}"
        );
    }

    #[test]
    fn a_partial_write_counts_only_the_dropped_points() {
        let body = r#"{"code":"unprocessable entity","message":"failure writing points to database: partial write: invalid field name: input field \"time\" on measurement \"dns_lookup\" is invalid dropped=1"}"#;
        assert_eq!(
            partial_write_dropped(StatusCode::UNPROCESSABLE_ENTITY, body),
            Some(1)
        );
        assert_eq!(partial_write_dropped(StatusCode::BAD_REQUEST, body), None);
        assert_eq!(
            partial_write_dropped(
                StatusCode::UNPROCESSABLE_ENTITY,
                r#"{"code":"unprocessable entity","message":"bucket not found"}"#
            ),
            None
        );
    }

    #[tokio::test]
    async fn a_missing_org_or_bucket_is_reported_before_writing() {
        let points = vec![DataPoint::new("m").with_field("n", 1i64)];

        let error = InfluxDb::v2("http://127.0.0.1:1")
            .with_bucket("metrics")
            .write(&points)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no org configured"), "got {error}");

        let error = InfluxDb::v2("http://127.0.0.1:1")
            .with_org("myorganization")
            .write(&points)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no bucket configured"), "got {error}");
    }

    #[test]
    fn global_values_are_written_with_every_point() {
        let backend = InfluxDb::v2("https://example.com")
            .with_global_tag("host", "web-1")
            .with_global_tags([("env", "prod"), ("region", "eu")])
            .with_global_field("version", "1.2.3")
            .with_global_fields([("pid", 42u32)]);

        let points = vec![
            DataPoint::new("app")
                .with_tag("route", "/a")
                .with_field("n", 1i64),
            // This point disagrees with a global and must win.
            DataPoint::new("app")
                .with_tag("env", "staging")
                .with_field("n", 2i64),
        ];
        let (payload, skipped) = backend.payload(&points);
        let lines: Vec<_> = payload
            .lines()
            .map(|line| line.rsplit_once(' ').unwrap().0.to_string())
            .collect();

        assert_eq!(skipped, 0);
        assert_eq!(
            lines[0],
            r#"app,route=/a,host=web-1,env=prod,region=eu n=1i,version="1.2.3",pid=42u"#
        );
        assert_eq!(
            lines[1],
            r#"app,env=staging,host=web-1,region=eu n=2i,version="1.2.3",pid=42u"#
        );
        assert_eq!(backend.options().global_tags().len(), 3);
        assert_eq!(backend.options().global_fields().len(), 2);
    }

    #[test]
    fn retry_policy() {
        assert!(is_retriable(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retriable(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_retriable(StatusCode::UNAUTHORIZED));
        assert!(!is_retriable(StatusCode::BAD_REQUEST));
    }
}
