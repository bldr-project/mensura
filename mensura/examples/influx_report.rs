//! Reports a handful of measurements to a real InfluxDB server.
//!
//! ```text
//! cargo run --example influx_report -- \
//!     --url https://influxdb.example.com --org myorganization --bucket metrics --token "$INFLUXDB_TOKEN"
//! ```
//!
//! The token is read from `--token` or the `INFLUXDB_TOKEN` environment
//! variable; it is never written to the repository.

use std::time::Duration;

use clap::Parser;
use mensura::backends::{Composite, InfluxDb, Stdout};
use mensura::{count, measure, timer};

#[derive(Debug, Parser)]
#[command(about = "Send example measurements to InfluxDB with mensura")]
struct Args {
    /// Base URL of the InfluxDB server.
    #[arg(long, env = "INFLUXDB_URL")]
    url: String,

    /// InfluxDB API token.
    #[arg(long, env = "INFLUXDB_TOKEN", hide_env_values = true)]
    token: String,

    /// Organization to write to.
    #[arg(long, env = "INFLUXDB_ORG", default_value = "myorganization")]
    org: String,

    /// Bucket to write to.
    #[arg(long, env = "INFLUXDB_BUCKET", default_value = "mensura")]
    bucket: String,

    /// How many rounds of measurements to report.
    #[arg(long, default_value_t = 3)]
    rounds: u32,

    /// Value for the `host` tag added to every point.
    #[arg(long, default_value = "example-host")]
    host: String,

    /// Deployment environment, added to every point as a tag.
    #[arg(long, default_value = "dev")]
    env: String,

    /// Also print every point to stdout.
    #[arg(long)]
    echo: bool,

    /// Check that the server is reachable before writing.
    #[arg(long)]
    ping: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let influx = InfluxDb::v2(&args.url)
        .with_auth_token(&args.token)
        .with_org(&args.org)
        .with_bucket(&args.bucket)
        .with_timeout(Duration::from_secs(10))
        // Facts about the process, not about any one event: set once here
        // rather than repeated at every call site.
        .with_global_tags([("host", args.host.as_str()), ("env", args.env.as_str())])
        .with_global_field("version", env!("CARGO_PKG_VERSION"));

    if args.ping {
        println!("server: {}", influx.ping().await?.trim());
    }
    println!("writing to {}", influx.write_url());

    // Fan out to stdout as well when asked, so you can see exactly what the
    // InfluxDB backend is sending.
    if args.echo {
        // Share the InfluxDB options so the echo shows the global values too.
        let echo = Stdout::line_protocol()
            .with_options(influx.options().clone())
            .with_prefix("> ");
        mensura::register_global_backend(Composite::new().with(influx).with(echo))
    } else {
        mensura::register_global_backend(influx)
    }
    .expect("backend is already registered");

    for round in 0..args.rounds {
        report(round).await;
    }

    // Keep a handle so the counters survive shutdown, then deregister: that
    // drains the queue and awaits the final write, so nothing is lost when the
    // process exits and no runtime thread is blocked while it happens.
    let queue = mensura::queue_handle().expect("registered above");
    let drained = mensura::deregister_global_backend_async().await;
    let stats = queue.stats();

    println!(
        "queued {} point(s), dropped {}, {} batch(es), {} error(s); drained: {drained}",
        stats.sent, stats.dropped, stats.batches, stats.errors
    );
    Ok(())
}

/// Records one round of measurements.
async fn report(round: u32) {
    // A timer reports `duration = <elapsed>` when it goes out of scope.
    let timer = timer!(
        /// Wall-clock duration of one example round.
        "round_duration",
        operation = "round",
        ?round,
    );

    measure!(
        /// A static reading, so the example always writes something recognisable.
        "sensor_reading",
        region = "eu-central",
        ?temperature = 21.5,
        ?humidity = 48u8,
        ?online = true,
        ?label = "hello from mensura",
    );

    count!("round_started", ?round);

    // Some work to give the timer something to measure.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Each measurement names its own series, so one function can report to
    // several of them.
    measure!("round_progress", ?elapsed = timer.elapsed());
}
