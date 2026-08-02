//! The whole library without a server: record measurements, print them.
//!
//! ```text
//! cargo run --example basic
//! ```

use std::time::Duration;

use mensura::backends::{Composite, Stdout};
use mensura::{Config, count, measure, timer};

fn main() {
    // No Tokio runtime here: mensura starts its own worker thread.
    mensura::register_global_backend_with_config(
        Composite::new()
            .with(Stdout::new().with_prefix("human      | "))
            .with(Stdout::line_protocol().with_prefix("influxdb   | ")),
        // Small batches so the output appears as the work happens.
        Config::default()
            .with_batch_size(4)
            .with_flush_interval(Duration::from_millis(100)),
    )
    .expect("backend is already registered");

    for id in 0..3u32 {
        handle_request(id);
    }

    // Drains the queue and waits for the final write.
    mensura::deregister_global_backend();
}

fn handle_request(id: u32) {
    let mut timer = timer!(
        /// Wall-clock time spent handling one request.
        "request_duration",
        route = "/search",
        ?request_id = id,
    );

    let rows = 10 * (id + 1);
    std::thread::sleep(Duration::from_millis(5 * u64::from(id + 1)));

    // Fields discovered while the timer is running can be attached to it.
    timer.field("rows", rows);
    timer.tag("outcome", if rows > 20 { "large" } else { "small" });

    count!("requests", route = "/search", status = 200);
    measure!(
        "response_size",
        route = "/search",
        ?rows,
        ?cached = id.is_multiple_of(2)
    );
}
