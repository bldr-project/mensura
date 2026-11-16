//! # mensura
//!
//! Metrics instrumentation with InfluxDB semantics: every measurement is a set
//! of **tags** (indexed dimensions) and **fields** (values), recorded through
//! macros that never block the calling code.
//!
//! ```
//! use mensura::{count, measure, timer};
//!
//! # let metrics = mensura::test_backend();
//! // The first argument names the measurement. Tags are bare after it;
//! // fields are marked with `?`.
//! measure!("http_latency", host = "web-1", region = "eu", ?ms = 12.5, ?ok = true);
//!
//! // Values are arbitrary expressions…
//! let route = "/search";
//! measure!("http_request", endpoint = format!("GET {route}"), ?bytes = route.len());
//!
//! // …and a bare name records the variable of that name.
//! let status = 200u16;
//! let ms = 8.0;
//! measure!("http_latency", route, status, ?ms);
//!
//! // `count!` implies `count = 1`.
//! count!("http_request", route = "/search", status = 200);
//!
//! // `timer!` records `duration = <elapsed>` when the binding is dropped.
//! {
//!     let _timer = timer!("query_duration", operation = "query");
//! }
//! # assert_eq!(metrics.query().measurement("http_latency").count(), 2);
//! # assert_eq!(metrics.query().count(), 5);
//! ```
//!
//! Every measurement is named, and the name is a compile-time constant stored
//! as a `&'static str` — it is the InfluxDB measurement the point lands in.
//!
//! A `measure!` also needs at least one field: a point of tags alone records
//! nothing and InfluxDB rejects it, so it is a compile error. `count!` and
//! `timer!` supply a field of their own and can be called with tags only.
//!
//! ## Registering a backend
//!
//! Nothing is recorded until a backend is registered; before that, the macros
//! do not even evaluate their arguments.
//!
//! ```no_run
//! use mensura::backends::{Composite, InfluxDb, Stdout};
//!
//! let backend = Composite::new()
//!     .with(Stdout::new())
//!     .with(
//!         InfluxDb::v2("https://influxdb.example.com")
//!             .with_auth_token(std::env::var("INFLUXDB_TOKEN").unwrap())
//!             .with_org("myorganization")
//!             .with_bucket("metrics"),
//!     );
//!
//! mensura::register_global_backend(backend).expect("backend is already registered");
//! ```
//!
//! Points travel to the backend over a bounded queue. One dispatcher task
//! reads that queue and groups points into batches; a pool of
//! [`Config::writers`] tasks publishes them. Batching comes first by design —
//! ten points arriving together become one request, not ten — and raising the
//! writer count lets those requests overlap when one round trip is not enough
//! to keep up.
//!
//! Sends are `try_send`s: if the queue is full, points are dropped, counted in
//! [`Stats::dropped`] and reported on stderr, rather than slowing down the code
//! being measured.
//!
//! If [`register_global_backend`] is called from inside a Tokio runtime the
//! tasks are spawned onto it; otherwise mensura starts a dedicated thread with
//! a small current-thread runtime, so synchronous programs work too. Either
//! way, drain what is still queued before exiting:
//! [`deregister_global_backend_async`] from async code, or
//! [`deregister_global_backend`] from a plain `main` — nothing in mensura ever
//! blocks a runtime thread unless you call the blocking form yourself.
//!
//! ## Testing
//!
//! [`test_backend`] captures measurements in-process and hands back a queryable
//! store:
//!
//! ```
//! use mensura::count;
//!
//! let metrics = mensura::test_backend();
//!
//! count!("http_request", route = "/search", status = 200);
//! count!("http_request", route = "/search", status = 500);
//!
//! assert_eq!(metrics.query().measurement("http_request").sum("count"), 2.0);
//! assert_eq!(metrics.query().tag("status", 500).count(), 1);
//! ```

#![warn(missing_docs)]

// So that macro expansions, which name `::mensura`, work inside this crate's
// own tests and doctests.
extern crate self as mensura;

pub mod backend;
pub mod backends;
pub mod datapoint;
pub mod line_protocol;
pub mod queue;
pub mod testing;
// Private: `mensura::timer` as a path would collide with the `timer!` macro.
// `Timer` and `TIMER_FIELD` are re-exported at the root instead.
mod timer;
pub mod value;

pub use mensura_macros::{count, measure, timer};

pub use backend::{Backend, BackendError, MultiError, PartialWrite};
pub use datapoint::{DataPoint, MAX_VALUES, ValueVec};
pub use queue::{
    AlreadyRegistered, Config, QueueHandle, Stats, deregister_global_backend,
    deregister_global_backend_async, flush, flush_async, flush_with_timeout, is_registered,
    queue_handle, register_global_backend, register_global_backend_with_config, stats,
};
pub use testing::{Query, TestBackend, test_backend};
pub use timer::{TIMER_FIELD, Timer};
pub use value::{MeasurementField, MeasurementTag, Value, ValueType};

/// Implementation details the macros expand to.
///
/// Nothing here is meant to be named by hand, and it is exempt from the usual
/// semver promises.
#[doc(hidden)]
pub mod internal {
    pub use tinyvec;

    pub use crate::datapoint::{DataPoint, Entry, MAX_VALUES, ValueVec};
    pub use crate::queue::{GLOBAL_QUEUE, GlobalQueue, QueueHandle};
    pub use crate::timer::Timer;
    pub use crate::value::{MeasurementField, MeasurementTag, Value, ValueType};
}
