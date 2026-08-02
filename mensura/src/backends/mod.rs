//! The backends mensura ships with.
//!
//! * [`InfluxDb`] — writes line protocol to an InfluxDB 2.x server.
//! * [`Stdout`] — prints measurements, for development.
//! * [`Composite`] — fans a batch out to several backends.
//!
//! Anything implementing [`Backend`](crate::Backend) can be registered, so
//! third-party sinks slot in the same way.

mod composite;
mod stdout;

pub use composite::Composite;
pub use stdout::{Format, Stdout};

#[cfg(feature = "influxdb")]
mod influxdb;
#[cfg(feature = "influxdb")]
pub use influxdb::{InfluxDb, SELF_MEASUREMENT_PREFIX, WRITE_MEASUREMENT};
