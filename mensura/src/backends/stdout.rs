//! A backend that prints measurements, for development and debugging.

use std::fmt::Write as _;
use std::io::Write as _;

use async_trait::async_trait;

use crate::backend::{Backend, BackendError};
use crate::datapoint::DataPoint;
use crate::line_protocol::{self, Options};

/// How the [`Stdout`] backend renders points.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Format {
    /// `call_latency host=web-1 ?ms=12.5` — compact and readable.
    #[default]
    Human,
    /// InfluxDB line protocol, exactly what the [`InfluxDb`](super::InfluxDb)
    /// backend would send.
    LineProtocol,
}

/// Writes every batch to standard output (or standard error).
///
/// ```
/// use mensura::backends::Stdout;
///
/// let backend = Stdout::new().with_prefix("metric: ");
/// ```
#[derive(Clone, Debug)]
pub struct Stdout {
    format: Format,
    prefix: String,
    options: Options,
    stderr: bool,
    show_docstrings: bool,
}

impl Default for Stdout {
    fn default() -> Self {
        Self::new()
    }
}

impl Stdout {
    /// A backend printing human-readable lines to stdout.
    pub fn new() -> Self {
        Stdout {
            format: Format::Human,
            prefix: String::new(),
            options: Options::new(),
            stderr: false,
            show_docstrings: true,
        }
    }

    /// A backend printing InfluxDB line protocol.
    pub fn line_protocol() -> Self {
        Stdout::new().with_format(Format::LineProtocol)
    }

    /// Sets the rendering [`Format`].
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    /// Prepends `prefix` to every printed line.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Sets the global tags and fields used by [`Format::LineProtocol`].
    ///
    /// Pass the same [`Options`] as the InfluxDB backend to see locally exactly
    /// what is being shipped.
    pub fn with_options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Prints to standard error instead of standard output.
    pub fn to_stderr(mut self) -> Self {
        self.stderr = true;
        self
    }

    /// Controls whether doc comments are appended to human-readable lines.
    pub fn with_docstrings(mut self, enabled: bool) -> Self {
        self.show_docstrings = enabled;
        self
    }

    fn render(&self, points: &[DataPoint]) -> String {
        let mut out = String::new();
        match self.format {
            Format::LineProtocol => {
                let (payload, _skipped) = line_protocol::write_batch(points, &self.options);
                for line in payload.lines() {
                    out.push_str(&self.prefix);
                    out.push_str(line);
                    out.push('\n');
                }
            }
            Format::Human => {
                for point in points {
                    out.push_str(&self.prefix);
                    // `Display` renders the point the way a call site writes it.
                    let _ = write!(out, "{point}");
                    if self.show_docstrings
                        && let Some(doc) = point.docstring
                    {
                        out.push_str("  // ");
                        out.push_str(doc);
                    }
                    out.push('\n');
                }
            }
        }
        out
    }
}

#[async_trait]
impl Backend for Stdout {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        let rendered = self.render(points);
        if rendered.is_empty() {
            return Ok(());
        }
        let stderr = self.stderr;

        // A terminal or a pipe can take its time; that must not stall a runtime
        // thread that has other work to do.
        match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::task::spawn_blocking(move || print(&rendered, stderr))
                .await
                .map_err(BackendError::write)?,
            Err(_) => print(&rendered, stderr),
        }
        .map_err(BackendError::write)
    }

    fn name(&self) -> &'static str {
        "stdout"
    }
}

fn print(rendered: &str, stderr: bool) -> std::io::Result<()> {
    if stderr {
        let mut handle = std::io::stderr().lock();
        handle.write_all(rendered.as_bytes())?;
        handle.flush()
    } else {
        let mut handle = std::io::stdout().lock();
        handle.write_all(rendered.as_bytes())?;
        handle.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn points() -> Vec<DataPoint> {
        vec![
            DataPoint::new("requests")
                .with_tag("host", "a")
                .with_field("count", 1i64),
            DataPoint::new("latency").with_field("ratio", 0.25f64),
        ]
    }

    #[test]
    fn human_format_marks_fields() {
        let rendered = Stdout::new().with_prefix("> ").render(&points());
        assert_eq!(
            rendered,
            "> requests host=a ?count=1\n> latency ?ratio=0.25\n"
        );
    }

    #[test]
    fn line_protocol_format_prefixes_every_line() {
        let rendered = Stdout::line_protocol().with_prefix("| ").render(&points());
        let lines: Vec<_> = rendered.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].starts_with("| requests,host=a count=1i "),
            "got {}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("| latency ratio=0.25 "),
            "got {}",
            lines[1]
        );
    }

    #[test]
    fn docstrings_can_be_hidden() {
        let mut point = DataPoint::new("requests").with_field("count", 1i64);
        point.docstring = Some("how many");
        let with = Stdout::new().render(std::slice::from_ref(&point));
        let without = Stdout::new()
            .with_docstrings(false)
            .render(std::slice::from_ref(&point));
        assert_eq!(with, "requests ?count=1  // how many\n");
        assert_eq!(without, "requests ?count=1\n");
    }
}
