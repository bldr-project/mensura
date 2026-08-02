//! A backend that fans batches out to several other backends.

use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::{Backend, BackendError, MultiError};
use crate::datapoint::DataPoint;

/// Dispatches every batch to a set of backends.
///
/// Each inner backend sees the same `&Vec<DataPoint>`, so fanning out costs no
/// copies. A failing backend does not prevent the others from receiving the
/// batch; all failures are reported together as
/// [`BackendError::Multiple`].
///
/// ```
/// use mensura::backends::{Composite, Stdout};
///
/// let backend = Composite::new()
///     .with(Stdout::new())
///     .with(Stdout::line_protocol().to_stderr());
/// assert_eq!(backend.len(), 2);
/// ```
#[derive(Clone, Default)]
pub struct Composite {
    backends: Vec<Arc<dyn Backend>>,
}

impl Composite {
    /// An empty composite. Writing to it succeeds and does nothing.
    pub fn new() -> Self {
        Composite::default()
    }

    /// Adds a backend.
    pub fn with(mut self, backend: impl Backend) -> Self {
        self.push(backend);
        self
    }

    /// Adds a backend that is already reference counted.
    pub fn with_arc(mut self, backend: Arc<dyn Backend>) -> Self {
        self.backends.push(backend);
        self
    }

    /// Adds a backend in place.
    pub fn push(&mut self, backend: impl Backend) {
        let backend: Arc<dyn Backend> = Arc::new(backend);
        self.backends.push(backend);
    }

    /// Number of backends in the fan-out.
    pub fn len(&self) -> usize {
        self.backends.len()
    }

    /// True if no backend has been added.
    pub fn is_empty(&self) -> bool {
        self.backends.is_empty()
    }
}

impl std::fmt::Debug for Composite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composite")
            .field(
                "backends",
                &self.backends.iter().map(|b| b.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl FromIterator<Arc<dyn Backend>> for Composite {
    fn from_iter<I: IntoIterator<Item = Arc<dyn Backend>>>(iter: I) -> Self {
        Composite {
            backends: iter.into_iter().collect(),
        }
    }
}

#[async_trait]
impl Backend for Composite {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        let mut errors = Vec::new();
        for backend in &self.backends {
            if let Err(error) = backend.write(points).await {
                errors.push((backend.name(), error));
            }
        }
        collect(errors)
    }

    async fn flush(&self) -> Result<(), BackendError> {
        let mut errors = Vec::new();
        for backend in &self.backends {
            if let Err(error) = backend.flush().await {
                errors.push((backend.name(), error));
            }
        }
        collect(errors)
    }

    fn name(&self) -> &'static str {
        "composite"
    }
}

fn collect(errors: Vec<(&'static str, BackendError)>) -> Result<(), BackendError> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(BackendError::Multiple(MultiError { errors }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Counting {
        writes: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait]
    impl Backend for Counting {
        async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
            self.writes.fetch_add(points.len(), Ordering::Relaxed);
            if self.fail {
                Err(BackendError::write("nope"))
            } else {
                Ok(())
            }
        }

        fn name(&self) -> &'static str {
            "counting"
        }
    }

    #[tokio::test]
    async fn every_backend_sees_the_batch_even_if_one_fails() {
        let a = Arc::new(AtomicUsize::new(0));
        let b = Arc::new(AtomicUsize::new(0));
        let composite = Composite::new()
            .with(Counting {
                writes: Arc::clone(&a),
                fail: true,
            })
            .with(Counting {
                writes: Arc::clone(&b),
                fail: false,
            });

        let points = vec![DataPoint::new("a"), DataPoint::new("b")];
        let error = composite.write(&points).await.unwrap_err();

        assert_eq!(a.load(Ordering::Relaxed), 2);
        assert_eq!(
            b.load(Ordering::Relaxed),
            2,
            "a failure must not short-circuit"
        );
        assert!(error.to_string().contains("counting: nope"), "got {error}");
    }

    #[tokio::test]
    async fn an_empty_composite_succeeds() {
        assert!(
            Composite::new()
                .write(&vec![DataPoint::new("a")])
                .await
                .is_ok()
        );
    }
}
