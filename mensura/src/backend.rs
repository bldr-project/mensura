//! The backend trait every sink implements.

use std::fmt::{self, Write as _};

use async_trait::async_trait;

use crate::datapoint::DataPoint;

/// Error returned by a [`Backend`] write.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The sink rejected the batch or could not be reached.
    #[error("{0}")]
    Write(String),
    /// One or more backends of a [`Composite`](crate::backends::Composite) failed.
    #[error("{0}")]
    Multiple(MultiError),
    /// Any other error, propagated from a third-party backend implementation.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl BackendError {
    /// Builds a [`BackendError::Write`] from anything printable.
    pub fn write(message: impl fmt::Display) -> Self {
        BackendError::Write(message.to_string())
    }

    /// Builds a [`BackendError::Write`] from an error and its causes.
    ///
    /// HTTP clients tend to say "error sending request" and keep the useful
    /// part — the DNS failure, the refused connection, the TLS complaint — in
    /// the source chain. A log line that stops at the first message is no help.
    pub fn from_source_chain(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut message = error.to_string();
        let mut cause = error.source();
        while let Some(error) = cause {
            let _ = write!(message, ": {error}");
            cause = error.source();
        }
        BackendError::Write(message)
    }

    /// Builds the error of a write that stored part of the batch and lost
    /// `lost` points, so the queue reports those rather than the whole batch.
    pub fn partial(lost: usize, message: impl fmt::Display) -> Self {
        BackendError::Other(Box::new(PartialWrite {
            lost,
            message: message.to_string(),
        }))
    }

    /// How many points of the batch this error lost, when the backend knows;
    /// `None` means the whole batch.
    ///
    /// For a [`BackendError::Multiple`] it is the most any one backend lost,
    /// and `None` if any of them lost the whole batch.
    pub fn points_lost(&self) -> Option<usize> {
        match self {
            BackendError::Write(_) => None,
            BackendError::Multiple(multiple) => {
                multiple.errors.iter().try_fold(0, |most, (_, error)| {
                    error.points_lost().map(|lost| most.max(lost))
                })
            }
            BackendError::Other(error) => error
                .downcast_ref::<PartialWrite>()
                .map(|partial| partial.lost),
        }
    }
}

/// A write that stored part of a batch; see [`BackendError::partial`].
#[derive(Debug)]
pub struct PartialWrite {
    /// How many points of the batch were not stored.
    pub lost: usize,
    /// What the backend said about them.
    pub message: String,
}

impl fmt::Display for PartialWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PartialWrite {}

/// The collected failures of a composite write.
#[derive(Debug)]
pub struct MultiError {
    /// Backend name paired with the error it returned.
    pub errors: Vec<(&'static str, BackendError)>,
}

impl fmt::Display for MultiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} backend(s) failed:", self.errors.len())?;
        for (name, error) in &self.errors {
            write!(f, " [{name}: {error}]")?;
        }
        Ok(())
    }
}

impl std::error::Error for MultiError {}

/// A sink for batches of measurements.
///
/// Implementations receive points in batches assembled by the global queue —
/// see [`Config`](crate::Config) for the batching knobs. `write` is called from
/// a single task, so implementations do not need to be reentrant, but they do
/// need to be `Send + Sync` because that task may move between runtime threads.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Writes a batch of points.
    ///
    /// The batch is passed by reference so a [`Composite`](crate::backends::Composite)
    /// can hand the same slice to several backends without cloning.
    #[allow(clippy::ptr_arg)] // `&Vec<_>` is part of the public contract.
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError>;

    /// Flushes any backend-internal buffering.
    ///
    /// Called when the user asks for a flush and once at shutdown. The queue has
    /// already delivered every pending point via [`Backend::write`] by then, so
    /// the default no-op is correct for backends that do not buffer.
    async fn flush(&self) -> Result<(), BackendError> {
        Ok(())
    }

    /// A short name used in log lines and composite error messages.
    fn name(&self) -> &'static str {
        "backend"
    }
}

#[async_trait]
impl<B: Backend + ?Sized> Backend for Box<B> {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        (**self).write(points).await
    }

    async fn flush(&self) -> Result<(), BackendError> {
        (**self).flush().await
    }

    fn name(&self) -> &'static str {
        (**self).name()
    }
}

#[async_trait]
impl<B: Backend + ?Sized> Backend for std::sync::Arc<B> {
    async fn write(&self, points: &Vec<DataPoint>) -> Result<(), BackendError> {
        (**self).write(points).await
    }

    async fn flush(&self) -> Result<(), BackendError> {
        (**self).flush().await
    }

    fn name(&self) -> &'static str {
        (**self).name()
    }
}
