//! The global, non-blocking queue that carries points from macro call sites to
//! the registered backend.
//!
//! Recording a measurement must never block the caller and must never fail
//! loudly: [`QueueHandle::send`] is a `try_send` that bumps a drop counter when
//! the queue is full. A background worker task drains the queue, assembles
//! batches, and hands them to the [`Backend`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::backend::Backend;
use crate::datapoint::DataPoint;

/// Tuning knobs for the global queue and its batching worker.
#[derive(Clone, Debug)]
pub struct Config {
    /// How many points may be in flight before new ones are dropped.
    pub queue_capacity: usize,
    /// Maximum number of points handed to the backend in one `write` call.
    pub batch_size: usize,
    /// How long a partial batch waits before being written anyway.
    pub flush_interval: Duration,
    /// How long [`deregister_global_backend`] waits for the worker to drain.
    pub shutdown_timeout: Duration,
    /// Whether backend errors are reported on stderr.
    pub log_errors: bool,
    /// How many batches may be written concurrently.
    ///
    /// One writer is enough until the backend becomes the bottleneck: a single
    /// in-flight request means the next batch waits for the round trip of the
    /// last. Raising this lets several requests overlap, at the cost of points
    /// reaching the backend out of order — harmless, since each carries its own
    /// timestamp.
    pub writers: usize,
    /// Run the worker on a thread of its own, even inside a Tokio runtime.
    ///
    /// Sharing an ambient current-thread runtime means the worker cannot make
    /// progress while that thread is blocked — including in
    /// [`flush`] and [`deregister_global_backend`]. A dedicated thread makes
    /// those calls safe from anywhere, at the cost of one thread.
    pub dedicated_thread: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            queue_capacity: 8192,
            batch_size: 512,
            flush_interval: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(5),
            log_errors: true,
            writers: 1,
            dedicated_thread: false,
        }
    }
}

impl Config {
    /// Sets [`Config::queue_capacity`].
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self
    }

    /// Sets [`Config::batch_size`].
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Sets [`Config::flush_interval`].
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Sets [`Config::shutdown_timeout`].
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Sets [`Config::log_errors`].
    pub fn with_error_logging(mut self, enabled: bool) -> Self {
        self.log_errors = enabled;
        self
    }

    /// Sets [`Config::writers`], clamped to at least one.
    pub fn with_writers(mut self, writers: usize) -> Self {
        self.writers = writers.clamp(1, MAX_WRITERS);
        self
    }

    /// Sets [`Config::dedicated_thread`].
    pub fn with_dedicated_thread(mut self, enabled: bool) -> Self {
        self.dedicated_thread = enabled;
        self
    }
}

/// Upper bound on [`Config::writers`]; past this the backend, not the queue,
/// is the thing to fix.
const MAX_WRITERS: usize = 1024;

/// Counters describing what the queue has done since registration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Points accepted into the queue.
    pub sent: u64,
    /// Points dropped because the queue was full or no backend was listening.
    pub dropped: u64,
    /// Batches handed to the backend.
    pub batches: u64,
    /// Batches the backend rejected.
    pub errors: u64,
}

#[derive(Debug, Default)]
struct Counters {
    sent: AtomicU64,
    dropped: AtomicU64,
    batches: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> Stats {
        Stats {
            sent: self.sent.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Acknowledgement channel for a flush request.
enum Ack {
    Sync(std::sync::mpsc::SyncSender<()>),
    Async(tokio::sync::oneshot::Sender<()>),
}

impl Ack {
    fn notify(self) {
        match self {
            Ack::Sync(tx) => {
                let _ = tx.try_send(());
            }
            Ack::Async(tx) => {
                let _ = tx.send(());
            }
        }
    }
}

// `Data` is by far the common case and the tokio channel allocates its slots in
// blocks as they are needed, so boxing here would only add a malloc per point.
#[allow(clippy::large_enum_variant)]
enum Message {
    Data(DataPoint),
    Flush(Ack),
}

/// The sending half of the global queue.
///
/// This is what the macros reach through; see
/// [`GLOBAL_QUEUE`].
#[derive(Clone, Debug)]
pub struct QueueHandle {
    tx: mpsc::Sender<Message>,
    counters: Arc<Counters>,
}

impl QueueHandle {
    /// Enqueues a point, never blocking.
    ///
    /// If the queue is full the point is dropped and [`Stats::dropped`] is
    /// incremented — instrumentation must not be able to stall or crash the
    /// code it measures.
    #[inline]
    pub fn send(&self, point: DataPoint) {
        match self.tx.try_send(Message::Data(point)) {
            Ok(()) => {
                self.counters.sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Blocks until every point enqueued so far has reached the backend.
    ///
    /// Returns `false` on timeout or if the worker is already gone. Do not call
    /// this from an async task — use [`QueueHandle::flush_async`] there.
    pub fn flush_blocking(&self, timeout: Duration) -> bool {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let mut request = Message::Flush(Ack::Sync(tx));
        let deadline = std::time::Instant::now() + timeout;

        // The request goes through the data channel so that it lands behind
        // every point recorded so far. That channel can be full, so keep
        // trying until the caller's timeout runs out.
        loop {
            match self.tx.try_send(request) {
                Ok(()) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    if std::time::Instant::now() >= deadline {
                        return false;
                    }
                    request = returned;
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }

        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        rx.recv_timeout(remaining).is_ok()
    }

    /// Waits until every point enqueued so far has reached the backend.
    pub async fn flush_async(&self) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(Message::Flush(Ack::Async(tx))).await.is_err() {
            return false;
        }
        rx.await.is_ok()
    }

    /// Returns the queue counters.
    pub fn stats(&self) -> Stats {
        self.counters.snapshot()
    }
}

/// Marker type behind the [`GLOBAL_QUEUE`] static that the macros expand to.
///
/// Hidden from the documentation: it exists so that expansions have something
/// to name, and carries no API of its own.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct GlobalQueue;

impl GlobalQueue {
    /// Borrows the registered queue, if there is one.
    ///
    /// The guard is held for the duration of a `send`, which is a couple of
    /// atomics; the write side is only taken when a backend is registered or
    /// deregistered.
    #[inline]
    pub fn get(&self) -> RwLockReadGuard<'static, Option<QueueHandle>> {
        QUEUE
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// True if a backend is currently registered.
    pub fn is_registered(&self) -> bool {
        self.get().is_some()
    }
}

/// The queue the macros reach through.
///
/// Expansions read it as `&*GLOBAL_QUEUE.get()`, which is `None` until a
/// backend is registered. Use [`queue_handle`] to reach the queue by hand.
#[doc(hidden)]
pub static GLOBAL_QUEUE: GlobalQueue = GlobalQueue;

static QUEUE: RwLock<Option<QueueHandle>> = RwLock::new(None);
static REGISTRATION: Mutex<Option<Registration>> = Mutex::new(None);

struct Registration {
    /// Tells the worker to drain and exit. An explicit signal rather than
    /// channel closure, because a user may well be holding a
    /// [`QueueHandle`] of their own — see [`queue_handle`].
    shutdown: Arc<tokio::sync::Notify>,
    /// The owning sender, dropped on shutdown.
    handle: QueueHandle,
    /// Signalled by the worker right before it returns, once for a caller that
    /// blocks and once for a caller that awaits.
    finished: std::sync::mpsc::Receiver<()>,
    finished_async: Arc<tokio::sync::Notify>,
    /// Set when the worker owns a runtime on a thread of our own making.
    thread: Option<std::thread::JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl Registration {
    /// Clears the global queue and tells the worker to drain, returning what is
    /// needed to wait for it.
    fn begin_shutdown(self) -> PendingShutdown {
        *QUEUE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        // Ask the worker to drain and exit, then let go of the sender.
        self.shutdown.notify_one();
        drop(self.handle);
        PendingShutdown {
            finished: self.finished,
            finished_async: self.finished_async,
            thread: self.thread,
            timeout: self.shutdown_timeout,
        }
    }
}

/// A worker that has been asked to stop and not yet confirmed that it has.
struct PendingShutdown {
    finished: std::sync::mpsc::Receiver<()>,
    finished_async: Arc<tokio::sync::Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
    timeout: Duration,
}

impl PendingShutdown {
    /// Reclaims the worker thread, if we made one. It has already signalled, so
    /// this returns straight away.
    fn join(thread: Option<std::thread::JoinHandle<()>>, drained: bool) -> bool {
        if drained && let Some(thread) = thread {
            let _ = thread.join();
        }
        drained
    }
}

/// Error returned when a backend is registered twice.
#[derive(Debug, thiserror::Error)]
#[error("a mensura backend is already registered")]
pub struct AlreadyRegistered;

/// Installs `backend` as the process-wide sink with the default [`Config`].
///
/// ```no_run
/// use mensura::backends::Stdout;
///
/// mensura::register_global_backend(Stdout::new()).expect("backend is already registered");
/// ```
///
/// If called from inside a Tokio runtime the worker is spawned onto it.
/// Otherwise mensura starts a dedicated thread with a small current-thread
/// runtime, so the library also works in plain synchronous programs.
pub fn register_global_backend<B: Backend>(backend: B) -> Result<(), AlreadyRegistered> {
    register_global_backend_with_config(backend, Config::default())
}

/// Installs `backend` as the process-wide sink with an explicit [`Config`].
pub fn register_global_backend_with_config<B: Backend>(
    backend: B,
    config: Config,
) -> Result<(), AlreadyRegistered> {
    let mut registration = lock_registration();
    if registration.is_some() {
        return Err(AlreadyRegistered);
    }

    let counters = Arc::new(Counters::default());
    let (tx, rx) = mpsc::channel(config.queue_capacity);
    let handle = QueueHandle {
        tx,
        counters: Arc::clone(&counters),
    };

    let (finished_tx, finished_rx) = std::sync::mpsc::sync_channel(1);
    let finished_async = Arc::new(tokio::sync::Notify::new());
    let backend: Arc<dyn Backend> = Arc::new(backend);
    let shutdown_timeout = config.shutdown_timeout;
    let dedicated_thread = config.dedicated_thread;
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (dispatcher, writers) = Dispatcher::new(backend, config, counters);
    let shutdown_signal = Arc::clone(&shutdown);
    let worker = async move {
        // Started from inside the runtime that will drive them.
        writers.spawn();
        dispatcher.run(rx, shutdown_signal).await;
    };

    let ambient = if dedicated_thread {
        None
    } else {
        tokio::runtime::Handle::try_current().ok()
    };
    let signal_finished = {
        let finished_async = Arc::clone(&finished_async);
        move || {
            let _ = finished_tx.try_send(());
            // `notify_one` leaves a permit behind, so a waiter that arrives
            // after the worker has already stopped is not left hanging.
            finished_async.notify_one();
        }
    };

    let thread = match ambient {
        Some(runtime) => {
            runtime.spawn(async move {
                worker.await;
                signal_finished();
            });
            None
        }
        None => Some(
            std::thread::Builder::new()
                .name("mensura-worker".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build the mensura worker runtime");
                    runtime.block_on(worker);
                    signal_finished();
                })
                .expect("failed to spawn the mensura worker thread"),
        ),
    };

    *QUEUE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle.clone());
    *registration = Some(Registration {
        shutdown,
        handle,
        finished: finished_rx,
        finished_async,
        thread,
        shutdown_timeout,
    });
    Ok(())
}

/// Removes the registered backend, flushing whatever is still queued.
///
/// **Blocks** until the worker has drained the queue or
/// [`Config::shutdown_timeout`] elapses. Returns `true` if a backend was
/// registered and shut down cleanly.
///
/// From async code call [`deregister_global_backend_async`] instead: this
/// would hold a runtime thread for the whole drain, and on a current-thread
/// runtime shared with the worker it would deadlock until the timeout.
pub fn deregister_global_backend() -> bool {
    let taken = lock_registration().take();
    let Some(registration) = taken else {
        return false;
    };

    let pending = registration.begin_shutdown();
    let drained = pending.finished.recv_timeout(pending.timeout).is_ok();
    PendingShutdown::join(pending.thread, drained)
}

/// Removes the registered backend, flushing whatever is still queued, without
/// blocking the runtime.
///
/// The asynchronous [`deregister_global_backend`]: the drain is awaited rather
/// than waited on, so the thread stays free for other tasks.
///
/// ```no_run
/// # async fn shutdown() {
/// mensura::deregister_global_backend_async().await;
/// # }
/// ```
pub async fn deregister_global_backend_async() -> bool {
    let taken = lock_registration().take();
    let Some(registration) = taken else {
        return false;
    };

    let pending = registration.begin_shutdown();
    let drained = tokio::time::timeout(pending.timeout, pending.finished_async.notified())
        .await
        .is_ok();
    // The worker has signalled, so this join returns immediately; it still goes
    // to the blocking pool rather than stalling a runtime thread on a syscall.
    let thread = pending.thread;
    tokio::task::spawn_blocking(move || PendingShutdown::join(thread, drained))
        .await
        .unwrap_or(drained)
}

/// True if a backend is currently registered.
pub fn is_registered() -> bool {
    GlobalQueue.is_registered()
}

/// Blocks until every measurement recorded so far has reached the backend.
///
/// Returns `false` if no backend is registered or the flush timed out. Call
/// [`flush_async`] instead from inside a Tokio task, so that the runtime
/// thread stays free while the queue drains.
pub fn flush() -> bool {
    flush_with_timeout(Duration::from_secs(5))
}

/// [`flush`] with an explicit timeout.
pub fn flush_with_timeout(timeout: Duration) -> bool {
    queue_handle().is_some_and(|handle| handle.flush_blocking(timeout))
}

/// Asynchronous [`flush`].
pub async fn flush_async() -> bool {
    match queue_handle() {
        Some(handle) => handle.flush_async().await,
        None => false,
    }
}

/// Returns the queue counters, or `None` if no backend is registered.
pub fn stats() -> Option<Stats> {
    GlobalQueue.get().as_ref().map(QueueHandle::stats)
}

/// Clones the registered queue handle, if there is one.
///
/// Holding a handle keeps the counters readable across
/// [`deregister_global_backend`], which is how a program can report what it
/// shipped after shutting the queue down.
pub fn queue_handle() -> Option<QueueHandle> {
    GlobalQueue.get().clone()
}

fn lock_registration() -> std::sync::MutexGuard<'static, Option<Registration>> {
    REGISTRATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Reports a backend failure, and how many measurements went with it.
fn warn(
    counters: &Counters,
    log_errors: bool,
    backend: &str,
    error: &crate::backend::BackendError,
    lost: usize,
) {
    counters.errors.fetch_add(1, Ordering::Relaxed);
    if !log_errors {
        return;
    }
    if lost == 0 {
        eprintln!("mensura: warning: {backend} backend failed: {error}");
    } else {
        eprintln!(
            "mensura: warning: {backend} backend failed, {lost} measurement(s) lost: {error}"
        );
    }
}

/// Reads the queue and forms batches.
///
/// Exactly one dispatcher reads the queue, whatever [`Config::writers`] says.
/// That is what makes batching work: ten points arriving at once become one
/// batch handed to one writer, not ten single-point requests raced between ten
/// tasks. The writers never touch the queue — they only publish what the
/// dispatcher has already grouped.
struct Dispatcher {
    backend: Arc<dyn Backend>,
    config: Config,
    counters: Arc<Counters>,
    batch: Vec<DataPoint>,
    /// Batches handed to the writer pool. Bounded, so that a backend which
    /// cannot keep up eventually makes the dispatcher wait rather than pile up
    /// unbounded work.
    batches: mpsc::Sender<Vec<DataPoint>>,
    /// One message per finished batch, used to know when the writers are idle.
    ///
    /// Unbounded, so that a writer never waits to report: the dispatcher reads
    /// it only between hand-offs, and a writer parked on a full channel here
    /// while the dispatcher is parked on a full `batches` would wait on each
    /// other forever, with every new point dropped. It holds one `()` per
    /// batch written and not yet reaped; the dispatcher's loop reaps them as
    /// they land.
    finished_batches: mpsc::UnboundedReceiver<()>,
    /// Batches handed out and not yet reported finished.
    in_flight: usize,
    /// Value of [`Counters::dropped`] the last time it was reported, so that a
    /// full queue warns once per interval instead of once per lost point.
    reported_drops: u64,
}

impl Dispatcher {
    /// Builds the dispatcher and the writer tasks it feeds.
    fn new(
        backend: Arc<dyn Backend>,
        config: Config,
        counters: Arc<Counters>,
    ) -> (Self, WriterPool) {
        let (batches, batch_rx) = mpsc::channel(config.writers);
        let (finished_tx, finished_batches) = mpsc::unbounded_channel();
        let pool = WriterPool {
            count: config.writers,
            batches: Arc::new(tokio::sync::Mutex::new(batch_rx)),
            finished: finished_tx,
            backend: Arc::clone(&backend),
            counters: Arc::clone(&counters),
            log_errors: config.log_errors,
        };
        let dispatcher = Dispatcher {
            batch: Vec::with_capacity(config.batch_size),
            batches,
            finished_batches,
            in_flight: 0,
            reported_drops: 0,
            backend,
            config,
            counters,
        };
        (dispatcher, pool)
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Message>, shutdown: Arc<tokio::sync::Notify>) {
        let mut ticker = tokio::time::interval(self.config.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately; consume it so an empty queue
        // does not trigger a spurious write.
        ticker.tick().await;

        loop {
            tokio::select! {
                message = rx.recv() => match message {
                    Some(Message::Data(point)) => {
                        self.batch.push(point);
                        if self.batch.len() >= self.config.batch_size {
                            self.dispatch().await;
                        }
                    }
                    Some(Message::Flush(ack)) => {
                        self.dispatch().await;
                        self.await_writers().await;
                        self.flush_backend().await;
                        ack.notify();
                    }
                    // Every sender is gone: the backend is being deregistered.
                    None => break,
                },
                // Reap finished batches as they land, so the count stays small
                // and the channel never backs up.
                Some(()) = self.finished_batches.recv() => self.in_flight -= 1,
                _ = ticker.tick() => {
                    self.dispatch().await;
                    self.report_drops();
                }
                _ = shutdown.notified() => break,
            }
        }

        // Take whatever raced in between the last `recv` and the shutdown.
        let mut pending = Vec::new();
        while let Ok(message) = rx.try_recv() {
            match message {
                Message::Data(point) => self.batch.push(point),
                Message::Flush(ack) => pending.push(ack),
            }
        }
        self.dispatch().await;
        self.await_writers().await;
        self.flush_backend().await;
        self.report_drops();
        // Acknowledged only now: a flush promises that everything queued ahead
        // of it has reached the backend.
        for ack in pending {
            ack.notify();
        }
    }

    /// Hands the current batch to the pool and starts a fresh one.
    ///
    /// Waits only when every writer is busy and the hand-off channel is full,
    /// which is the back-pressure that keeps a slow backend from being handed
    /// unbounded work.
    async fn dispatch(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(self.config.batch_size));
        self.counters.batches.fetch_add(1, Ordering::Relaxed);
        if self.batches.send(batch).await.is_ok() {
            self.in_flight += 1;
        }
    }

    /// Waits for every batch already handed out to be written.
    async fn await_writers(&mut self) {
        while self.in_flight > 0 {
            match self.finished_batches.recv().await {
                Some(()) => self.in_flight -= 1,
                // Every writer is gone; nothing left to wait for.
                None => break,
            }
        }
    }

    async fn flush_backend(&self) {
        if let Err(error) = self.backend.flush().await {
            warn(
                &self.counters,
                self.config.log_errors,
                self.backend.name(),
                &error,
                0,
            );
        }
    }

    /// Reports measurements the queue itself had to drop.
    ///
    /// Points are dropped one at a time when the queue is full, so this reports
    /// the running total per interval rather than warning on each one.
    fn report_drops(&mut self) {
        let dropped = self.counters.dropped.load(Ordering::Relaxed);
        let new_drops = dropped - self.reported_drops;
        self.reported_drops = dropped;
        if new_drops == 0 || !self.config.log_errors {
            return;
        }
        eprintln!(
            "mensura: warning: queue full, {new_drops} measurement(s) dropped \
             ({dropped} in total); raise `Config::queue_capacity`, `Config::writers`, \
             or lower `Config::flush_interval`"
        );
    }
}

/// The tasks that publish batches, and what they need to do it.
struct WriterPool {
    count: usize,
    /// Shared so that a batch goes to whichever writer is free next. The lock
    /// is held only while taking a batch, never while writing one.
    batches: Arc<tokio::sync::Mutex<mpsc::Receiver<Vec<DataPoint>>>>,
    finished: mpsc::UnboundedSender<()>,
    backend: Arc<dyn Backend>,
    counters: Arc<Counters>,
    log_errors: bool,
}

impl WriterPool {
    /// Starts the writers on the current runtime.
    fn spawn(self) {
        for _ in 0..self.count {
            let batches = Arc::clone(&self.batches);
            let finished = self.finished.clone();
            let backend = Arc::clone(&self.backend);
            let counters = Arc::clone(&self.counters);
            let log_errors = self.log_errors;
            tokio::spawn(async move {
                loop {
                    let batch = match batches.lock().await.recv().await {
                        Some(batch) => batch,
                        // The dispatcher is gone.
                        None => break,
                    };
                    if let Err(error) = backend.write(&batch).await {
                        // The batch is not retried: a backend that rejected it
                        // once will reject it again, and holding on would stall
                        // everything behind it. Say plainly how much was lost:
                        // the points the backend names, or else all of them.
                        let lost = error.points_lost().unwrap_or(batch.len());
                        warn(&counters, log_errors, backend.name(), &error, lost);
                    }
                    if finished.send(()).is_err() {
                        break;
                    }
                }
            });
        }
    }
}
