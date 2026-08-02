//! With no backend registered, instrumentation must cost as close to nothing as
//! possible — in particular, the recorded expressions are never evaluated.
//!
//! This lives in its own test binary because it depends on the process having
//! no backend registered at all.

use std::sync::atomic::{AtomicUsize, Ordering};

use mensura::{count, measure, timer};

// Tests in a binary run in parallel, so each one counts on its own.
static MEASURED: AtomicUsize = AtomicUsize::new(0);
static TIMED: AtomicUsize = AtomicUsize::new(0);

fn tracked(counter: &AtomicUsize, value: u32) -> u32 {
    counter.fetch_add(1, Ordering::SeqCst);
    value
}

#[test]
fn nothing_is_recorded_or_evaluated() {
    assert!(!mensura::is_registered());

    measure!("m", host = "a", ?value = tracked(&MEASURED, 1));
    count!("m", host = "a", ?value = tracked(&MEASURED, 2));

    assert_eq!(
        MEASURED.load(Ordering::SeqCst),
        0,
        "measure!/count! must not evaluate their arguments with no backend"
    );

    assert!(!mensura::flush());
    assert!(mensura::stats().is_none());
    assert!(!mensura::deregister_global_backend());
}

#[test]
fn a_timer_still_captures_its_values() {
    // A timer has to evaluate eagerly: a backend may be registered before it is
    // dropped. It must simply not blow up when there is nowhere to report.
    let timer = timer!("orphan", operation = "orphan", ?value = tracked(&TIMED, 3));
    assert_eq!(TIMED.load(Ordering::SeqCst), 1);
    assert!(timer.elapsed().as_secs() < 1);
    drop(timer);
}
