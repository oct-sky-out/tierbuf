//! A compact hybrid latch for shared, exclusive, and optimistic access.
//!
//! The latch stores a 48-bit version and a 16-bit lock state in one atomic
//! word. Shared and exclusive access use RAII guards. Optimistic access only
//! snapshots and validates the version; it does not itself make concurrent
//! access to non-atomic page bytes safe.

use std::error::Error;
use std::fmt;
use std::hint;
use std::sync::atomic::{self, AtomicU64, Ordering};
use std::thread;

const STATE_BITS: u32 = 16;
const STATE_MASK: u64 = u16::MAX as u64;
const VERSION_MASK: u64 = (1_u64 << 48) - 1;
const EXCLUSIVE_STATE: u16 = u16::MAX;
const MAX_SHARED: u16 = EXCLUSIVE_STATE - 1;
const SPIN_LIMIT: u32 = 64;

/// Indicates that an optimistic snapshot cannot start while a writer holds the
/// latch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Contended;

impl fmt::Display for Contended {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the latch is exclusively locked")
    }
}

impl Error for Contended {}

/// A versioned latch supporting shared, exclusive, and optimistic access.
///
/// The low 16 bits contain the lock state: zero is unlocked, `u16::MAX` is
/// exclusively locked, and every other value is the number of shared holders.
/// The upper 48 bits contain a version incremented whenever an exclusive guard
/// is released.
///
/// The latch is reader-preferring and does not promise fairness. Callers must
/// still use a safe representation for optimistically read data: validating a
/// version after concurrently reading ordinary mutable bytes does not make
/// such a data race valid in Rust.
#[derive(Debug)]
pub struct HybridLatch(AtomicU64);

impl HybridLatch {
    /// Creates an unlocked latch at version zero.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Starts an optimistic access attempt and returns its version snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Contended`] when an exclusive holder is active. Shared
    /// holders do not prevent an optimistic snapshot.
    pub fn optimistic(&self) -> Result<u64, Contended> {
        let word = self.0.load(Ordering::Acquire);
        if state(word) == EXCLUSIVE_STATE {
            Err(Contended)
        } else {
            Ok(version(word))
        }
    }

    /// Validates an optimistic version snapshot.
    ///
    /// Validation succeeds only when `snapshot` is an exact 48-bit version,
    /// the current version is identical, and no exclusive holder is active.
    /// The acquire fence orders validation after optimistic reads performed by
    /// the caller. A snapshot can theoretically become valid again after
    /// `2^48` completed exclusive sections because the version wraps.
    #[must_use]
    pub fn validate(&self, snapshot: u64) -> bool {
        if snapshot > VERSION_MASK {
            return false;
        }

        atomic::fence(Ordering::Acquire);
        let word = self.0.load(Ordering::Acquire);
        state(word) != EXCLUSIVE_STATE && version(word) == snapshot
    }

    /// Acquires one shared hold, spinning briefly before yielding the thread.
    ///
    /// The shared count never wraps into the exclusive sentinel. When all
    /// `u16::MAX - 1` shared slots are occupied, acquisition waits for a slot
    /// exactly as it waits for an exclusive holder.
    #[must_use = "dropping the guard immediately releases the shared hold"]
    pub fn lock_shared(&self) -> SharedRaw<'_> {
        let mut backoff = Backoff::new();
        loop {
            if self.try_lock_shared_once() {
                return SharedRaw { latch: self };
            }
            backoff.snooze();
        }
    }

    /// Acquires the exclusive hold, spinning briefly before yielding the
    /// thread.
    #[must_use = "dropping the guard immediately releases the exclusive hold"]
    pub fn lock_exclusive(&self) -> ExclusiveRaw<'_> {
        let mut backoff = Backoff::new();
        loop {
            let current = self.0.load(Ordering::Relaxed);
            if state(current) == 0 {
                let desired = with_state(current, EXCLUSIVE_STATE);
                if self
                    .0
                    .compare_exchange_weak(current, desired, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return ExclusiveRaw { latch: self };
                }
            }
            backoff.snooze();
        }
    }

    /// Attempts to atomically upgrade a shared guard to an exclusive guard.
    ///
    /// The upgrade succeeds only when `shared` is the latch's sole shared
    /// holder at the linearization point. On failure, the original shared
    /// guard is returned without releasing it.
    ///
    /// # Errors
    ///
    /// Returns the original `shared` guard when another shared holder exists
    /// or the latch state changes concurrently.
    pub fn try_upgrade(shared: SharedRaw<'_>) -> Result<ExclusiveRaw<'_>, SharedRaw<'_>> {
        let latch = shared.latch;
        let current = latch.0.load(Ordering::Relaxed);
        if state(current) != 1 {
            return Err(shared);
        }

        let desired = with_state(current, EXCLUSIVE_STATE);
        if latch
            .0
            .compare_exchange(current, desired, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // The single shared hold represented by `shared` was atomically
            // replaced with the exclusive sentinel, so its Drop must not
            // decrement the shared count.
            std::mem::forget(shared);
            Ok(ExclusiveRaw { latch })
        } else {
            Err(shared)
        }
    }

    fn try_lock_shared_once(&self) -> bool {
        let current = self.0.load(Ordering::Relaxed);
        let current_state = state(current);
        if current_state >= MAX_SHARED {
            return false;
        }

        self.0
            .compare_exchange_weak(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    fn release_shared(&self) {
        let previous = self.0.fetch_sub(1, Ordering::Release);
        debug_assert!(
            (1..=MAX_SHARED).contains(&state(previous)),
            "shared guard released an invalid latch state"
        );
    }

    fn release_exclusive(&self) {
        let current = self.0.load(Ordering::Relaxed);
        debug_assert_eq!(
            state(current),
            EXCLUSIVE_STATE,
            "exclusive guard released an invalid latch state"
        );

        let next_version = version(current).wrapping_add(1) & VERSION_MASK;
        self.0.store(pack(next_version, 0), Ordering::Release);
    }
}

impl Default for HybridLatch {
    fn default() -> Self {
        Self::new()
    }
}

/// An RAII shared hold acquired from [`HybridLatch::lock_shared`].
///
/// Dropping this guard decrements the latch's shared-holder count.
#[derive(Debug)]
pub struct SharedRaw<'a> {
    latch: &'a HybridLatch,
}

impl Drop for SharedRaw<'_> {
    fn drop(&mut self) {
        self.latch.release_shared();
    }
}

/// An RAII exclusive hold acquired from [`HybridLatch::lock_exclusive`].
///
/// Dropping this guard releases exclusivity and advances the 48-bit version.
#[derive(Debug)]
pub struct ExclusiveRaw<'a> {
    latch: &'a HybridLatch,
}

impl Drop for ExclusiveRaw<'_> {
    fn drop(&mut self) {
        self.latch.release_exclusive();
    }
}

#[derive(Debug)]
struct Backoff {
    attempts: u32,
}

impl Backoff {
    const fn new() -> Self {
        Self { attempts: 0 }
    }

    fn snooze(&mut self) {
        if self.attempts < SPIN_LIMIT {
            self.attempts += 1;
            hint::spin_loop();
        } else {
            thread::yield_now();
        }
    }
}

const fn state(word: u64) -> u16 {
    (word & STATE_MASK) as u16
}

const fn version(word: u64) -> u64 {
    word >> STATE_BITS
}

const fn with_state(word: u64, new_state: u16) -> u64 {
    (word & !STATE_MASK) | new_state as u64
}

const fn pack(version: u64, state: u16) -> u64 {
    ((version & VERSION_MASK) << STATE_BITS) | state as u64
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{
        Contended, EXCLUSIVE_STATE, HybridLatch, MAX_SHARED, VERSION_MASK, state, version,
    };

    #[test]
    fn multiple_shared_guards_coexist_and_release() {
        let latch = HybridLatch::new();
        let first = latch.lock_shared();
        let second = latch.lock_shared();

        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 2);
        assert_eq!(latch.optimistic(), Ok(0));

        drop(first);
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 1);
        drop(second);
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 0);
    }

    #[test]
    fn exclusive_guard_is_raii_and_advances_version() {
        let latch = HybridLatch::new();
        let snapshot = latch.optimistic().expect("unlocked latch");

        let exclusive = latch.lock_exclusive();
        assert_eq!(latch.optimistic(), Err(Contended));
        assert!(!latch.validate(snapshot));
        drop(exclusive);

        assert_eq!(version(latch.0.load(Ordering::Relaxed)), 1);
        assert!(!latch.validate(snapshot));
        assert!(latch.validate(1));
    }

    #[test]
    fn shared_holders_do_not_invalidate_optimistic_snapshot() {
        let latch = HybridLatch::new();
        let snapshot = latch.optimistic().expect("unlocked latch");
        let shared = latch.lock_shared();

        assert!(latch.validate(snapshot));
        drop(shared);
        assert!(latch.validate(snapshot));
    }

    #[test]
    fn oversized_snapshot_is_never_accepted() {
        let latch = HybridLatch::new();
        assert!(!latch.validate(VERSION_MASK + 1));
        assert!(!latch.validate(u64::MAX));
    }

    #[test]
    fn successful_upgrade_replaces_shared_hold_and_versions_on_drop() {
        let latch = HybridLatch::new();
        let shared = latch.lock_shared();

        let exclusive = HybridLatch::try_upgrade(shared).expect("sole reader should upgrade");
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), EXCLUSIVE_STATE);
        assert_eq!(latch.optimistic(), Err(Contended));

        drop(exclusive);
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 0);
        assert_eq!(version(latch.0.load(Ordering::Relaxed)), 1);
    }

    #[test]
    fn failed_upgrade_returns_still_held_shared_guard() {
        let latch = HybridLatch::new();
        let first = latch.lock_shared();
        let second = latch.lock_shared();

        let first =
            HybridLatch::try_upgrade(first).expect_err("multiple readers must prevent upgrade");
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 2);

        drop(second);
        let exclusive =
            HybridLatch::try_upgrade(first).expect("remaining sole reader should upgrade");
        drop(exclusive);
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 0);
    }

    #[test]
    fn saturated_shared_count_cannot_overflow_into_exclusive_state() {
        let latch = HybridLatch::new();
        latch.0.store(u64::from(MAX_SHARED), Ordering::Relaxed);

        assert!(!latch.try_lock_shared_once());
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), MAX_SHARED);
    }

    #[test]
    fn exclusive_release_wraps_only_the_48_bit_version() {
        let latch = HybridLatch::new();
        latch
            .0
            .store(super::pack(VERSION_MASK, 0), Ordering::Relaxed);

        drop(latch.lock_exclusive());
        let word = latch.0.load(Ordering::Relaxed);
        assert_eq!(version(word), 0);
        assert_eq!(state(word), 0);
    }

    #[test]
    fn exclusive_waits_for_existing_shared_holder() {
        let latch = Arc::new(HybridLatch::new());
        let shared = latch.lock_shared();
        let started = Arc::new(Barrier::new(2));
        let acquired = Arc::new(AtomicBool::new(false));

        let worker_latch = Arc::clone(&latch);
        let worker_started = Arc::clone(&started);
        let worker_acquired = Arc::clone(&acquired);
        let worker = thread::spawn(move || {
            worker_started.wait();
            let _exclusive = worker_latch.lock_exclusive();
            worker_acquired.store(true, Ordering::Release);
        });

        started.wait();
        for _ in 0..1_000 {
            thread::yield_now();
        }
        assert!(!acquired.load(Ordering::Acquire));

        drop(shared);
        worker.join().expect("exclusive worker should finish");
        assert!(acquired.load(Ordering::Acquire));
    }

    #[test]
    fn exclusive_stress_preserves_mutual_exclusion_and_count() {
        const THREADS: usize = 8;
        #[cfg(not(miri))]
        const ITERATIONS: usize = 100_000;
        #[cfg(miri)]
        const ITERATIONS: usize = 100;

        let latch = Arc::new(HybridLatch::new());
        let inside = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(THREADS));

        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let latch = Arc::clone(&latch);
                let inside = Arc::clone(&inside);
                let count = Arc::clone(&count);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        let _exclusive = latch.lock_exclusive();
                        assert_eq!(
                            inside.fetch_add(1, Ordering::Relaxed),
                            0,
                            "two exclusive holders overlapped"
                        );
                        count.fetch_add(1, Ordering::Relaxed);
                        assert_eq!(inside.fetch_sub(1, Ordering::Relaxed), 1);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("stress worker should finish");
        }

        assert_eq!(count.load(Ordering::Relaxed), (THREADS * ITERATIONS) as u64);
        assert_eq!(inside.load(Ordering::Relaxed), 0);
        assert_eq!(state(latch.0.load(Ordering::Relaxed)), 0);
        assert_eq!(
            version(latch.0.load(Ordering::Relaxed)),
            (THREADS * ITERATIONS) as u64
        );
    }

    #[test]
    fn optimistic_readers_observe_writer_versions_without_panicking() {
        const WRITES: u64 = 50_000;
        const READERS: usize = 4;

        let latch = Arc::new(HybridLatch::new());
        let value = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicBool::new(false));

        let writer_latch = Arc::clone(&latch);
        let writer_value = Arc::clone(&value);
        let writer_finished = Arc::clone(&finished);
        let writer = thread::spawn(move || {
            for next in 1..=WRITES {
                let _exclusive = writer_latch.lock_exclusive();
                writer_value.store(next, Ordering::Relaxed);
            }
            writer_finished.store(true, Ordering::Release);
        });

        let readers: Vec<_> = (0..READERS)
            .map(|_| {
                let latch = Arc::clone(&latch);
                let value = Arc::clone(&value);
                let finished = Arc::clone(&finished);
                thread::spawn(move || {
                    let mut last_valid = 0;
                    while !finished.load(Ordering::Acquire) {
                        let Ok(snapshot) = latch.optimistic() else {
                            thread::yield_now();
                            continue;
                        };
                        let observed = value.load(Ordering::Relaxed);
                        if latch.validate(snapshot) {
                            assert!(observed >= last_valid);
                            last_valid = observed;
                        }
                    }
                    last_valid
                })
            })
            .collect();

        writer.join().expect("writer should finish");
        for reader in readers {
            let observed = reader.join().expect("reader should finish");
            assert!(observed <= WRITES);
        }

        assert_eq!(value.load(Ordering::Relaxed), WRITES);
        assert_eq!(version(latch.0.load(Ordering::Relaxed)), WRITES);
    }
}
