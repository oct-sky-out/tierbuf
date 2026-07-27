//! Lazy, fixed-point access-heat tracking.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::frame::Frame;

/// Number of fractional bits in the unsigned 8.24 heat representation.
pub const HEAT_FRACTION_BITS: u32 = 24;

/// Raw 8.24 representation of one access per epoch.
pub const HEAT_ONE: u32 = 1 << HEAT_FRACTION_BITS;

const MAX_DECAY_SHIFT: u32 = u32::BITS;

/// Global epoch clock and lock-free per-frame heat updater.
///
/// Each frame stores heat and its last-update epoch together in one
/// `AtomicU64`. Advancing the global epoch touches no frames. The next access
/// lazily right-shifts old heat once per elapsed epoch, then adds one fixed
/// point access with saturation.
#[derive(Debug)]
pub struct HeatTracker {
    epoch: AtomicU32,
}

impl HeatTracker {
    /// Creates a tracker at epoch zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            epoch: AtomicU32::new(0),
        }
    }

    /// Returns the current global epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u32 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Advances the global epoch by one and returns its new value.
    ///
    /// No frame is scanned or modified. The counter wraps naturally after
    /// `u32::MAX`; frame deltas use wrapping subtraction.
    pub fn on_epoch(&self) -> u32 {
        self.epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1)
    }

    /// Lazily decays and increments `frame`, returning its new raw 8.24 heat.
    pub fn on_access(&self, frame: &Frame) -> u32 {
        let epoch = self.current_epoch();
        let mut observed = frame.heat_and_epoch();

        loop {
            let decayed = decay(observed.0, epoch.wrapping_sub(observed.1));
            let replacement = (decayed.saturating_add(HEAT_ONE), epoch);

            match frame.compare_exchange_heat_and_epoch(observed, replacement) {
                Ok(_) => return replacement.0,
                Err(current) => observed = current,
            }
        }
    }

    /// Returns the frame's current lazily decayed raw 8.24 heat.
    ///
    /// This observation does not write the frame or advance its stored epoch.
    #[must_use]
    pub fn current_heat_raw(&self, frame: &Frame) -> u32 {
        let (heat, last_epoch) = frame.heat_and_epoch();
        decay(heat, self.current_epoch().wrapping_sub(last_epoch))
    }

    /// Returns the frame's current heat in accesses per epoch.
    #[must_use]
    pub fn current_heat(&self, frame: &Frame) -> f64 {
        f64::from(self.current_heat_raw(frame)) / f64::from(HEAT_ONE)
    }
}

impl Default for HeatTracker {
    fn default() -> Self {
        Self::new()
    }
}

const fn decay(heat: u32, elapsed_epochs: u32) -> u32 {
    if elapsed_epochs >= MAX_DECAY_SHIFT {
        0
    } else {
        heat >> elapsed_epochs
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{HEAT_ONE, HeatTracker};
    use crate::frame::Frame;

    #[test]
    fn four_epochs_decay_one_access_to_one_sixteenth() {
        let tracker = HeatTracker::new();
        let frame = Frame::new();

        assert_eq!(tracker.on_access(&frame), HEAT_ONE);
        for _ in 0..4 {
            tracker.on_epoch();
        }

        assert_eq!(tracker.current_heat_raw(&frame), HEAT_ONE / 16);
        assert_eq!(frame.heat_and_epoch(), (HEAT_ONE, 0));
        assert_eq!(tracker.on_access(&frame), HEAT_ONE + HEAT_ONE / 16);
        assert_eq!(frame.heat_and_epoch(), (HEAT_ONE + HEAT_ONE / 16, 4));
    }

    #[test]
    fn heat_addition_and_large_decay_saturate() {
        let tracker = HeatTracker::new();
        let frame = Frame::new();

        frame.set_heat_and_epoch(u32::MAX - HEAT_ONE / 2, 0);
        assert_eq!(tracker.on_access(&frame), u32::MAX);

        for _ in 0..u32::BITS {
            tracker.on_epoch();
        }
        assert_eq!(tracker.current_heat_raw(&frame), 0);
        assert_eq!(tracker.on_access(&frame), HEAT_ONE);
    }

    #[test]
    fn eight_threads_do_not_lose_concurrent_accesses() {
        const THREADS: usize = 8;
        const ACCESSES_PER_THREAD: usize = 16;

        let tracker = Arc::new(HeatTracker::new());
        let frame = Arc::new(Frame::new());
        let start = Arc::new(Barrier::new(THREADS));

        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let tracker = Arc::clone(&tracker);
                let frame = Arc::clone(&frame);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..ACCESSES_PER_THREAD {
                        tracker.on_access(&frame);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("heat worker should finish");
        }

        assert_eq!(
            tracker.current_heat_raw(&frame),
            (THREADS * ACCESSES_PER_THREAD) as u32 * HEAT_ONE
        );
        assert_eq!(frame.heat_and_epoch().1, 0);
    }
}
