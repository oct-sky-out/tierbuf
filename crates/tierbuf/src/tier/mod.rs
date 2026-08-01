//! Storage-tier interfaces and implementations.
//!
//! A tier stores fixed-size pages at backend-specific byte offsets. The
//! synchronous interface is intentionally runtime-independent; asynchronous
//! submission can be layered over backends that expose a raw file descriptor.

use std::fmt;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::atomic::try_update_u64;
use crate::{Result, TierBufError};

pub mod envelope;
pub mod file;
pub mod mock;
#[cfg(feature = "s3")]
pub mod s3;

const NANOS_PER_DAY: u128 = 86_400 * 1_000_000_000;

/// A byte offset identifying a fixed-size page within a storage tier.
///
/// Backends may reject offsets that are not aligned to the crate's page size.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct TierOffset(u64);

impl TierOffset {
    /// Creates an offset from an absolute byte position within a tier.
    #[must_use]
    pub const fn new(byte_offset: u64) -> Self {
        Self(byte_offset)
    }

    /// Returns the absolute byte position represented by this offset.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for TierOffset {
    fn from(byte_offset: u64) -> Self {
        Self::new(byte_offset)
    }
}

impl From<TierOffset> for u64 {
    fn from(offset: TierOffset) -> Self {
        offset.get()
    }
}

impl fmt::Display for TierOffset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A backend's representative latency and sequential-throughput properties.
///
/// These values describe policy inputs rather than hard service guarantees.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LatencyProfile {
    /// Representative page-read latency in microseconds.
    pub read_us_p50: u64,
    /// Representative page-write latency in microseconds.
    pub write_us_p50: u64,
    /// Representative sequential throughput in gibibytes per second.
    pub seq_gbps: f64,
}

impl LatencyProfile {
    /// Creates a latency profile.
    #[must_use]
    pub const fn new(read_us_p50: u64, write_us_p50: u64, seq_gbps: f64) -> Self {
        Self {
            read_us_p50,
            write_us_p50,
            seq_gbps,
        }
    }
}

/// Per-request monetary costs charged by a storage tier.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RequestCosts {
    /// Dollars charged per read request.
    pub read_usd: f64,
    /// Dollars charged per write request.
    pub write_usd: f64,
}

/// A deterministic token bucket limiting bytes written to a storage tier.
///
/// The bucket starts full. Its capacity is one day's write allowance, and
/// [`Self::tick`] refills that allowance uniformly over 24 hours. No wall
/// clock is consulted, making policy and failure tests reproducible.
#[derive(Debug)]
pub struct WriteBudget {
    daily_allowance_bytes: u64,
    available_bytes: AtomicU64,
    refill_remainder: AtomicU64,
}

impl WriteBudget {
    /// Creates a full bucket from device capacity and drive-writes-per-day.
    ///
    /// This is an alias for [`Self::from_dwpd`].
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source if `dwpd` is
    /// negative, NaN, or infinite.
    pub fn new(device_capacity_bytes: u64, dwpd: f64) -> Result<Self> {
        Self::from_dwpd(device_capacity_bytes, dwpd)
    }

    /// Creates a full bucket with the given daily write allowance.
    ///
    /// An allowance of zero creates a budget that rejects every non-empty
    /// write.
    #[must_use]
    pub const fn from_daily_allowance(daily_allowance_bytes: u64) -> Self {
        Self {
            daily_allowance_bytes,
            available_bytes: AtomicU64::new(daily_allowance_bytes),
            refill_remainder: AtomicU64::new(0),
        }
    }

    /// Creates a full bucket from device capacity and drive-writes-per-day.
    ///
    /// The resulting daily allowance is
    /// `floor(device_capacity_bytes * dwpd)`, saturated to `u64::MAX`.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source if `dwpd` is
    /// negative, NaN, or infinite.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    pub fn from_dwpd(device_capacity_bytes: u64, dwpd: f64) -> Result<Self> {
        if !dwpd.is_finite() || dwpd < 0.0 {
            return Err(invalid_input(
                "DWPD must be finite and greater than or equal to zero",
            ));
        }

        let allowance = (device_capacity_bytes as f64 * dwpd)
            .floor()
            .min(u64::MAX as f64) as u64;
        Ok(Self::from_daily_allowance(allowance))
    }

    /// Attempts to consume `bytes` from the current allowance.
    ///
    /// Returns `false` without changing the bucket when insufficient tokens
    /// are available. Consuming zero bytes always succeeds.
    pub fn try_consume(&self, bytes: u64) -> bool {
        try_update_u64(
            &self.available_bytes,
            Ordering::AcqRel,
            Ordering::Acquire,
            |available| available.checked_sub(bytes),
        )
        .is_ok()
    }

    /// Advances the deterministic refill clock by `elapsed`.
    ///
    /// Fractional-byte credit is retained across calls, so many small ticks
    /// refill the same total number of bytes as one equivalent large tick.
    pub fn tick(&self, elapsed: Duration) {
        if self.daily_allowance_bytes == 0 || elapsed.is_zero() {
            return;
        }

        let elapsed_nanos = elapsed.as_nanos();
        let includes_full_day = elapsed_nanos >= NANOS_PER_DAY;
        let partial_nanos = elapsed_nanos % NANOS_PER_DAY;
        let partial_credit = partial_nanos * u128::from(self.daily_allowance_bytes);

        let added = loop {
            let old_remainder = self.refill_remainder.load(Ordering::Acquire);
            let credit = partial_credit + u128::from(old_remainder);
            let partial_bytes = credit / NANOS_PER_DAY;
            let new_remainder = (credit % NANOS_PER_DAY) as u64;
            let refill_bytes = if includes_full_day {
                self.daily_allowance_bytes
            } else {
                u64::try_from(partial_bytes).unwrap_or(self.daily_allowance_bytes)
            };

            if self
                .refill_remainder
                .compare_exchange_weak(
                    old_remainder,
                    new_remainder,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                break refill_bytes;
            }
        };

        if added == 0 {
            return;
        }

        let _ = try_update_u64(
            &self.available_bytes,
            Ordering::AcqRel,
            Ordering::Acquire,
            |available| {
                Some(
                    available
                        .saturating_add(added)
                        .min(self.daily_allowance_bytes),
                )
            },
        );
    }

    /// Returns the maximum number of tokens held by the bucket.
    #[must_use]
    pub const fn capacity_bytes(&self) -> u64 {
        self.daily_allowance_bytes
    }

    /// Returns a snapshot of the currently available write tokens.
    #[must_use]
    pub fn available_bytes(&self) -> u64 {
        self.available_bytes.load(Ordering::Acquire)
    }
}

/// A synchronous fixed-page storage backend.
///
/// Implementations must be safe to call concurrently. `read` and `write`
/// accept only one complete page; implementations should reject other buffer
/// sizes instead of silently truncating or padding them.
pub trait TierBackend: Send + Sync + 'static {
    /// Returns the stable human-readable name of this tier.
    fn name(&self) -> &str;

    /// Reads one page at `location` into `buffer`.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific error when the offset is invalid, the page
    /// does not exist, the buffer is not exactly one page, or I/O fails.
    fn read(&self, location: TierOffset, buffer: &mut [u8]) -> Result<()>;

    /// Writes one page and returns its allocated byte offset.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific error when the buffer is not exactly one
    /// page, the tier has no free page slot, or I/O fails.
    fn write(&self, buffer: &[u8]) -> Result<TierOffset>;

    /// Releases a page slot.
    ///
    /// Invalid or already-free locations are ignored, making cleanup
    /// idempotent.
    fn free(&self, location: TierOffset);

    /// Returns the configured storage capacity in bytes.
    fn capacity_bytes(&self) -> u64;

    /// Returns the currently allocated storage in bytes.
    fn used_bytes(&self) -> u64;

    /// Returns this tier's storage price in dollars per GiB-month.
    fn price_gb_month(&self) -> f64;

    /// Returns per-request costs charged by this tier.
    ///
    /// Local tiers normally keep the zero-cost default.
    fn request_costs(&self) -> RequestCosts {
        RequestCosts::default()
    }

    /// Returns this tier's write-endurance budget, if one is enforced.
    fn write_budget(&self) -> Option<&WriteBudget>;

    /// Returns representative latency and throughput policy inputs.
    fn latency(&self) -> LatencyProfile;

    /// Returns a file descriptor suitable for direct asynchronous reads.
    ///
    /// Backends without a stable file descriptor return `None`.
    fn raw_fd(&self) -> Option<RawFd> {
        None
    }
}

fn invalid_input(message: &'static str) -> TierBufError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use super::WriteBudget;

    #[test]
    fn budget_consumes_and_refills_deterministically() {
        const MIB: u64 = 1024 * 1024;
        let budget = WriteBudget::from_daily_allowance(MIB);

        assert!(budget.try_consume(MIB));
        assert!(!budget.try_consume(1));
        assert_eq!(budget.available_bytes(), 0);

        budget.tick(Duration::from_secs(12 * 60 * 60));
        assert_eq!(budget.available_bytes(), MIB / 2);
        assert!(budget.try_consume(MIB / 2));
        assert_eq!(budget.available_bytes(), 0);

        budget.tick(Duration::from_secs(24 * 60 * 60));
        assert_eq!(budget.available_bytes(), MIB);
    }

    #[test]
    fn budget_retains_fractional_refill_credit() {
        let budget = WriteBudget::from_daily_allowance(1);
        assert!(budget.try_consume(1));

        budget.tick(Duration::from_secs(12 * 60 * 60));
        assert_eq!(budget.available_bytes(), 0);
        budget.tick(Duration::from_secs(12 * 60 * 60));
        assert_eq!(budget.available_bytes(), 1);
    }

    #[test]
    fn concurrent_consumers_cannot_overdraw_budget() {
        const TOKEN: u64 = 64 * 1024;
        const TOKENS: usize = 4;
        const THREADS: usize = 16;

        let budget = Arc::new(WriteBudget::from_daily_allowance(TOKEN * TOKENS as u64));
        let admitted = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let budget = Arc::clone(&budget);
                let admitted = Arc::clone(&admitted);
                thread::spawn(move || {
                    if budget.try_consume(TOKEN) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("budget worker should finish");
        }

        assert_eq!(admitted.load(Ordering::Relaxed), TOKENS);
        assert_eq!(budget.available_bytes(), 0);
    }

    #[test]
    fn invalid_dwpd_is_rejected() {
        assert!(WriteBudget::from_dwpd(1024, f64::NAN).is_err());
        assert!(WriteBudget::from_dwpd(1024, f64::INFINITY).is_err());
        assert!(WriteBudget::from_dwpd(1024, -0.1).is_err());
    }
}
