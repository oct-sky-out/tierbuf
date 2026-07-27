//! An in-memory storage tier with deterministic fault and latency injection.

use std::collections::HashMap;
use std::io;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use crate::{PAGE_SIZE, Result, TierBufError};

use super::{LatencyProfile, TierBackend, TierOffset, WriteBudget};

/// An in-memory fixed-page backend intended for deterministic tests.
///
/// Pages occupy fixed slots and freed offsets are reused before new offsets
/// are allocated. Reported latency is also used as the artificial delay for
/// reads and writes.
#[derive(Debug)]
pub struct MockTier {
    name: String,
    capacity_bytes: u64,
    slot_count: u64,
    price_gb_month: f64,
    latency: LatencyProfile,
    write_budget: Option<WriteBudget>,
    state: Mutex<MockState>,
}

#[derive(Debug, Default)]
struct MockState {
    pages: HashMap<TierOffset, Box<[u8]>>,
    free_slots: Vec<TierOffset>,
    next_slot: u64,
    read_attempts: u64,
    fail_read_at: Option<u64>,
}

impl MockTier {
    /// Creates a zero-latency, zero-price tier without a write budget.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source when capacity
    /// is zero or is not an exact multiple of the fixed page size.
    pub fn new(capacity_bytes: u64) -> Result<Self> {
        Self::with_options("mock", capacity_bytes, 0.0, LatencyProfile::default(), None)
    }

    /// Creates a fully configured mock tier.
    ///
    /// `latency.read_us_p50` and `latency.write_us_p50` are used as artificial
    /// sleep durations in addition to being returned as policy inputs.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source when capacity
    /// is zero or not page-aligned, the name is empty, or a floating-point
    /// configuration value is negative, NaN, or infinite.
    pub fn with_options(
        name: impl Into<String>,
        capacity_bytes: u64,
        price_gb_month: f64,
        latency: LatencyProfile,
        write_budget: Option<WriteBudget>,
    ) -> Result<Self> {
        let name = name.into();
        let page_size = PAGE_SIZE as u64;
        if capacity_bytes == 0 || !capacity_bytes.is_multiple_of(page_size) {
            return Err(invalid_input(
                "mock tier capacity must be a non-zero multiple of PAGE_SIZE",
            ));
        }
        if name.is_empty() {
            return Err(invalid_input("mock tier name must not be empty"));
        }
        if !price_gb_month.is_finite() || price_gb_month < 0.0 {
            return Err(invalid_input(
                "mock tier price must be finite and greater than or equal to zero",
            ));
        }
        if !latency.seq_gbps.is_finite() || latency.seq_gbps < 0.0 {
            return Err(invalid_input(
                "mock tier throughput must be finite and greater than or equal to zero",
            ));
        }

        Ok(Self {
            name,
            capacity_bytes,
            slot_count: capacity_bytes / page_size,
            price_gb_month,
            latency,
            write_budget,
            state: Mutex::new(MockState::default()),
        })
    }

    /// Configures one read attempt to fail and resets the attempt counter.
    ///
    /// `Some(1)` fails the next read, `Some(2)` fails the read after that, and
    /// `None` disables injection. An injected failure is one-shot.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source for `Some(0)`.
    pub fn fail_nth_read(&self, nth: Option<u64>) -> Result<()> {
        if nth == Some(0) {
            return Err(invalid_input("injected read number must be at least one"));
        }

        let mut state = self.lock_state();
        state.read_attempts = 0;
        state.fail_read_at = nth;
        Ok(())
    }

    fn lock_state(&self) -> MutexGuard<'_, MockState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn delay_read(&self) {
        if self.latency.read_us_p50 != 0 {
            thread::sleep(Duration::from_micros(self.latency.read_us_p50));
        }
    }

    fn delay_write(&self) {
        if self.latency.write_us_p50 != 0 {
            thread::sleep(Duration::from_micros(self.latency.write_us_p50));
        }
    }

    fn validate_location(&self, location: TierOffset) -> Result<()> {
        let page_size = PAGE_SIZE as u64;
        if !location.get().is_multiple_of(page_size) || location.get() >= self.capacity_bytes {
            return Err(invalid_input(
                "mock tier offset must identify an aligned slot within capacity",
            ));
        }
        Ok(())
    }
}

impl TierBackend for MockTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, location: TierOffset, buffer: &mut [u8]) -> Result<()> {
        validate_page_buffer(buffer.len())?;
        self.validate_location(location)?;
        self.delay_read();

        let mut state = self.lock_state();
        state.read_attempts = state.read_attempts.saturating_add(1);
        if state.fail_read_at == Some(state.read_attempts) {
            state.fail_read_at = None;
            return Err(io::Error::other("injected mock tier read failure").into());
        }

        let page = state
            .pages
            .get(&location)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "mock tier page not found"))?;
        buffer.copy_from_slice(page);
        Ok(())
    }

    fn write(&self, buffer: &[u8]) -> Result<TierOffset> {
        validate_page_buffer(buffer.len())?;
        self.delay_write();

        let mut state = self.lock_state();
        let location = if let Some(reused) = state.free_slots.pop() {
            reused
        } else if state.next_slot < self.slot_count {
            let byte_offset = state
                .next_slot
                .checked_mul(PAGE_SIZE as u64)
                .ok_or_else(|| io::Error::other("mock tier offset overflow"))?;
            state.next_slot += 1;
            TierOffset::new(byte_offset)
        } else {
            return Err(
                io::Error::new(io::ErrorKind::StorageFull, "mock tier capacity exhausted").into(),
            );
        };

        let previous = state.pages.insert(location, buffer.into());
        debug_assert!(
            previous.is_none(),
            "allocated mock slot was already occupied"
        );
        Ok(location)
    }

    fn free(&self, location: TierOffset) {
        if self.validate_location(location).is_err() {
            return;
        }

        let mut state = self.lock_state();
        if state.pages.remove(&location).is_some() {
            state.free_slots.push(location);
        }
    }

    fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    fn used_bytes(&self) -> u64 {
        let page_count = self.lock_state().pages.len() as u64;
        page_count * PAGE_SIZE as u64
    }

    fn price_gb_month(&self) -> f64 {
        self.price_gb_month
    }

    fn write_budget(&self) -> Option<&WriteBudget> {
        self.write_budget.as_ref()
    }

    fn latency(&self) -> LatencyProfile {
        self.latency
    }
}

fn validate_page_buffer(length: usize) -> Result<()> {
    if length != PAGE_SIZE {
        return Err(invalid_input("tier I/O buffer length must equal PAGE_SIZE"));
    }
    Ok(())
}

fn invalid_input(message: &'static str) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::{PAGE_SIZE, TierBufError};

    use super::{LatencyProfile, MockTier, TierBackend, TierOffset, WriteBudget};

    fn page(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE]
    }

    fn io_kind(error: TierBufError) -> io::ErrorKind {
        match error {
            TierBufError::Io(source) => source.kind(),
            other => panic!("expected I/O error, got {other:?}"),
        }
    }

    #[test]
    fn write_read_round_trip_and_accounting() {
        let tier = MockTier::with_options(
            "warm",
            (PAGE_SIZE * 2) as u64,
            0.25,
            LatencyProfile::new(0, 0, 3.5),
            None,
        )
        .expect("valid mock tier");
        let expected = page(0xa5);

        let location = tier.write(&expected).expect("page write");
        let mut actual = page(0);
        tier.read(location, &mut actual).expect("page read");

        assert_eq!(location, TierOffset::new(0));
        assert_eq!(actual, expected);
        assert_eq!(tier.name(), "warm");
        assert_eq!(tier.capacity_bytes(), (PAGE_SIZE * 2) as u64);
        assert_eq!(tier.used_bytes(), PAGE_SIZE as u64);
        assert!((tier.price_gb_month() - 0.25).abs() < f64::EPSILON);
        assert_eq!(tier.latency(), LatencyProfile::new(0, 0, 3.5));
        assert!(tier.raw_fd().is_none());
    }

    #[test]
    fn free_makes_page_unreadable_and_slot_is_reused() {
        let tier = MockTier::new((PAGE_SIZE * 2) as u64).expect("valid mock tier");
        let first = tier.write(&page(1)).expect("first write");
        let second = tier.write(&page(2)).expect("second write");

        tier.free(first);
        assert_eq!(tier.used_bytes(), PAGE_SIZE as u64);
        let error = tier
            .read(first, &mut page(0))
            .expect_err("freed page must not be readable");
        assert_eq!(io_kind(error), io::ErrorKind::NotFound);

        let reused = tier.write(&page(3)).expect("reused-slot write");
        assert_eq!(reused, first);
        assert_ne!(reused, second);
        assert_eq!(tier.used_bytes(), (PAGE_SIZE * 2) as u64);
    }

    #[test]
    fn capacity_exhaustion_is_reported_as_storage_full() {
        let tier = MockTier::new(PAGE_SIZE as u64).expect("one-slot mock tier");
        tier.write(&page(1)).expect("first write");

        let error = tier
            .write(&page(2))
            .expect_err("second write must exceed capacity");
        assert_eq!(io_kind(error), io::ErrorKind::StorageFull);
        assert_eq!(tier.used_bytes(), PAGE_SIZE as u64);
    }

    #[test]
    fn invalid_buffer_lengths_and_offsets_are_rejected() {
        let tier = MockTier::new(PAGE_SIZE as u64).expect("one-slot mock tier");

        let write_error = tier
            .write(&vec![0; PAGE_SIZE - 1])
            .expect_err("short write must fail");
        assert_eq!(io_kind(write_error), io::ErrorKind::InvalidInput);

        let mut short = vec![0; PAGE_SIZE - 1];
        let read_error = tier
            .read(TierOffset::new(0), &mut short)
            .expect_err("short read must fail");
        assert_eq!(io_kind(read_error), io::ErrorKind::InvalidInput);

        let mut full = page(0);
        let offset_error = tier
            .read(TierOffset::new(1), &mut full)
            .expect_err("unaligned offset must fail");
        assert_eq!(io_kind(offset_error), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn nth_read_failure_is_one_shot_and_resettable() {
        let tier = MockTier::new(PAGE_SIZE as u64).expect("one-slot mock tier");
        let location = tier.write(&page(7)).expect("page write");
        let mut buffer = page(0);

        tier.fail_nth_read(Some(2)).expect("valid injection");
        tier.read(location, &mut buffer).expect("first read");
        let injected = tier
            .read(location, &mut buffer)
            .expect_err("second read should fail");
        assert_eq!(io_kind(injected), io::ErrorKind::Other);
        tier.read(location, &mut buffer)
            .expect("failure should be one-shot");

        tier.fail_nth_read(None).expect("disable injection");
        tier.read(location, &mut buffer)
            .expect("disabled injection should not fail");
        assert!(tier.fail_nth_read(Some(0)).is_err());
    }

    #[test]
    fn optional_write_budget_is_exposed_without_implicit_consumption() {
        let tier = MockTier::with_options(
            "budgeted",
            PAGE_SIZE as u64,
            0.0,
            LatencyProfile::default(),
            Some(WriteBudget::from_daily_allowance(PAGE_SIZE as u64)),
        )
        .expect("valid mock tier");

        tier.write(&page(1)).expect("backend write");
        let budget = tier.write_budget().expect("configured budget");
        assert_eq!(budget.available_bytes(), PAGE_SIZE as u64);
        assert!(budget.try_consume(PAGE_SIZE as u64));
        assert!(!budget.try_consume(1));
    }

    #[test]
    fn invalid_mock_configuration_is_rejected() {
        assert!(MockTier::new(0).is_err());
        assert!(MockTier::new((PAGE_SIZE - 1) as u64).is_err());
        assert!(
            MockTier::with_options("", PAGE_SIZE as u64, 0.0, LatencyProfile::default(), None)
                .is_err()
        );
        assert!(
            MockTier::with_options(
                "mock",
                PAGE_SIZE as u64,
                f64::NAN,
                LatencyProfile::default(),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn backend_trait_is_object_safe_and_mock_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<MockTier>();
        let backend: Box<dyn TierBackend> =
            Box::new(MockTier::new(PAGE_SIZE as u64).expect("valid mock tier"));
        assert_eq!(backend.name(), "mock");
    }
}
