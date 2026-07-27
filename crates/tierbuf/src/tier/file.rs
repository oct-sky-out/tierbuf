//! Portable fixed-page file storage with optional Linux direct I/O.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::{PAGE_SIZE, Result, TierBufError};

use super::{LatencyProfile, TierBackend, TierOffset, WriteBudget};

/// Controls whether a file tier requests Linux `O_DIRECT`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DirectIo {
    /// Always use ordinary buffered file I/O.
    Off,

    /// Prefer `O_DIRECT` on Linux and fall back to buffered I/O when opening
    /// the direct file fails. Other operating systems use buffered I/O.
    #[default]
    Preferred,

    /// Require `O_DIRECT` on Linux.
    ///
    /// Opening with this mode on another operating system returns an
    /// [`io::ErrorKind::Unsupported`] error.
    Required,
}

/// Configuration used to open a [`FileTier`].
#[derive(Debug)]
pub struct FileTierConfig {
    /// Stable human-readable tier name.
    pub name: String,

    /// Total fixed-slot capacity. It must be a non-zero multiple of
    /// [`PAGE_SIZE`].
    pub capacity_bytes: u64,

    /// Storage price in dollars per GiB-month.
    pub price_gb_month: f64,

    /// Representative latency and throughput policy inputs.
    pub latency: LatencyProfile,

    /// Optional write-endurance budget.
    pub write_budget: Option<WriteBudget>,

    /// Direct-I/O behavior for the backing file.
    pub direct_io: DirectIo,
}

impl FileTierConfig {
    /// Creates a zero-price configuration that prefers direct I/O.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            name: "file".to_owned(),
            capacity_bytes,
            price_gb_month: 0.0,
            latency: LatencyProfile::default(),
            write_budget: None,
            direct_io: DirectIo::Preferred,
        }
    }
}

/// A fixed-page storage tier backed by one pre-sized file.
///
/// Opening a tier creates or truncates its backing file because v0.1 keeps the
/// logical page directory in memory rather than recovering it from disk. The
/// file is split into [`PAGE_SIZE`]-byte slots; freed slots are reused before
/// the tier grows into a never-used slot.
///
/// I/O and slot metadata changes are serialized under one mutex. This keeps a
/// read from racing with `free` and slot reuse. Later phases may replace this
/// coarse lock with per-slot synchronization without changing the backend
/// contract.
#[derive(Debug)]
pub struct FileTier {
    name: String,
    path: PathBuf,
    file: File,
    capacity_bytes: u64,
    price_gb_month: f64,
    latency: LatencyProfile,
    write_budget: Option<WriteBudget>,
    direct_io_active: bool,
    state: Mutex<SlotState>,
}

#[derive(Debug)]
struct SlotState {
    allocated: Vec<bool>,
    free_slots: Vec<usize>,
    next_slot: usize,
    used_slots: usize,
}

#[repr(C, align(65536))]
struct AlignedPage([u8; PAGE_SIZE]);

impl FileTier {
    /// Opens a file tier with the supplied configuration.
    ///
    /// The backing file is created if necessary, truncated, sized to
    /// `config.capacity_bytes`, and physically preallocated when supported.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] when configuration is invalid, the file
    /// cannot be opened or sized, direct I/O is required on an unsupported
    /// platform, or the slot count cannot be represented by this process.
    pub fn open(path: impl AsRef<Path>, config: FileTierConfig) -> Result<Self> {
        validate_config(&config)?;

        let slot_count_u64 = config.capacity_bytes / PAGE_SIZE as u64;
        let slot_count = usize::try_from(slot_count_u64)
            .map_err(|_| invalid_input("file tier has too many slots for this process"))?;
        let path = path.as_ref().to_path_buf();
        let direct_io = if std::env::var_os("TIERBUF_NO_DIRECT").is_some_and(|value| value == "1") {
            DirectIo::Off
        } else {
            config.direct_io
        };
        let (file, direct_io_active) = open_backing_file(&path, config.capacity_bytes, direct_io)?;

        Ok(Self {
            name: config.name,
            path,
            file,
            capacity_bytes: config.capacity_bytes,
            price_gb_month: config.price_gb_month,
            latency: config.latency,
            write_budget: config.write_budget,
            direct_io_active,
            state: Mutex::new(SlotState {
                allocated: vec![false; slot_count],
                free_slots: Vec::new(),
                next_slot: 0,
                used_slots: 0,
            }),
        })
    }

    /// Opens a tier with default policy metadata and the requested capacity.
    ///
    /// This convenience constructor uses [`DirectIo::Preferred`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::open`].
    pub fn with_capacity(path: impl AsRef<Path>, capacity_bytes: u64) -> Result<Self> {
        Self::open(path, FileTierConfig::new(capacity_bytes))
    }

    /// Returns the path of the backing file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns whether this tier successfully enabled Linux `O_DIRECT`.
    #[must_use]
    pub const fn direct_io_active(&self) -> bool {
        self.direct_io_active
    }

    fn lock_state(&self) -> MutexGuard<'_, SlotState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn slot_for_location(&self, location: TierOffset) -> Result<usize> {
        let offset = location.get();
        let page_size = PAGE_SIZE as u64;
        if !offset.is_multiple_of(page_size) || offset >= self.capacity_bytes {
            return Err(invalid_input(
                "file tier offset must identify an aligned slot within capacity",
            ));
        }

        usize::try_from(offset / page_size)
            .map_err(|_| invalid_input("file tier slot does not fit this process"))
    }

    fn read_page(&self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        if self.direct_io_active {
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            read_exact_at(&self.file, &mut aligned.0, offset)?;
            buffer.copy_from_slice(&aligned.0);
            Ok(())
        } else {
            read_exact_at(&self.file, buffer, offset)
        }
    }

    fn write_page(&self, offset: u64, buffer: &[u8]) -> io::Result<()> {
        if self.direct_io_active {
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            aligned.0.copy_from_slice(buffer);
            write_all_at(&self.file, &aligned.0, offset)
        } else {
            write_all_at(&self.file, buffer, offset)
        }
    }
}

impl TierBackend for FileTier {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, location: TierOffset, buffer: &mut [u8]) -> Result<()> {
        validate_page_buffer(buffer.len())?;
        let slot = self.slot_for_location(location)?;
        let state = self.lock_state();
        if !state.allocated[slot] {
            return Err(
                io::Error::new(io::ErrorKind::NotFound, "file tier page is not allocated").into(),
            );
        }

        self.read_page(location.get(), buffer)?;
        Ok(())
    }

    fn write(&self, buffer: &[u8]) -> Result<TierOffset> {
        validate_page_buffer(buffer.len())?;
        let mut state = self.lock_state();

        let reused_slot = state.free_slots.last().copied();
        let slot = if let Some(slot) = reused_slot {
            slot
        } else if state.next_slot < state.allocated.len() {
            state.next_slot
        } else {
            return Err(
                io::Error::new(io::ErrorKind::StorageFull, "file tier capacity exhausted").into(),
            );
        };

        debug_assert!(!state.allocated[slot], "allocated slot selected for write");
        let offset = (slot as u64)
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| io::Error::other("file tier offset overflow"))?;
        self.write_page(offset, buffer)?;

        if reused_slot.is_some() {
            let removed = state.free_slots.pop();
            debug_assert_eq!(removed, Some(slot));
        } else {
            state.next_slot += 1;
        }
        state.allocated[slot] = true;
        state.used_slots += 1;

        Ok(TierOffset::new(offset))
    }

    fn free(&self, location: TierOffset) {
        let Ok(slot) = self.slot_for_location(location) else {
            return;
        };

        let mut state = self.lock_state();
        if state.allocated[slot] {
            state.allocated[slot] = false;
            state.free_slots.push(slot);
            state.used_slots -= 1;
        }
    }

    fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    fn used_bytes(&self) -> u64 {
        let used_slots = self.lock_state().used_slots as u64;
        used_slots * PAGE_SIZE as u64
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

    fn raw_fd(&self) -> Option<RawFd> {
        Some(self.file.as_raw_fd())
    }
}

fn validate_config(config: &FileTierConfig) -> Result<()> {
    let page_size = PAGE_SIZE as u64;
    if config.capacity_bytes == 0 || !config.capacity_bytes.is_multiple_of(page_size) {
        return Err(invalid_input(
            "file tier capacity must be a non-zero multiple of PAGE_SIZE",
        ));
    }
    if config.name.is_empty() {
        return Err(invalid_input("file tier name must not be empty"));
    }
    if !config.price_gb_month.is_finite() || config.price_gb_month.is_sign_negative() {
        return Err(invalid_input(
            "file tier price must be finite and greater than or equal to zero",
        ));
    }
    if !config.latency.seq_gbps.is_finite() || config.latency.seq_gbps.is_sign_negative() {
        return Err(invalid_input(
            "file tier throughput must be finite and greater than or equal to zero",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_backing_file(
    path: &Path,
    capacity_bytes: u64,
    direct_io: DirectIo,
) -> Result<(File, bool)> {
    match direct_io {
        DirectIo::Off => open_and_size(path, capacity_bytes, false)
            .map(|file| (file, false))
            .map_err(Into::into),
        DirectIo::Required => open_and_size(path, capacity_bytes, true)
            .map(|file| (file, true))
            .map_err(Into::into),
        DirectIo::Preferred => match open_and_size(path, capacity_bytes, true) {
            Ok(file) => Ok((file, true)),
            Err(direct_error) => open_and_size(path, capacity_bytes, false)
                .map(|file| (file, false))
                .map_err(|buffered_error| {
                    io::Error::new(
                        buffered_error.kind(),
                        format!(
                            "direct open failed ({direct_error}); buffered fallback failed \
                             ({buffered_error})"
                        ),
                    )
                    .into()
                }),
        },
    }
}

#[cfg(not(target_os = "linux"))]
fn open_backing_file(
    path: &Path,
    capacity_bytes: u64,
    direct_io: DirectIo,
) -> Result<(File, bool)> {
    if direct_io == DirectIo::Required {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "DirectIo::Required is supported only on Linux",
        )
        .into());
    }

    open_and_size(path, capacity_bytes, false)
        .map(|file| (file, false))
        .map_err(Into::into)
}

fn open_and_size(path: &Path, capacity_bytes: u64, direct: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(true);
    configure_direct_io(&mut options, direct);

    let file = options.open(path)?;
    file.set_len(capacity_bytes)?;
    crate::sys::file::preallocate(&file, capacity_bytes)?;
    Ok(file)
}

#[cfg(target_os = "linux")]
fn configure_direct_io(options: &mut OpenOptions, direct: bool) {
    use std::os::unix::fs::OpenOptionsExt;

    if direct {
        options.custom_flags(libc::O_DIRECT);
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_direct_io(_options: &mut OpenOptions, _direct: bool) {}

fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read_offset = offset
            .checked_add(filled as u64)
            .ok_or_else(|| io::Error::other("file tier read offset overflow"))?;
        match file.read_at(&mut buffer[filled..], read_offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file tier page ended before PAGE_SIZE bytes",
                ));
            }
            Ok(bytes) => filled += bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0;
    while written < buffer.len() {
        let write_offset = offset
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("file tier write offset overflow"))?;
        match file.write_at(&buffer[written..], write_offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "file tier could not write a complete page",
                ));
            }
            Ok(bytes) => written += bytes,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
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
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{PAGE_SIZE, TierBufError};

    use super::{
        DirectIo, FileTier, FileTierConfig, LatencyProfile, TierBackend, TierOffset, WriteBudget,
    };

    static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

    struct TempFilePath(PathBuf);

    impl TempFilePath {
        fn new(label: &str) -> Self {
            let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
            let filename = format!("tierbuf-{label}-{}-{sequence}.bin", std::process::id());
            let path = std::env::temp_dir().join(filename);
            let _ = fs::remove_file(&path);
            Self(path)
        }

        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempFilePath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn buffered_config(slot_count: usize) -> FileTierConfig {
        let mut config = FileTierConfig::new((slot_count * PAGE_SIZE) as u64);
        config.direct_io = DirectIo::Off;
        config
    }

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
    fn one_hundred_slots_round_trip_and_account_exactly() {
        const SLOTS: usize = 100;

        let temp = TempFilePath::new("round-trip");
        let tier =
            FileTier::open(temp.as_path(), buffered_config(SLOTS)).expect("open buffered tier");
        let mut locations = Vec::with_capacity(SLOTS);

        for slot in 0..SLOTS {
            let location = tier.write(&page(slot as u8)).expect("write page");
            assert_eq!(location, TierOffset::new((slot * PAGE_SIZE) as u64));
            locations.push(location);
        }

        assert_eq!(tier.capacity_bytes(), (SLOTS * PAGE_SIZE) as u64);
        assert_eq!(tier.used_bytes(), tier.capacity_bytes());
        assert_eq!(
            fs::metadata(temp.as_path())
                .expect("backing metadata")
                .len(),
            tier.capacity_bytes()
        );

        for (slot, location) in locations.into_iter().enumerate() {
            let mut actual = page(0);
            tier.read(location, &mut actual).expect("read page");
            assert_eq!(actual, page(slot as u8));
        }
    }

    #[test]
    fn freed_slots_are_unreadable_reused_and_accounted_once() {
        let temp = TempFilePath::new("reuse");
        let tier = FileTier::open(temp.as_path(), buffered_config(3)).expect("open buffered tier");
        let first = tier.write(&page(1)).expect("first write");
        let second = tier.write(&page(2)).expect("second write");
        let third = tier.write(&page(3)).expect("third write");

        tier.free(first);
        tier.free(first);
        assert_eq!(tier.used_bytes(), (2 * PAGE_SIZE) as u64);
        let error = tier
            .read(first, &mut page(0))
            .expect_err("freed slot must be unreadable");
        assert_eq!(io_kind(error), io::ErrorKind::NotFound);

        let reused = tier.write(&page(9)).expect("reuse write");
        assert_eq!(reused, first);
        assert_ne!(reused, second);
        assert_ne!(reused, third);
        assert_eq!(tier.used_bytes(), (3 * PAGE_SIZE) as u64);

        let mut actual = page(0);
        tier.read(reused, &mut actual).expect("read reused slot");
        assert_eq!(actual, page(9));
    }

    #[test]
    fn exhaustion_is_storage_full_and_does_not_change_accounting() {
        let temp = TempFilePath::new("exhaustion");
        let tier = FileTier::open(temp.as_path(), buffered_config(1)).expect("open buffered tier");
        tier.write(&page(1)).expect("first write");

        let error = tier
            .write(&page(2))
            .expect_err("second write must exceed capacity");
        assert_eq!(io_kind(error), io::ErrorKind::StorageFull);
        assert_eq!(tier.used_bytes(), PAGE_SIZE as u64);
    }

    #[test]
    fn invalid_lengths_offsets_and_config_are_rejected() {
        let temp = TempFilePath::new("validation");
        let tier = FileTier::open(temp.as_path(), buffered_config(1)).expect("open buffered tier");

        assert_eq!(
            io_kind(tier.write(&[0; 8]).expect_err("short write")),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            io_kind(
                tier.read(TierOffset::new(1), &mut page(0))
                    .expect_err("unaligned offset")
            ),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            io_kind(
                tier.read(TierOffset::new(PAGE_SIZE as u64), &mut page(0))
                    .expect_err("out-of-range offset")
            ),
            io::ErrorKind::InvalidInput
        );

        let invalid_path = TempFilePath::new("invalid-config");
        let error = FileTier::open(invalid_path.as_path(), buffered_config(0))
            .expect_err("zero capacity must fail");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn metadata_budget_and_raw_fd_match_configuration() {
        let temp = TempFilePath::new("metadata");
        let mut config = buffered_config(2);
        config.name = "nvme".to_owned();
        config.price_gb_month = 0.17;
        config.latency = LatencyProfile::new(80, 120, 3.25);
        config.write_budget = Some(WriteBudget::from_daily_allowance(4 * PAGE_SIZE as u64));

        let tier = FileTier::open(temp.as_path(), config).expect("open configured tier");
        assert_eq!(tier.name(), "nvme");
        assert_eq!(tier.path(), temp.as_path());
        assert!(!tier.direct_io_active());
        assert_eq!(tier.price_gb_month(), 0.17);
        assert_eq!(tier.latency(), LatencyProfile::new(80, 120, 3.25));
        assert_eq!(
            tier.write_budget()
                .expect("configured budget")
                .capacity_bytes(),
            4 * PAGE_SIZE as u64
        );
        assert!(tier.raw_fd().is_some());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn required_direct_io_is_unsupported_off_linux() {
        let temp = TempFilePath::new("required-direct");
        let mut config = buffered_config(1);
        config.direct_io = DirectIo::Required;

        let error =
            FileTier::open(temp.as_path(), config).expect_err("required direct I/O must fail");
        assert_eq!(io_kind(error), io::ErrorKind::Unsupported);
        assert!(!temp.as_path().exists());
    }
}
