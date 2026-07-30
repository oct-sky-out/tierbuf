//! Portable fixed-page file storage with optional Linux direct I/O.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::{PAGE_SIZE, Result, TierBufError};

use super::envelope::{PageCodec, decode_page, encode_page};
use super::{LatencyProfile, RequestCosts, TierBackend, TierOffset, WriteBudget};

const DIRECT_IO_BLOCK_SIZE: usize = 4096;

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

    /// Per-request monetary costs used by the placement policy.
    pub request_costs: RequestCosts,

    /// Representative latency and throughput policy inputs.
    pub latency: LatencyProfile,

    /// Optional write-endurance budget.
    pub write_budget: Option<WriteBudget>,

    /// Direct-I/O behavior for the backing file.
    pub direct_io: DirectIo,

    /// Codec used for file-slot storage.
    ///
    /// [`PageCodec::None`] preserves the raw fixed-slot representation and
    /// permits Linux `io_uring` reads through [`TierBackend::raw_fd`].
    /// [`PageCodec::Lz4`] stores a page envelope only when the complete
    /// envelope fits inside one slot; otherwise the page is stored raw.
    pub codec: PageCodec,
}

impl FileTierConfig {
    /// Creates a zero-price configuration that prefers direct I/O.
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            name: "file".to_owned(),
            capacity_bytes,
            price_gb_month: 0.0,
            request_costs: RequestCosts::default(),
            latency: LatencyProfile::default(),
            write_budget: None,
            direct_io: DirectIo::Preferred,
            codec: PageCodec::None,
        }
    }
}

/// A fixed-page storage tier backed by one pre-sized file.
///
/// Opening a tier creates or truncates its backing file because tierbuf keeps the
/// logical page directory in memory rather than recovering it from disk. The
/// file is split into [`PAGE_SIZE`]-byte logical slots; freed slots are reused
/// before the tier grows into a never-used slot. With LZ4 enabled, a slot may
/// contain a shorter checksummed envelope, but capacity and [`TierBackend::used_bytes`]
/// remain fixed-slot logical accounting. The buffer manager likewise charges
/// [`PAGE_SIZE`] to the write budget conservatively.
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
    request_costs: RequestCosts,
    latency: LatencyProfile,
    write_budget: Option<WriteBudget>,
    direct_io_active: bool,
    codec: PageCodec,
    state: Mutex<SlotState>,
}

#[derive(Debug)]
struct SlotState {
    storage: Vec<SlotStorage>,
    free_slots: Vec<usize>,
    next_slot: usize,
    used_slots: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SlotStorage {
    #[default]
    Free,
    Raw,
    Envelope {
        stored_len: usize,
    },
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
            request_costs: config.request_costs,
            latency: config.latency,
            write_budget: config.write_budget,
            direct_io_active,
            codec: config.codec,
            state: Mutex::new(SlotState {
                storage: vec![SlotStorage::Free; slot_count],
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

    fn read_raw_page(&self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        if self.direct_io_active {
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            read_exact_at(&self.file, &mut aligned.0, offset)?;
            buffer.copy_from_slice(&aligned.0);
            Ok(())
        } else {
            read_exact_at(&self.file, buffer, offset)
        }
    }

    fn write_raw_page(&self, offset: u64, buffer: &[u8]) -> io::Result<()> {
        if self.direct_io_active {
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            aligned.0.copy_from_slice(buffer);
            write_all_at(&self.file, &aligned.0, offset)
        } else {
            write_all_at(&self.file, buffer, offset)
        }
    }

    fn read_envelope(&self, offset: u64, stored_len: usize, out: &mut [u8]) -> Result<()> {
        if self.direct_io_active {
            let io_len = direct_io_len(stored_len)?;
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            read_exact_at(&self.file, &mut aligned.0[..io_len], offset)?;
            decode_page(&aligned.0[..stored_len], out)
        } else {
            let mut envelope = vec![0; stored_len];
            read_exact_at(&self.file, &mut envelope, offset)?;
            decode_page(&envelope, out)
        }
    }

    fn write_envelope(&self, offset: u64, envelope: &[u8]) -> Result<()> {
        if self.direct_io_active {
            let io_len = direct_io_len(envelope.len())?;
            let mut aligned = Box::new(AlignedPage([0; PAGE_SIZE]));
            aligned.0[..envelope.len()].copy_from_slice(envelope);
            write_all_at(&self.file, &aligned.0[..io_len], offset)?;
        } else {
            write_all_at(&self.file, envelope, offset)?;
        }
        Ok(())
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
        let result = match state.storage[slot] {
            SlotStorage::Free => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file tier page is not allocated",
            )
            .into()),
            SlotStorage::Raw => self
                .read_raw_page(location.get(), buffer)
                .map_err(Into::into),
            SlotStorage::Envelope { stored_len } => {
                self.read_envelope(location.get(), stored_len, buffer)
            }
        };
        drop(state);
        result
    }

    fn write(&self, buffer: &[u8]) -> Result<TierOffset> {
        validate_page_buffer(buffer.len())?;
        let envelope = match self.codec {
            PageCodec::None => None,
            PageCodec::Lz4 => {
                let encoded = encode_page(buffer, PageCodec::Lz4)?;
                envelope_fits_slot(encoded.len()).then_some(encoded)
            }
        };
        let mut state = self.lock_state();

        let reused_slot = state.free_slots.last().copied();
        let slot = if let Some(slot) = reused_slot {
            slot
        } else if state.next_slot < state.storage.len() {
            state.next_slot
        } else {
            return Err(
                io::Error::new(io::ErrorKind::StorageFull, "file tier capacity exhausted").into(),
            );
        };

        debug_assert_eq!(
            state.storage[slot],
            SlotStorage::Free,
            "allocated slot selected for write"
        );
        let offset = (slot as u64)
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| io::Error::other("file tier offset overflow"))?;
        let storage = if let Some(envelope) = envelope.as_deref() {
            self.write_envelope(offset, envelope)?;
            SlotStorage::Envelope {
                stored_len: envelope.len(),
            }
        } else {
            self.write_raw_page(offset, buffer)?;
            SlotStorage::Raw
        };

        if reused_slot.is_some() {
            let removed = state.free_slots.pop();
            debug_assert_eq!(removed, Some(slot));
        } else {
            state.next_slot += 1;
        }
        state.storage[slot] = storage;
        state.used_slots += 1;

        Ok(TierOffset::new(offset))
    }

    fn free(&self, location: TierOffset) {
        let Ok(slot) = self.slot_for_location(location) else {
            return;
        };

        let mut state = self.lock_state();
        if state.storage[slot] != SlotStorage::Free {
            state.storage[slot] = SlotStorage::Free;
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

    fn request_costs(&self) -> RequestCosts {
        self.request_costs
    }

    fn write_budget(&self) -> Option<&WriteBudget> {
        self.write_budget.as_ref()
    }

    fn latency(&self) -> LatencyProfile {
        self.latency
    }

    fn raw_fd(&self) -> Option<RawFd> {
        (self.codec == PageCodec::None).then(|| self.file.as_raw_fd())
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
    if !config.request_costs.read_usd.is_finite()
        || config.request_costs.read_usd.is_sign_negative()
        || !config.request_costs.write_usd.is_finite()
        || config.request_costs.write_usd.is_sign_negative()
    {
        return Err(invalid_input(
            "file tier request costs must be finite and greater than or equal to zero",
        ));
    }
    if !config.latency.seq_gbps.is_finite() || config.latency.seq_gbps.is_sign_negative() {
        return Err(invalid_input(
            "file tier throughput must be finite and greater than or equal to zero",
        ));
    }
    #[cfg(not(feature = "lz4"))]
    if config.codec == PageCodec::Lz4 {
        return Err(invalid_input(
            "FileTier LZ4 compression requires you to enable the `lz4` feature",
        ));
    }
    Ok(())
}

fn envelope_fits_slot(envelope_len: usize) -> bool {
    envelope_len < PAGE_SIZE
}

fn direct_io_len(stored_len: usize) -> Result<usize> {
    if stored_len == 0 || !envelope_fits_slot(stored_len) {
        return Err(invalid_input(
            "compressed file-tier length must fit within one page slot",
        ));
    }
    stored_len
        .checked_add(DIRECT_IO_BLOCK_SIZE - 1)
        .map(|length| length / DIRECT_IO_BLOCK_SIZE * DIRECT_IO_BLOCK_SIZE)
        .filter(|length| *length <= PAGE_SIZE)
        .ok_or_else(|| invalid_input("compressed file-tier I/O length overflow"))
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
    #[cfg(feature = "lz4")]
    use std::fs::OpenOptions;
    use std::io;
    #[cfg(feature = "lz4")]
    use std::os::unix::fs::FileExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{PAGE_SIZE, TierBufError};

    use super::{
        DirectIo, FileTier, FileTierConfig, LatencyProfile, PageCodec, RequestCosts, SlotStorage,
        TierBackend, TierOffset, WriteBudget, direct_io_len, envelope_fits_slot,
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

    fn pseudo_random_page() -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut page = vec![0; PAGE_SIZE];
        for chunk in page.chunks_exact_mut(size_of::<u64>()) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        page
    }

    fn slot_storage(tier: &FileTier, location: TierOffset) -> SlotStorage {
        let slot = usize::try_from(location.get() / PAGE_SIZE as u64).expect("slot index");
        tier.lock_state().storage[slot]
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

        let mut nan_cost = buffered_config(1);
        nan_cost.request_costs.read_usd = f64::NAN;
        let error = FileTier::open(invalid_path.as_path(), nan_cost)
            .expect_err("NaN request cost must fail");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);

        let mut negative_cost = buffered_config(1);
        negative_cost.request_costs.write_usd = -1.0;
        let error = FileTier::open(invalid_path.as_path(), negative_cost)
            .expect_err("negative request cost must fail");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn metadata_budget_and_raw_fd_match_configuration() {
        let temp = TempFilePath::new("metadata");
        let mut config = buffered_config(2);
        config.name = "nvme".to_owned();
        config.price_gb_month = 0.17;
        config.request_costs = RequestCosts {
            read_usd: 1.0e-8,
            write_usd: 2.0e-8,
        };
        config.latency = LatencyProfile::new(80, 120, 3.25);
        config.write_budget = Some(WriteBudget::from_daily_allowance(4 * PAGE_SIZE as u64));

        let tier = FileTier::open(temp.as_path(), config).expect("open configured tier");
        assert_eq!(tier.name(), "nvme");
        assert_eq!(tier.path(), temp.as_path());
        assert!(!tier.direct_io_active());
        assert_eq!(tier.price_gb_month(), 0.17);
        assert_eq!(
            tier.request_costs(),
            RequestCosts {
                read_usd: 1.0e-8,
                write_usd: 2.0e-8,
            }
        );
        assert_eq!(tier.latency(), LatencyProfile::new(80, 120, 3.25));
        assert_eq!(
            tier.write_budget()
                .expect("configured budget")
                .capacity_bytes(),
            4 * PAGE_SIZE as u64
        );
        assert!(tier.raw_fd().is_some());
    }

    #[test]
    fn default_codec_preserves_raw_slot_bytes_and_raw_fd() {
        let temp = TempFilePath::new("raw-default");
        let config = buffered_config(1);
        assert_eq!(config.codec, PageCodec::None);
        let tier = FileTier::open(temp.as_path(), config).expect("open raw tier");
        let expected = pseudo_random_page();

        let location = tier.write(&expected).expect("write raw page");

        assert_eq!(slot_storage(&tier, location), SlotStorage::Raw);
        assert_eq!(
            &fs::read(temp.as_path()).expect("read backing file")[..PAGE_SIZE],
            expected.as_slice()
        );
        assert!(tier.raw_fd().is_some());
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn compressed_slot_round_trips_without_changing_logical_accounting() {
        let temp = TempFilePath::new("compressed");
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;
        let tier = FileTier::open(temp.as_path(), config).expect("open compressed tier");
        let expected = page(0x5a);

        let location = tier.write(&expected).expect("write compressed page");
        let SlotStorage::Envelope { stored_len } = slot_storage(&tier, location) else {
            panic!("compressible page must use an envelope");
        };
        assert!(stored_len < PAGE_SIZE);
        assert_eq!(tier.used_bytes(), PAGE_SIZE as u64);
        assert!(tier.raw_fd().is_none());

        let mut actual = page(0);
        tier.read(location, &mut actual)
            .expect("read compressed page");
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn incompressible_page_falls_back_to_raw_slot() {
        let temp = TempFilePath::new("incompressible");
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;
        let tier = FileTier::open(temp.as_path(), config).expect("open compressed tier");
        let expected = pseudo_random_page();

        let location = tier.write(&expected).expect("write incompressible page");

        assert_eq!(slot_storage(&tier, location), SlotStorage::Raw);
        assert!(tier.raw_fd().is_none());
        let mut actual = page(0);
        tier.read(location, &mut actual).expect("read raw fallback");
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn slot_reuse_replaces_compression_metadata() {
        let temp = TempFilePath::new("compression-reuse");
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;
        let tier = FileTier::open(temp.as_path(), config).expect("open compressed tier");

        let first = tier.write(&page(7)).expect("write compressed page");
        assert!(matches!(
            slot_storage(&tier, first),
            SlotStorage::Envelope { .. }
        ));
        tier.free(first);
        assert_eq!(slot_storage(&tier, first), SlotStorage::Free);

        let expected = pseudo_random_page();
        let reused = tier.write(&expected).expect("reuse slot with raw page");
        assert_eq!(reused, first);
        assert_eq!(slot_storage(&tier, reused), SlotStorage::Raw);

        let mut actual = page(0);
        tier.read(reused, &mut actual).expect("read reused slot");
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn corrupted_compressed_envelope_is_rejected() {
        const ENVELOPE_HEADER_LEN: u64 = 32;

        let temp = TempFilePath::new("compressed-corruption");
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;
        let tier = FileTier::open(temp.as_path(), config).expect("open compressed tier");
        let location = tier.write(&page(3)).expect("write compressed page");
        assert!(matches!(
            slot_storage(&tier, location),
            SlotStorage::Envelope { .. }
        ));

        let backing = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp.as_path())
            .expect("open backing file for corruption");
        let payload_offset = location.get() + ENVELOPE_HEADER_LEN;
        let mut byte = [0_u8; 1];
        assert_eq!(
            backing
                .read_at(&mut byte, payload_offset)
                .expect("read payload byte"),
            1
        );
        byte[0] ^= 0x80;
        assert_eq!(
            backing
                .write_at(&byte, payload_offset)
                .expect("corrupt payload byte"),
            1
        );

        let error = tier
            .read(location, &mut page(0))
            .expect_err("CRC corruption must fail");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidData);
    }

    #[test]
    fn envelope_fit_is_strict_and_direct_io_lengths_round_to_4k() {
        assert!(envelope_fits_slot(PAGE_SIZE - 1));
        assert!(!envelope_fits_slot(PAGE_SIZE));
        assert!(!envelope_fits_slot(PAGE_SIZE + 1));

        assert_eq!(direct_io_len(1).expect("one byte"), 4096);
        assert_eq!(direct_io_len(4096).expect("one block"), 4096);
        assert_eq!(direct_io_len(4097).expect("two blocks"), 8192);
        assert_eq!(
            direct_io_len(PAGE_SIZE - 1).expect("last fitting length"),
            PAGE_SIZE
        );
        assert_eq!(
            io_kind(direct_io_len(0).expect_err("zero length")),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            io_kind(direct_io_len(PAGE_SIZE).expect_err("full slot does not fit")),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn lz4_file_codec_requires_feature() {
        let temp = TempFilePath::new("lz4-disabled");
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;

        let error =
            FileTier::open(temp.as_path(), config).expect_err("LZ4 feature must be required");

        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);
        let mut config = buffered_config(1);
        config.codec = PageCodec::Lz4;
        let error =
            FileTier::open(temp.as_path(), config).expect_err("LZ4 feature must be required");
        assert!(error.to_string().contains("enable the `lz4` feature"));
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
