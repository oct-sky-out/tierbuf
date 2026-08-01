//! S3-backed fixed-page storage tier.
//!
//! Each logical page is stored as one immutable, self-describing envelope.
//! Object identifiers are process-wide, monotonically allocated, and never
//! reused. With the required unique prefix per process or dataset generation,
//! a delayed background delete can never remove a later page.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::tier::envelope::{PageCodec, decode_page, encode_page};
use crate::tier::{LatencyProfile, RequestCosts, TierBackend, TierOffset, WriteBudget};
use crate::{PAGE_SIZE, Result, TierBufError};

pub mod client;
pub mod credentials;
pub mod sigv4;

use client::{ObjectApi, S3Client, S3ClientConfig};

const DELETE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
// Object identifiers are process-wide rather than tier-local. A tier can be
// dropped while its delete worker is still draining, so a replacement tier
// using the same prefix must not reuse one of its identifiers.
static NEXT_OBJECT_ID: AtomicU64 = AtomicU64::new(0);

/// Configuration for an S3-backed storage tier.
#[derive(Debug)]
pub struct S3TierConfig {
    /// Stable human-readable tier name.
    pub name: String,
    /// Logical page capacity in bytes; must be a non-zero page-size multiple.
    pub capacity_bytes: u64,
    /// Storage price in dollars per GiB-month.
    pub price_gb_month: f64,
    /// Representative latency and throughput policy inputs.
    pub latency: LatencyProfile,
    /// Per-request monetary costs used by the placement policy.
    pub request_costs: RequestCosts,
    /// Envelope codec used for newly written pages.
    pub codec: PageCodec,
    /// Non-empty bucket-relative prefix unique to one process or dataset generation.
    pub key_prefix: String,
    /// Whether `free` performs DELETE on the calling thread.
    pub synchronous_delete: bool,
    /// Optional write-endurance or administrative write budget.
    pub write_budget: Option<WriteBudget>,
}

impl Default for S3TierConfig {
    fn default() -> Self {
        Self {
            name: "s3".to_owned(),
            capacity_bytes: PAGE_SIZE as u64,
            price_gb_month: 0.023,
            latency: LatencyProfile::new(30_000, 40_000, 0.1),
            request_costs: RequestCosts {
                read_usd: 4.0e-7,
                write_usd: 5.0e-6,
            },
            codec: PageCodec::Lz4,
            key_prefix: String::new(),
            synchronous_delete: false,
            write_budget: None,
        }
    }
}

/// S3-backed implementation of [`TierBackend`].
///
/// Capacity is enforced by logical page count, while [`TierBackend::used_bytes`]
/// reports actual envelope bytes so compression savings are visible to cost
/// accounting.
pub struct S3Tier {
    api: Arc<dyn ObjectApi>,
    name: String,
    capacity_bytes: u64,
    price_gb_month: f64,
    latency: LatencyProfile,
    request_costs: RequestCosts,
    codec: PageCodec,
    key_prefix: String,
    synchronous_delete: bool,
    write_budget: Option<WriteBudget>,
    state: Mutex<TierState>,
    stats: S3TierStatsHandle,
    delete_sender: Option<Sender<String>>,
    delete_worker: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct TierState {
    objects: HashMap<u64, StoredMeta>,
    pending_writes: u64,
}

#[derive(Clone, Copy, Debug)]
struct StoredMeta {
    stored_len: u32,
}

#[derive(Debug, Default)]
struct S3TierStatsInner {
    get_requests: AtomicU64,
    put_requests: AtomicU64,
    delete_requests: AtomicU64,
    request_failures: AtomicU64,
    stored_bytes: AtomicU64,
    logical_bytes: AtomicU64,
    bytes_uploaded: AtomicU64,
    bytes_downloaded: AtomicU64,
}

/// Cloneable handle for observing S3 tier request and compression statistics.
#[derive(Clone, Debug)]
pub struct S3TierStatsHandle {
    inner: Arc<S3TierStatsInner>,
}

/// Point-in-time S3 tier statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct S3TierStats {
    /// Logical GET calls made through the object API, excluding internal retries.
    pub get_requests: u64,
    /// Logical PUT calls made through the object API, excluding internal retries.
    pub put_requests: u64,
    /// Logical DELETE calls made through the object API, excluding internal retries.
    pub delete_requests: u64,
    /// Object API calls that returned a final error.
    pub request_failures: u64,
    /// Current sum of stored envelope lengths.
    pub stored_bytes: u64,
    /// Current logical bytes represented by live pages.
    pub logical_bytes: u64,
    /// Cumulative bytes in successful PUT bodies.
    pub bytes_uploaded: u64,
    /// Cumulative bytes in successful GET bodies.
    pub bytes_downloaded: u64,
}

impl S3TierStatsHandle {
    /// Returns a best-effort point-in-time statistics snapshot.
    ///
    /// Counters are loaded independently and are intended for observability,
    /// not as a transactional accounting record.
    #[must_use]
    pub fn snapshot(&self) -> S3TierStats {
        S3TierStats {
            get_requests: self.inner.get_requests.load(Ordering::Relaxed),
            put_requests: self.inner.put_requests.load(Ordering::Relaxed),
            delete_requests: self.inner.delete_requests.load(Ordering::Relaxed),
            request_failures: self.inner.request_failures.load(Ordering::Relaxed),
            stored_bytes: self.inner.stored_bytes.load(Ordering::Relaxed),
            logical_bytes: self.inner.logical_bytes.load(Ordering::Relaxed),
            bytes_uploaded: self.inner.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: self.inner.bytes_downloaded.load(Ordering::Relaxed),
        }
    }
}

impl S3Tier {
    /// Opens a tier backed by a real S3-compatible HTTP endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when either client or tier configuration is invalid,
    /// or the background delete worker cannot be started.
    pub fn open(client_config: S3ClientConfig, config: S3TierConfig) -> Result<Self> {
        let client = Arc::new(S3Client::new(client_config)?);
        Self::with_object_api(client, config)
    }

    /// Opens a tier over an injected object API.
    ///
    /// This constructor supports deterministic tests and future compatible
    /// object stores without introducing an asynchronous runtime.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::InvalidConfig`] for invalid tier configuration,
    /// or [`TierBufError::Io`] if the delete worker cannot be spawned.
    pub fn with_object_api(api: Arc<dyn ObjectApi>, mut config: S3TierConfig) -> Result<Self> {
        validate_config(&config)?;
        if !config.key_prefix.ends_with('/') {
            config.key_prefix.push('/');
        }

        let stats = S3TierStatsHandle {
            inner: Arc::new(S3TierStatsInner::default()),
        };
        let (delete_sender, delete_worker) = if config.synchronous_delete {
            (None, None)
        } else {
            let (sender, receiver) = channel::<String>();
            let worker_api = Arc::clone(&api);
            let worker_stats = stats.clone();
            let worker = thread::Builder::new()
                .name(format!("tierbuf-{}-delete", config.name))
                .spawn(move || {
                    while let Ok(key) = receiver.recv() {
                        delete_object(&*worker_api, &worker_stats, &key);
                    }
                })
                .map_err(TierBufError::Io)?;
            (Some(sender), Some(worker))
        };

        Ok(Self {
            api,
            name: config.name,
            capacity_bytes: config.capacity_bytes,
            price_gb_month: config.price_gb_month,
            latency: config.latency,
            request_costs: config.request_costs,
            codec: config.codec,
            key_prefix: config.key_prefix,
            synchronous_delete: config.synchronous_delete,
            write_budget: config.write_budget,
            state: Mutex::new(TierState::default()),
            stats,
            delete_sender,
            delete_worker,
        })
    }

    /// Returns a cloneable statistics handle independent of tier ownership.
    #[must_use]
    pub fn stats_handle(&self) -> S3TierStatsHandle {
        self.stats.clone()
    }

    fn lock_state(&self) -> MutexGuard<'_, TierState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn object_key(&self, object_id: u64) -> String {
        format!("{}p{object_id:016x}", self.key_prefix)
    }

    fn object_id_from_location(&self, location: TierOffset) -> Result<u64> {
        let offset = location.get();
        let page_size = PAGE_SIZE as u64;
        if !offset.is_multiple_of(page_size) {
            return Err(invalid_input("s3 tier offset must be aligned to PAGE_SIZE"));
        }
        Ok(offset / page_size)
    }

    fn allocate_object_id(&self) -> Result<u64> {
        let maximum_id = u64::MAX / PAGE_SIZE as u64;
        NEXT_OBJECT_ID
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current <= maximum_id).then(|| current.saturating_add(1))
            })
            .map_err(|_| io::Error::other("s3 tier object identifier space exhausted").into())
    }

    fn reserve_logical_page(&self) -> Result<()> {
        let capacity_pages = self.capacity_bytes / PAGE_SIZE as u64;
        let mut state = self.lock_state();
        let occupied = u64::try_from(state.objects.len())
            .unwrap_or(u64::MAX)
            .saturating_add(state.pending_writes);
        if occupied >= capacity_pages {
            return Err(
                io::Error::new(io::ErrorKind::StorageFull, "s3 tier capacity exhausted").into(),
            );
        }
        state.pending_writes = state.pending_writes.saturating_add(1);
        Ok(())
    }

    fn cancel_reservation(&self) {
        let mut state = self.lock_state();
        state.pending_writes = state.pending_writes.saturating_sub(1);
    }
}

impl TierBackend for S3Tier {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, location: TierOffset, buffer: &mut [u8]) -> Result<()> {
        validate_page_buffer(buffer.len())?;
        let object_id = self.object_id_from_location(location)?;
        let metadata = {
            let state = self.lock_state();
            state.objects.get(&object_id).copied().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "s3 tier page is not allocated")
            })?
        };
        let key = self.object_key(object_id);
        self.stats
            .inner
            .get_requests
            .fetch_add(1, Ordering::Relaxed);
        let envelope = match self.api.get(&key) {
            Ok(envelope) => envelope,
            Err(TierBufError::Io(source)) if source.kind() == io::ErrorKind::NotFound => {
                self.stats
                    .inner
                    .request_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("s3 tier metadata references missing object for id {object_id}"),
                )
                .into());
            }
            Err(error) => {
                self.stats
                    .inner
                    .request_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        self.stats
            .inner
            .bytes_downloaded
            .fetch_add(envelope.len() as u64, Ordering::Relaxed);
        if envelope.len() != metadata.stored_len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "s3 tier object length differs from committed metadata",
            )
            .into());
        }
        decode_page(&envelope, buffer)
    }

    fn write(&self, buffer: &[u8]) -> Result<TierOffset> {
        validate_page_buffer(buffer.len())?;
        let envelope = encode_page(buffer, self.codec)?;
        self.reserve_logical_page()?;
        let object_id = match self.allocate_object_id() {
            Ok(object_id) => object_id,
            Err(error) => {
                self.cancel_reservation();
                return Err(error);
            }
        };
        let key = self.object_key(object_id);
        self.stats
            .inner
            .put_requests
            .fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.api.put(&key, &envelope) {
            self.cancel_reservation();
            self.stats
                .inner
                .request_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }

        let stored_len = match u32::try_from(envelope.len()) {
            Ok(stored_len) => stored_len,
            Err(_) => {
                self.cancel_reservation();
                self.stats
                    .inner
                    .request_failures
                    .fetch_add(1, Ordering::Relaxed);
                let _ = self.api.delete(&key);
                return Err(io::Error::other("s3 envelope length exceeds metadata format").into());
            }
        };
        {
            let mut state = self.lock_state();
            state.pending_writes = state.pending_writes.saturating_sub(1);
            state.objects.insert(object_id, StoredMeta { stored_len });
        }
        let stored_len_u64 = u64::from(stored_len);
        self.stats
            .inner
            .stored_bytes
            .fetch_add(stored_len_u64, Ordering::Relaxed);
        self.stats
            .inner
            .logical_bytes
            .fetch_add(PAGE_SIZE as u64, Ordering::Relaxed);
        self.stats
            .inner
            .bytes_uploaded
            .fetch_add(stored_len_u64, Ordering::Relaxed);

        let offset = object_id
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| io::Error::other("s3 tier offset overflow"))?;
        Ok(TierOffset::new(offset))
    }

    fn free(&self, location: TierOffset) {
        let Ok(object_id) = self.object_id_from_location(location) else {
            return;
        };
        let metadata = self.lock_state().objects.remove(&object_id);
        let Some(metadata) = metadata else {
            return;
        };
        self.stats
            .inner
            .stored_bytes
            .fetch_sub(u64::from(metadata.stored_len), Ordering::Relaxed);
        self.stats
            .inner
            .logical_bytes
            .fetch_sub(PAGE_SIZE as u64, Ordering::Relaxed);

        let key = self.object_key(object_id);
        if self.synchronous_delete {
            delete_object(&*self.api, &self.stats, &key);
        } else if self
            .delete_sender
            .as_ref()
            .is_none_or(|sender| sender.send(key).is_err())
        {
            self.stats
                .inner
                .request_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    fn used_bytes(&self) -> u64 {
        self.stats.inner.stored_bytes.load(Ordering::Relaxed)
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
}

impl Drop for S3Tier {
    fn drop(&mut self) {
        self.delete_sender.take();
        let Some(worker) = self.delete_worker.take() else {
            return;
        };
        let deadline = Instant::now() + DELETE_DRAIN_TIMEOUT;
        while !worker.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        if worker.is_finished() {
            let _ = worker.join();
        }
        // A worker still blocked in object-store retries is detached. Prefix
        // lifecycle policies are the recovery mechanism for such rare orphans.
    }
}

fn delete_object(api: &dyn ObjectApi, stats: &S3TierStatsHandle, key: &str) {
    stats.inner.delete_requests.fetch_add(1, Ordering::Relaxed);
    if api.delete(key).is_err() {
        stats.inner.request_failures.fetch_add(1, Ordering::Relaxed);
    }
}

fn validate_config(config: &S3TierConfig) -> Result<()> {
    if config.name.trim().is_empty() {
        return Err(invalid_config("S3 tier name must not be empty"));
    }
    if config.capacity_bytes == 0 || !config.capacity_bytes.is_multiple_of(PAGE_SIZE as u64) {
        return Err(invalid_config(
            "S3 tier capacity must be a non-zero multiple of PAGE_SIZE",
        ));
    }
    if !config.price_gb_month.is_finite() || config.price_gb_month < 0.0 {
        return Err(invalid_config(
            "S3 tier price must be finite and greater than or equal to zero",
        ));
    }
    if !config.latency.seq_gbps.is_finite() || config.latency.seq_gbps < 0.0 {
        return Err(invalid_config(
            "S3 tier throughput must be finite and greater than or equal to zero",
        ));
    }
    if !config.request_costs.read_usd.is_finite()
        || config.request_costs.read_usd < 0.0
        || !config.request_costs.write_usd.is_finite()
        || config.request_costs.write_usd < 0.0
    {
        return Err(invalid_config(
            "S3 tier request costs must be finite and greater than or equal to zero",
        ));
    }
    if config.key_prefix.is_empty() || config.key_prefix.starts_with('/') {
        return Err(invalid_config(
            "S3 tier key prefix must be non-empty and bucket-relative",
        ));
    }
    #[cfg(not(feature = "lz4"))]
    if config.codec == PageCodec::Lz4 {
        return Err(invalid_config(
            "S3 tier LZ4 codec requires enabling the `lz4` feature",
        ));
    }
    Ok(())
}

fn validate_page_buffer(length: usize) -> Result<()> {
    if length != PAGE_SIZE {
        return Err(invalid_input(
            "s3 tier buffers must contain exactly PAGE_SIZE bytes",
        ));
    }
    Ok(())
}

fn invalid_input(message: &'static str) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

fn invalid_config(message: &'static str) -> TierBufError {
    TierBufError::InvalidConfig(message.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::client::MemoryObjectApi;
    use super::{PageCodec, S3Tier, S3TierConfig};
    use crate::tier::TierBackend;
    use crate::{PAGE_SIZE, TierBufError};

    fn config(capacity_pages: u64, synchronous_delete: bool) -> S3TierConfig {
        S3TierConfig {
            capacity_bytes: capacity_pages * PAGE_SIZE as u64,
            key_prefix: "tests/".to_owned(),
            synchronous_delete,
            codec: PageCodec::None,
            ..S3TierConfig::default()
        }
    }

    fn page(seed: u64) -> Vec<u8> {
        let mut state = seed;
        let mut page = vec![0_u8; PAGE_SIZE];
        for chunk in page.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        page
    }

    #[test]
    fn hundred_pages_round_trip() {
        let api = Arc::new(MemoryObjectApi::new());
        let tier = S3Tier::with_object_api(api, config(100, true)).expect("open memory tier");
        let mut locations = Vec::new();
        for seed in 0..100 {
            let input = page(seed);
            locations.push((tier.write(&input).expect("write"), input));
        }
        assert!(locations.windows(2).all(|pair| pair[0].0 < pair[1].0));
        for (location, input) in locations {
            let mut output = vec![0_u8; PAGE_SIZE];
            tier.read(location, &mut output).expect("read");
            assert_eq!(output, input);
        }
    }

    #[test]
    fn freed_page_is_unreadable_and_ids_are_never_reused() {
        let api = Arc::new(MemoryObjectApi::new());
        let tier = S3Tier::with_object_api(api.clone(), config(2, true)).expect("open memory tier");
        let first = tier.write(&page(1)).expect("first write");
        tier.free(first);
        let mut output = vec![0_u8; PAGE_SIZE];
        assert!(tier.read(first, &mut output).is_err());
        let second = tier.write(&page(2)).expect("second write");
        assert!(second > first);
        let first_key = format!(
            "tests/p{:016x}",
            first.get() / u64::try_from(PAGE_SIZE).expect("PAGE_SIZE fits u64")
        );
        let second_key = format!(
            "tests/p{:016x}",
            second.get() / u64::try_from(PAGE_SIZE).expect("PAGE_SIZE fits u64")
        );
        assert!(!api.contains_key(&first_key));
        assert!(api.contains_key(&second_key));
    }

    #[test]
    fn replacement_tier_does_not_reuse_object_ids() {
        let api = Arc::new(MemoryObjectApi::new());
        let first = {
            let tier =
                S3Tier::with_object_api(api.clone(), config(1, true)).expect("open first tier");
            tier.write(&page(1)).expect("first write")
        };
        let replacement =
            S3Tier::with_object_api(api, config(1, true)).expect("open replacement tier");
        let second = replacement.write(&page(2)).expect("replacement write");
        assert!(second > first);
    }

    #[test]
    fn capacity_uses_logical_pages() {
        let api = Arc::new(MemoryObjectApi::new());
        let mut tier_config = config(2, true);
        tier_config.codec = if cfg!(feature = "lz4") {
            PageCodec::Lz4
        } else {
            PageCodec::None
        };
        let tier = S3Tier::with_object_api(api, tier_config).expect("open memory tier");
        let compressible = vec![7_u8; PAGE_SIZE];
        tier.write(&compressible).expect("first write");
        tier.write(&compressible).expect("second write");
        let error = tier
            .write(&compressible)
            .expect_err("capacity must be full");
        assert!(
            matches!(error, TierBufError::Io(source) if source.kind() == std::io::ErrorKind::StorageFull)
        );
        assert_eq!(
            tier.stats_handle().snapshot().logical_bytes,
            2 * PAGE_SIZE as u64
        );
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn compression_reduces_stored_bytes() {
        let api = Arc::new(MemoryObjectApi::new());
        let mut tier_config = config(2, true);
        tier_config.codec = PageCodec::Lz4;
        let tier = S3Tier::with_object_api(api, tier_config).expect("open memory tier");
        tier.write(&vec![0x55; PAGE_SIZE])
            .expect("compressed write");
        let after_compressible = tier.stats_handle().snapshot();
        assert!(after_compressible.stored_bytes < after_compressible.logical_bytes);
        tier.write(&page(42)).expect("random write");
        let final_stats = tier.stats_handle().snapshot();
        assert!(final_stats.stored_bytes > after_compressible.stored_bytes);
    }

    #[test]
    fn put_failure_leaves_no_state() {
        let api = Arc::new(MemoryObjectApi::new());
        let tier = S3Tier::with_object_api(api.clone(), config(1, true)).expect("open memory tier");
        api.fail_next_put();
        assert!(tier.write(&page(1)).is_err());
        let failed = tier.stats_handle().snapshot();
        assert_eq!(failed.stored_bytes, 0);
        assert_eq!(failed.logical_bytes, 0);
        assert_eq!(api.len(), 0);
        tier.write(&page(2)).expect("next write");
        assert_eq!(
            tier.stats_handle().snapshot().logical_bytes,
            PAGE_SIZE as u64
        );
    }

    #[test]
    fn background_delete_drains() {
        let api = Arc::new(MemoryObjectApi::new());
        let tier =
            S3Tier::with_object_api(api.clone(), config(8, false)).expect("open memory tier");
        let locations: Vec<_> = (0..8)
            .map(|seed| tier.write(&page(seed)).expect("write"))
            .collect();
        for location in locations {
            tier.free(location);
        }
        let stats = tier.stats_handle();
        let deadline = Instant::now() + Duration::from_secs(2);
        while stats.snapshot().delete_requests < 8 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(stats.snapshot().delete_requests, 8);
        drop(tier);
        assert!(api.is_empty());
    }

    #[test]
    fn concurrent_writers_and_readers() {
        let api = Arc::new(MemoryObjectApi::new());
        let tier =
            Arc::new(S3Tier::with_object_api(api, config(8 * 32, true)).expect("open memory tier"));
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let tier = Arc::clone(&tier);
                thread::spawn(move || {
                    for index in 0..32 {
                        let input = page(worker * 100 + index);
                        let location = tier.write(&input).expect("write");
                        let mut output = vec![0_u8; PAGE_SIZE];
                        tier.read(location, &mut output).expect("read");
                        assert_eq!(output, input);
                        tier.free(location);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("join worker");
        }
        let stats = tier.stats_handle().snapshot();
        assert_eq!(stats.stored_bytes, 0);
        assert_eq!(stats.logical_bytes, 0);
    }

    #[cfg(not(feature = "lz4"))]
    #[test]
    fn lz4_codec_without_feature_is_invalid_config() {
        let api = Arc::new(MemoryObjectApi::new());
        let mut tier_config = config(1, true);
        tier_config.codec = PageCodec::Lz4;
        let result = S3Tier::with_object_api(api, tier_config);
        assert!(
            matches!(result, Err(TierBufError::InvalidConfig(message)) if message.contains("lz4"))
        );
    }
}
