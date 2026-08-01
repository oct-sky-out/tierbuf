//! S3-compatible HTTP boundary and buffer-manager integration tests.
//!
//! The real-endpoint tests are skipped unless every required environment
//! variable is set. A local MinIO run can be started with:
//!
//! ```text
//! docker run -d --rm -p 9000:9000 --name tierbuf-minio \
//!   -e MINIO_ROOT_USER=tierbuf -e MINIO_ROOT_PASSWORD=tierbuf-secret \
//!   minio/minio server /data
//! TIERBUF_S3_ENDPOINT=http://127.0.0.1:9000 TIERBUF_S3_BUCKET=tierbuf-it \
//! TIERBUF_S3_REGION=us-east-1 AWS_ACCESS_KEY_ID=tierbuf \
//! AWS_SECRET_ACCESS_KEY=tierbuf-secret \
//! cargo test -p tierbuf --features s3 --test s3_integration -- --nocapture
//! ```

#![cfg(feature = "s3")]

use std::env;
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tierbuf::pool::{BufConfig, BufferManager, Economics, EvictionMode, SharedGuard};
use tierbuf::swip::Swip;
use tierbuf::tier::TierBackend;
use tierbuf::tier::envelope::PageCodec;
use tierbuf::tier::s3::client::{MemoryObjectApi, ObjectApi, S3Client, S3ClientConfig};
use tierbuf::tier::s3::credentials::CredentialSource;
use tierbuf::tier::s3::{S3Tier, S3TierConfig};
use tierbuf::{PAGE_SIZE, TierBufError};

const RETRY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
struct IntegrationEnvironment {
    endpoint: String,
    bucket: String,
    region: String,
}

impl IntegrationEnvironment {
    fn load() -> Option<Self> {
        let endpoint = required_environment("TIERBUF_S3_ENDPOINT")?;
        let bucket = required_environment("TIERBUF_S3_BUCKET")?;
        let region = required_environment("TIERBUF_S3_REGION")?;
        // CredentialSource::Environment reads the values again when each
        // request is signed. Requiring them here keeps partial configurations
        // from accidentally reaching a real endpoint.
        required_environment("AWS_ACCESS_KEY_ID")?;
        required_environment("AWS_SECRET_ACCESS_KEY")?;
        Some(Self {
            endpoint,
            bucket,
            region,
        })
    }

    fn client_config(&self) -> S3ClientConfig {
        S3ClientConfig {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            endpoint: Some(self.endpoint.clone()),
            credentials: CredentialSource::Environment,
            ..S3ClientConfig::default()
        }
    }
}

#[test]
fn client_object_lifecycle() {
    let Some(environment) = IntegrationEnvironment::load() else {
        return;
    };
    let client = S3Client::new(environment.client_config()).expect("create S3 client");
    client.create_bucket().expect("create integration bucket");
    let key = format!("{}raw-object", unique_prefix("client"));
    let body = b"tierbuf S3 integration lifecycle";

    client.put(&key, body).expect("put integration object");
    assert_eq!(client.get(&key).expect("get integration object"), body);
    client.delete(&key).expect("delete integration object");
    client
        .delete(&key)
        .expect("deleting a missing integration object is idempotent");

    let error = client
        .get(&key)
        .expect_err("deleted integration object must be missing");
    assert!(matches!(
        error,
        TierBufError::Io(source) if source.kind() == io::ErrorKind::NotFound
    ));
}

#[cfg(feature = "lz4")]
#[test]
fn tier_round_trip_with_compression() {
    let Some(environment) = IntegrationEnvironment::load() else {
        return;
    };
    ensure_bucket(&environment);
    let tier = S3Tier::open(
        environment.client_config(),
        S3TierConfig {
            capacity_bytes: 64 * PAGE_SIZE as u64,
            codec: PageCodec::Lz4,
            key_prefix: integration_prefix(),
            synchronous_delete: true,
            ..S3TierConfig::default()
        },
    )
    .expect("open S3 integration tier");
    let mut pages = Vec::with_capacity(64);

    for seed in 0..64_u64 {
        let page = deterministic_page(seed);
        let location = tier.write(&page).expect("write compressed page");
        pages.push((location, page));
    }
    let written = tier.stats_handle().snapshot();
    assert_eq!(written.logical_bytes, 64 * PAGE_SIZE as u64);
    assert!(
        written.stored_bytes < written.logical_bytes,
        "the integration fixture should compress"
    );

    for (location, expected) in &pages {
        let mut actual = vec![0_u8; PAGE_SIZE];
        tier.read(*location, &mut actual)
            .expect("read compressed page");
        assert_eq!(&actual, expected);
    }
    for (location, _) in pages {
        tier.free(location);
    }

    let freed = tier.stats_handle().snapshot();
    assert_eq!(freed.stored_bytes, 0);
    assert_eq!(freed.logical_bytes, 0);
}

#[test]
fn buffer_manager_cliff_smoke() {
    let Some(environment) = IntegrationEnvironment::load() else {
        return;
    };
    ensure_bucket(&environment);
    let s3_tier = S3Tier::open(
        environment.client_config(),
        S3TierConfig {
            capacity_bytes: 256 * 1024 * 1024,
            codec: available_codec(),
            key_prefix: unique_prefix("smoke"),
            ..S3TierConfig::default()
        },
    )
    .expect("open S3 smoke tier");
    let s3_stats = s3_tier.stats_handle();
    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes: 32 * PAGE_SIZE,
        tiers: vec![Box::new(s3_tier)],
        prefetch_workers: 64,
        max_prefetch_in_flight: 256,
        ..BufConfig::default()
    })
    .expect("create S3 smoke buffer manager");
    let mut swips = Vec::with_capacity(512);

    for index in 0..512 {
        let mut guard = retry_until(RETRY_TIMEOUT, || manager.allocate())
            .expect("pressure allocation must make progress");
        guard.write_with(|page| write_marker(page, index));
        swips.push(guard.swip());
    }

    let mut random = XorShift64::new(0x9e37_79b9_7f4a_7c15);
    for _ in 0..1_000 {
        let index = random.next() as usize % swips.len();
        let guard =
            retry_shared(&manager, &swips[index]).expect("random S3-backed fix must make progress");
        guard.read_with(|page| assert_marker(page, index));
    }

    let manager_stats = manager.stats();
    assert!(manager_stats.faults > 0);
    assert!(manager_stats.tiers[0].demand_hits > 0);
    assert!(manager_stats.tiers[0].reads > 0);
    assert!(s3_stats.snapshot().get_requests > 0);

    // The public API does not yet free logical pages. A unique prefix
    // isolates these objects until the bucket lifecycle rule removes them.
    drop(manager);
}

#[test]
fn buffer_manager_evicts_through_s3_tier() {
    let object_api = Arc::new(MemoryObjectApi::new());
    let tier = S3Tier::with_object_api(
        object_api.clone(),
        S3TierConfig {
            capacity_bytes: 64 * PAGE_SIZE as u64,
            codec: available_codec(),
            key_prefix: "manager-integration/".to_owned(),
            synchronous_delete: true,
            ..S3TierConfig::default()
        },
    )
    .expect("open in-memory S3 tier");
    let s3_stats = tier.stats_handle();
    assert!(tier.raw_fd().is_none());
    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes: 4 * PAGE_SIZE,
        cooling_ratio: 0.25,
        eviction_mode: EvictionMode::Demand,
        economics: Economics {
            epoch: Duration::from_millis(10),
            ..Economics::default()
        },
        tiers: vec![Box::new(tier)],
        ..BufConfig::default()
    })
    .expect("create in-memory S3 buffer manager");
    let mut swips = Vec::with_capacity(16);

    for index in 0..16 {
        let mut guard = retry_until(RETRY_TIMEOUT, || manager.allocate())
            .expect("in-memory pressure allocation must make progress");
        guard.write_with(|page| page.fill(marker(index)));
        swips.push(guard.swip());
    }

    for (index, swip) in swips.iter().enumerate() {
        let guard =
            retry_shared(&manager, swip).expect("evicted in-memory page must fault back in");
        guard.read_with(|page| {
            assert!(
                page.iter().all(|&byte| byte == marker(index)),
                "page {index} did not survive its envelope round trip"
            );
        });
    }

    let manager_stats = manager.stats();
    assert!(manager_stats.evictions > 0);
    assert!(manager_stats.tiers[0].writes > 0);
    assert!(manager_stats.tiers[0].reads > 0);
    Arc::clone(&manager)
        .shutdown()
        .expect("in-memory S3 manager must shut down cleanly");
    let stats = s3_stats.snapshot();
    assert!(stats.put_requests > 0);
    assert!(stats.get_requests > 0);
    assert_eq!(object_api.snapshot().put_requests, stats.put_requests);
    assert_eq!(object_api.snapshot().get_requests, stats.get_requests);
}

fn required_environment(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => {
            eprintln!("skipping: {name} not set");
            None
        }
    }
}

fn ensure_bucket(environment: &IntegrationEnvironment) {
    S3Client::new(environment.client_config())
        .and_then(|client| client.create_bucket())
        .expect("create integration bucket");
}

#[cfg(feature = "lz4")]
fn integration_prefix() -> String {
    format!("it-{}-{}/", unix_seconds(), std::process::id())
}

fn unique_prefix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "it-{label}-{}-{}-{nanos}/",
        unix_seconds(),
        std::process::id()
    )
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(feature = "lz4")]
fn deterministic_page(seed: u64) -> Vec<u8> {
    let mut page = vec![0_u8; PAGE_SIZE];
    for (index, byte) in page.iter_mut().enumerate() {
        *byte = ((index as u64 / 256) ^ seed).to_le_bytes()[0];
    }
    page
}

fn available_codec() -> PageCodec {
    #[cfg(feature = "lz4")]
    {
        PageCodec::Lz4
    }
    #[cfg(not(feature = "lz4"))]
    {
        PageCodec::None
    }
}

fn retry_shared<'a>(manager: &'a BufferManager, swip: &Swip) -> tierbuf::Result<SharedGuard<'a>> {
    retry_until(RETRY_TIMEOUT, || manager.fix_shared(swip))
}

fn retry_until<T>(
    timeout: Duration,
    mut operation: impl FnMut() -> tierbuf::Result<T>,
) -> tierbuf::Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(TierBufError::PoolExhausted | TierBufError::Contended | TierBufError::Retry)
                if Instant::now() < deadline =>
            {
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_marker(page: &mut [u8; PAGE_SIZE], index: usize) {
    page.fill(marker(index));
    page[..8].copy_from_slice(&(index as u64).to_le_bytes());
}

fn assert_marker(page: &[u8; PAGE_SIZE], index: usize) {
    assert_eq!(&page[..8], &(index as u64).to_le_bytes());
    assert!(page[8..].iter().all(|&byte| byte == marker(index)));
}

fn marker(index: usize) -> u8 {
    (index as u8).wrapping_mul(37).wrapping_add(11)
}

#[derive(Clone, Copy, Debug)]
struct XorShift64(u64);

impl XorShift64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
