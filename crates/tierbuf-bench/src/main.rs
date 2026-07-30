#![deny(missing_docs)]
//! Degradation-curve benchmark harness for tierbuf.

use std::env;
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tierbuf::metrics::{TierCounters, TierStats};
use tierbuf::policy::AccessHint;
use tierbuf::pool::{BufConfig, BufferManager, Economics, EvictionMode, FixSource};
use tierbuf::swip::Swip;
use tierbuf::tier::envelope::PageCodec;
use tierbuf::tier::file::{FileTier, FileTierConfig};
use tierbuf::tier::mock::MockTier;
use tierbuf::tier::s3::client::{ObjectApi, S3Client, S3ClientConfig};
use tierbuf::tier::s3::credentials::CredentialSource;
use tierbuf::tier::s3::{S3Tier, S3TierConfig, S3TierStats, S3TierStatsHandle};
use tierbuf::tier::{LatencyProfile, TierBackend};
use tierbuf::{PAGE_SIZE, TierBufError};

const MIB: u64 = 1024 * 1024;
const DEFAULT_DATASET_MIB: u64 = 4 * 1024;
const DEFAULT_WARMUP: Duration = Duration::from_secs(10);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(30);
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_MOCK_LATENCY_US: u64 = 80;
const DEFAULT_PREFETCH_WORKERS: usize = 4;
const DEFAULT_PREFETCH_IN_FLIGHT: usize = 128;
const DEFAULT_S3_PREFETCH_WORKERS: usize = 64;
const DEFAULT_S3_PREFETCH_IN_FLIGHT: usize = 256;
const DEFAULT_S3_REGION: &str = "us-east-1";
const DEFAULT_OUTPUT: &str = "results/curve.csv";
const DEFAULT_STATS_OUTPUT: &str = "results/stats.json";
const ECONOMIC_EPOCH: Duration = Duration::from_millis(100);
const DRAM_PRICE_GIB_MONTH: f64 = 4.5;
const MOCK_PRICE_GIB_MONTH: f64 = 0.08;
const MOCK_SEQUENTIAL_GIB_PER_SECOND: f64 = 3.0;
const ZIPF_THETA: f64 = 0.99;
const POINT_ACCESS_PERCENT: u64 = 70;
const SCAN_PREFETCH_WINDOW: usize = 8;
const SAMPLE_EVERY_OPERATIONS: u64 = 64;
const MAX_LATENCY_SAMPLES_PER_WORKER: usize = 50_000;
const RESOURCE_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const S3_RESOURCE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const RESOURCE_RETRY_PAUSE: Duration = Duration::from_micros(100);
const CSV_HEADER: &str = concat!(
    "fraction,throughput_ops,p50_us,p99_us,cost_usd_per_1e6ops,",
    "point_ops,point_throughput_ops,point_p50_us,point_p99_us,",
    "scan_ops,scan_throughput_ops,scan_p50_us,scan_p99_us,",
    "dram_hit_rate,lower_tier_hit_rate,point_dram_hit_rate,scan_dram_hit_rate\n"
);
const WORKLOAD_SEED: u64 = 0x6a09_e667_f3bc_c909;
const MEASUREMENT_SEED_SALT: u64 = 0xbb67_ae85_84ca_a73b;
const SAMPLE_SEED_SALT: u64 = 0x3c6e_f372_fe94_f82b;

const DRAM_FRACTIONS: [DramFraction; 6] = [
    DramFraction::new(10),
    DramFraction::new(8),
    DramFraction::new(6),
    DramFraction::new(4),
    DramFraction::new(2),
    DramFraction::new(1),
];

fn main() -> ExitCode {
    match run_from_env() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tierbuf-bench: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_from_env() -> Result<(), String> {
    match parse_cli(env::args().skip(1))? {
        CliAction::Help => {
            print_usage();
            Ok(())
        }
        CliAction::Run(config) => run_benchmark(&config),
    }
}

fn run_benchmark(config: &Config) -> Result<(), String> {
    let layout = config.dataset_layout()?;
    let s3_runtime = prepare_s3_runtime(config)?;
    let mut output = CsvOutput::open(&config.output)?;
    let mut stats_runs = Vec::with_capacity(config.fractions.len());

    println!(
        "tierbuf degradation curve: {} MiB, {} workers, {:.3}s warmup, {:.3}s measurement",
        config.dataset_mib,
        config.workers,
        config.warmup.as_secs_f64(),
        config.measurement.as_secs_f64()
    );

    for &fraction in &config.fractions {
        eprintln!(
            "fraction {}: initializing {} pages in {} DRAM frames",
            fraction.label(),
            layout.page_count,
            fraction.dram_pages(layout.page_count)?
        );
        let (row, stats) = run_fraction(config, layout, fraction, s3_runtime.as_ref())?;
        output.append(&row)?;
        stats_runs.push(stats);
        println!("{}", format_csv_row(&row).trim_end());
    }

    write_stats_json(&config.stats_output, &stats_runs)?;
    Ok(())
}

fn run_fraction(
    config: &Config,
    layout: DatasetLayout,
    fraction: DramFraction,
    s3_runtime: Option<&S3Runtime>,
) -> Result<(BenchRow, Value), String> {
    let ManagerContext { manager, s3_stats } = make_manager(config, layout, fraction, s3_runtime)?;

    let benchmark_result = (|| {
        let initialized = if config.s3.is_some() {
            // Keep loading concurrency fixed so --compare-prefetch changes
            // only the measured prefetch path, not the initial resident set.
            initialize_dataset_parallel(&manager, layout.page_count, DEFAULT_S3_PREFETCH_WORKERS)?
        } else {
            initialize_dataset(&manager, layout.page_count)?
        };
        let swips: Arc<[Swip]> = initialized.into();
        let zipf = Arc::new(ZipfSampler::new(layout.page_count)?);

        run_phase(
            &manager,
            &swips,
            &zipf,
            PhaseConfig {
                worker_count: config.workers,
                duration: config.warmup,
                phase: Phase::Warmup,
                prefetch_scan: config.prefetch_scan,
                scan_only: config.scan_only,
            },
        )?;

        let cost_before = manager.cost_report().actual_cost_usd;
        let stats_before = manager.stats();
        let measured = run_phase(
            &manager,
            &swips,
            &zipf,
            PhaseConfig {
                worker_count: config.workers,
                duration: config.measurement,
                phase: Phase::Measurement,
                prefetch_scan: config.prefetch_scan,
                scan_only: config.scan_only,
            },
        )?;
        let cost_after = manager.cost_report().actual_cost_usd;
        let stats_after = manager.stats();
        eprintln!(
            "fraction {}: hits={} faults={} evictions={} prefetch={}/{} skipped={}",
            fraction.label(),
            stats_after.dram_hits,
            stats_after.faults,
            stats_after.evictions,
            stats_after.prefetch_hits,
            stats_after.prefetch_submitted,
            stats_after.prefetch_skipped
        );

        let stats_dump = fraction_stats_json(
            fraction,
            &measured,
            &stats_before,
            &stats_after,
            s3_stats.as_ref().map(S3TierStatsHandle::snapshot),
        )?;
        let row = BenchRow::from_measurement(
            fraction,
            measured,
            (cost_after - cost_before).max(0.0),
            &stats_before,
            &stats_after,
        )?;
        Ok((row, stats_dump))
    })();

    let shutdown_result = if config.s3.is_some() {
        // S3 benchmark prefixes are ephemeral. BufferManager::shutdown would
        // synchronously persist every still-resident dirty page, adding a
        // serial tail that is outside the measured workload. Dropping the
        // manager invokes its non-flushing Drop path, which signals workers
        // and unswizzles resident pages without creating extra S3 objects.
        drop(manager);
        Ok(())
    } else {
        manager
            .shutdown()
            .map_err(|error| format!("failed to shut down fraction {}: {error}", fraction.label()))
    };

    match (benchmark_result, shutdown_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(benchmark_error), Err(shutdown_error)) => {
            Err(format!("{benchmark_error}; additionally, {shutdown_error}"))
        }
    }
}

fn write_stats_json(path: &Path, runs: &[Value]) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create stats output directory '{}': {error}",
                parent.display()
            )
        })?;
    }

    let file = File::create(path)
        .map_err(|error| format!("failed to open stats output '{}': {error}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(
        &mut writer,
        &json!({
            "schema_version": 1,
            "capture_point": "after_measurement_before_shutdown",
            "runs": runs,
        }),
    )
    .map_err(|error| {
        format!(
            "failed to serialize stats output '{}': {error}",
            path.display()
        )
    })?;
    writer
        .write_all(b"\n")
        .and_then(|()| writer.flush())
        .map_err(|error| format!("failed to flush stats output '{}': {error}", path.display()))
}

fn fraction_stats_json(
    fraction: DramFraction,
    measured: &PhaseResult,
    stats_before: &TierStats,
    stats_after: &TierStats,
    s3_stats: Option<S3TierStats>,
) -> Result<Value, String> {
    let measurement_stats = tier_stats_delta(stats_before, stats_after)?;
    let mut run = json!({
        "fraction": fraction.label(),
        "measurement_seconds": measured.elapsed.as_secs_f64(),
        "measurement_operations": {
            "total": measured.operations,
            "point": measured.point_operations,
            "scan": measured.scan_operations,
            "point_dram_hits": measured.point_dram_hits,
            "scan_dram_hits": measured.scan_dram_hits,
        },
        "measurement_stats": tier_stats_json(&measurement_stats),
        "cumulative_stats": tier_stats_json(stats_after),
    });
    if let Some(s3_stats) = s3_stats
        && let Some(run) = run.as_object_mut()
    {
        run.insert("s3".to_owned(), s3_stats_json(s3_stats));
    }
    Ok(run)
}

fn s3_stats_json(stats: S3TierStats) -> Value {
    let compression_ratio =
        (stats.logical_bytes != 0).then(|| stats.stored_bytes as f64 / stats.logical_bytes as f64);
    json!({
        "get_requests": stats.get_requests,
        "put_requests": stats.put_requests,
        "delete_requests": stats.delete_requests,
        "request_failures": stats.request_failures,
        "stored_bytes": stats.stored_bytes,
        "logical_bytes": stats.logical_bytes,
        "bytes_uploaded": stats.bytes_uploaded,
        "bytes_downloaded": stats.bytes_downloaded,
        "compression_ratio": compression_ratio,
    })
}

fn tier_stats_json(stats: &TierStats) -> Value {
    let lower_tier_fixes = stats.tiers.iter().map(|tier| tier.demand_hits).sum::<u64>();
    json!({
        "total_fixes": stats.dram_hits.saturating_add(lower_tier_fixes),
        "dram_hits": stats.dram_hits,
        "faults": stats.faults,
        "evictions": stats.evictions,
        "second_chances": stats.second_chances,
        "prefetch_submitted": stats.prefetch_submitted,
        "prefetch_hits": stats.prefetch_hits,
        "prefetch_skipped": stats.prefetch_skipped,
        "budget_denied": stats.budget_denied,
        "tiers": stats.tiers.iter().map(|tier| json!({
            "name": tier.name,
            "demand_hits": tier.demand_hits,
            "reads": tier.reads,
            "writes": tier.writes,
            "bytes_read": tier.bytes_read,
            "bytes_written": tier.bytes_written,
        })).collect::<Vec<_>>(),
    })
}

fn tier_stats_delta(before: &TierStats, after: &TierStats) -> Result<TierStats, String> {
    if before.tiers.len() != after.tiers.len()
        || before
            .tiers
            .iter()
            .zip(&after.tiers)
            .any(|(before_tier, after_tier)| before_tier.name != after_tier.name)
    {
        return Err("tier stats changed shape during measurement".to_owned());
    }

    let delta = |name: &str, before: u64, after: u64| {
        after
            .checked_sub(before)
            .ok_or_else(|| format!("{name} counter regressed during measurement"))
    };
    let tiers = before
        .tiers
        .iter()
        .zip(&after.tiers)
        .map(|(before_tier, after_tier)| {
            Ok(TierCounters {
                name: after_tier.name.clone(),
                demand_hits: delta(
                    &format!("{} demand hits", after_tier.name),
                    before_tier.demand_hits,
                    after_tier.demand_hits,
                )?,
                reads: delta(
                    &format!("{} reads", after_tier.name),
                    before_tier.reads,
                    after_tier.reads,
                )?,
                writes: delta(
                    &format!("{} writes", after_tier.name),
                    before_tier.writes,
                    after_tier.writes,
                )?,
                bytes_read: delta(
                    &format!("{} bytes read", after_tier.name),
                    before_tier.bytes_read,
                    after_tier.bytes_read,
                )?,
                bytes_written: delta(
                    &format!("{} bytes written", after_tier.name),
                    before_tier.bytes_written,
                    after_tier.bytes_written,
                )?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(TierStats {
        dram_hits: delta("DRAM hits", before.dram_hits, after.dram_hits)?,
        faults: delta("faults", before.faults, after.faults)?,
        evictions: delta("evictions", before.evictions, after.evictions)?,
        second_chances: delta(
            "second chances",
            before.second_chances,
            after.second_chances,
        )?,
        prefetch_submitted: delta(
            "prefetch submitted",
            before.prefetch_submitted,
            after.prefetch_submitted,
        )?,
        prefetch_hits: delta("prefetch hits", before.prefetch_hits, after.prefetch_hits)?,
        prefetch_skipped: delta(
            "prefetch skipped",
            before.prefetch_skipped,
            after.prefetch_skipped,
        )?,
        budget_denied: delta("budget denied", before.budget_denied, after.budget_denied)?,
        tiers,
    })
}

struct S3Runtime {
    client: Arc<S3Client>,
}

struct ManagerContext {
    manager: Arc<BufferManager>,
    s3_stats: Option<S3TierStatsHandle>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TierKind {
    Mock,
    File,
    S3,
}

fn tier_plan(config: &Config) -> Vec<TierKind> {
    let mut tiers = Vec::with_capacity(2);
    if config.file_tier.is_some() {
        tiers.push(TierKind::File);
    }
    if config.s3.is_some() {
        tiers.push(TierKind::S3);
    }
    if tiers.is_empty() {
        tiers.push(TierKind::Mock);
    }
    tiers
}

fn prepare_s3_runtime(config: &Config) -> Result<Option<S3Runtime>, String> {
    let Some(s3) = config.s3.as_ref() else {
        return Ok(None);
    };
    let client = Arc::new(
        S3Client::new(s3_client_config(s3))
            .map_err(|error| format!("failed to create S3 client: {error}"))?,
    );
    smoke_object_api(&*client, &s3.key_prefix)?;
    Ok(Some(S3Runtime { client }))
}

fn s3_client_config(config: &S3BenchConfig) -> S3ClientConfig {
    S3ClientConfig {
        bucket: config.bucket.clone(),
        region: config.region.clone(),
        endpoint: config.endpoint.clone(),
        credentials: CredentialSource::Auto,
        ..S3ClientConfig::default()
    }
}

fn smoke_object_api(api: &dyn ObjectApi, key_prefix: &str) -> Result<(), String> {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let key = format!(
        "{}__smoke-{}-{unix_nanos}",
        with_trailing_slash(key_prefix),
        std::process::id()
    );
    let page = vec![0xa5; PAGE_SIZE];
    api.put(&key, &page)
        .map_err(|error| format!("S3 startup smoke PUT failed: {error}"))?;
    let fetched = api.get(&key);
    let delete_result = api.delete(&key);
    let fetched = fetched.map_err(|error| format!("S3 startup smoke GET failed: {error}"))?;
    delete_result.map_err(|error| format!("S3 startup smoke DELETE failed: {error}"))?;
    if fetched != page {
        return Err("S3 startup smoke GET returned different page bytes".to_owned());
    }
    Ok(())
}

fn with_trailing_slash(prefix: &str) -> String {
    if prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    }
}

fn make_manager(
    config: &Config,
    layout: DatasetLayout,
    fraction: DramFraction,
    s3_runtime: Option<&S3Runtime>,
) -> Result<ManagerContext, String> {
    let latency = LatencyProfile::new(
        config.mock_latency_us,
        config.mock_latency_us,
        MOCK_SEQUENTIAL_GIB_PER_SECOND,
    );
    let mut tiers: Vec<Box<dyn TierBackend>> = Vec::with_capacity(2);
    let mut s3_stats = None;
    for kind in tier_plan(config) {
        match kind {
            TierKind::Mock => {
                tiers.push(Box::new(
                    MockTier::with_options(
                        "mock",
                        layout.tier_capacity_bytes,
                        MOCK_PRICE_GIB_MONTH,
                        latency,
                        None,
                    )
                    .map_err(|error| format!("failed to create MockTier: {error}"))?,
                ));
            }
            TierKind::File => {
                let path = config
                    .file_tier
                    .as_ref()
                    .ok_or_else(|| "file tier plan is missing its path".to_owned())?;
                let mut file_config = FileTierConfig::new(layout.tier_capacity_bytes);
                file_config.name = "file".to_owned();
                file_config.price_gb_month = MOCK_PRICE_GIB_MONTH;
                file_config.latency = latency;
                tiers
                    .push(Box::new(FileTier::open(path, file_config).map_err(
                        |error| format!("failed to create FileTier: {error}"),
                    )?));
            }
            TierKind::S3 => {
                let options = config
                    .s3
                    .as_ref()
                    .ok_or_else(|| "S3 tier plan is missing its configuration".to_owned())?;
                let runtime = s3_runtime
                    .ok_or_else(|| "S3 tier plan is missing its prepared client".to_owned())?;
                let capacity_bytes = options
                    .capacity_mib
                    .checked_mul(MIB)
                    .ok_or_else(|| "--s3-capacity-mib is too large".to_owned())?;
                let fraction_prefix = format!(
                    "{}fraction-{}/",
                    with_trailing_slash(&options.key_prefix),
                    fraction.label()
                );
                let object_api: Arc<dyn ObjectApi> = runtime.client.clone();
                let tier = S3Tier::with_object_api(
                    object_api,
                    S3TierConfig {
                        capacity_bytes,
                        codec: if options.compression {
                            PageCodec::Lz4
                        } else {
                            PageCodec::None
                        },
                        key_prefix: fraction_prefix,
                        ..S3TierConfig::default()
                    },
                )
                .map_err(|error| format!("failed to create S3Tier: {error}"))?;
                s3_stats = Some(tier.stats_handle());
                tiers.push(Box::new(tier));
            }
        }
    }
    let dram_pages = fraction.dram_pages(layout.page_count)?;
    let dram_pool_bytes = dram_pages
        .checked_mul(PAGE_SIZE)
        .ok_or_else(|| "DRAM pool size overflowed usize".to_owned())?;

    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes,
        eviction_mode: config.eviction_mode,
        economics: Economics {
            dram_price_gb_month: DRAM_PRICE_GIB_MONTH,
            epoch: ECONOMIC_EPOCH,
        },
        tiers,
        prefetch_workers: config.prefetch_workers,
        max_prefetch_in_flight: config.prefetch_in_flight,
        ..BufConfig::default()
    })
    .map_err(|error| {
        format!(
            "failed to create buffer manager for fraction {}: {error}",
            fraction.label()
        )
    })?;
    Ok(ManagerContext { manager, s3_stats })
}

fn initialize_dataset(
    manager: &Arc<BufferManager>,
    page_count: usize,
) -> Result<Vec<Swip>, String> {
    let mut swips = Vec::with_capacity(page_count);

    for index in 0..page_count {
        swips.push(initialize_page(
            manager,
            index,
            page_count,
            RESOURCE_RETRY_TIMEOUT,
        )?);
    }

    Ok(swips)
}

fn initialize_dataset_parallel(
    manager: &Arc<BufferManager>,
    page_count: usize,
    worker_count: usize,
) -> Result<Vec<Swip>, String> {
    parallel_collect_ordered(page_count, worker_count, |index| {
        initialize_page(manager, index, page_count, S3_RESOURCE_RETRY_TIMEOUT)
    })
}

fn initialize_page(
    manager: &Arc<BufferManager>,
    index: usize,
    page_count: usize,
    retry_timeout: Duration,
) -> Result<Swip, String> {
    let retry_deadline = Instant::now() + retry_timeout;
    loop {
        match manager.allocate() {
            Ok(mut guard) => {
                guard.write_with(|page| page[0] = page_marker(index));
                return Ok(guard.swip());
            }
            Err(TierBufError::PoolExhausted) if Instant::now() < retry_deadline => {
                thread::sleep(RESOURCE_RETRY_PAUSE);
            }
            Err(TierBufError::PoolExhausted) => {
                return Err(pool_exhausted_message(&format!(
                    "initializing logical page {index} of {page_count}"
                )));
            }
            Err(error) => {
                return Err(format!(
                    "failed to initialize logical page {index} of {page_count}: {error}"
                ));
            }
        }
    }
}

fn parallel_collect_ordered<T, F>(
    item_count: usize,
    worker_count: usize,
    operation: F,
) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(usize) -> Result<T, String> + Sync,
{
    if item_count == 0 {
        return Ok(Vec::new());
    }

    let worker_count = worker_count.clamp(1, 256).min(item_count);
    let next_index = AtomicUsize::new(0);
    let lowest_failure = AtomicUsize::new(item_count);
    let (sender, receiver) = mpsc::channel::<(usize, Result<T, String>)>();

    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let sender = sender.clone();
            let operation = &operation;
            let next_index = &next_index;
            let lowest_failure = &lowest_failure;
            workers.push((
                worker_id,
                scope.spawn(move || {
                    loop {
                        let index = next_index.fetch_add(1, Ordering::Relaxed);
                        if index >= item_count || index >= lowest_failure.load(Ordering::Acquire) {
                            break;
                        }

                        let result = panic::catch_unwind(AssertUnwindSafe(|| operation(index)))
                            .unwrap_or_else(|_| {
                                Err(format!(
                                    "dataset initialization panicked at logical page {index}"
                                ))
                            });
                        if result.is_err() {
                            lowest_failure.fetch_min(index, Ordering::AcqRel);
                        }
                        if sender.send((index, result)).is_err() {
                            break;
                        }
                    }
                }),
            ));
        }
        drop(sender);

        let mut values = std::iter::repeat_with(|| None)
            .take(item_count)
            .collect::<Vec<Option<T>>>();
        let mut failure: Option<(usize, String)> = None;
        for (index, result) in receiver {
            match result {
                Ok(value) => values[index] = Some(value),
                Err(error)
                    if failure
                        .as_ref()
                        .is_none_or(|(failed_index, _)| index < *failed_index) =>
                {
                    failure = Some((index, error));
                }
                Err(_) => {}
            }
        }

        let first_panicked_worker = workers
            .into_iter()
            .filter_map(|(worker_id, worker)| worker.join().err().map(|_| worker_id))
            .min();
        if let Some((_, error)) = failure {
            return Err(error);
        }
        if let Some(worker_id) = first_panicked_worker {
            return Err(format!(
                "dataset initialization worker {worker_id} panicked"
            ));
        }

        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                value.ok_or_else(|| format!("dataset initialization omitted logical page {index}"))
            })
            .collect()
    })
}

fn run_phase(
    manager: &Arc<BufferManager>,
    swips: &Arc<[Swip]>,
    zipf: &Arc<ZipfSampler>,
    config: PhaseConfig,
) -> Result<PhaseResult, String> {
    let ready = Arc::new(Barrier::new(config.worker_count + 1));
    let start = Arc::new(Barrier::new(config.worker_count + 1));
    let deadline = Arc::new(Mutex::new(None::<Instant>));
    let mut handles = Vec::with_capacity(config.worker_count);

    for worker_id in 0..config.worker_count {
        let manager = Arc::clone(manager);
        let swips = Arc::clone(swips);
        let zipf = Arc::clone(zipf);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let deadline = Arc::clone(&deadline);

        handles.push(thread::spawn(move || {
            ready.wait();
            start.wait();
            let phase_deadline = deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .expect("phase deadline must be published before the start barrier");
            run_worker(
                manager,
                swips,
                zipf,
                WorkerRun {
                    worker_id,
                    worker_count: config.worker_count,
                    deadline: phase_deadline,
                    phase: config.phase,
                    prefetch_scan: config.prefetch_scan,
                    scan_only: config.scan_only,
                },
            )
        }));
    }

    ready.wait();
    let phase_start = Instant::now();
    *deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(phase_start + config.duration);
    start.wait();

    let mut operations = 0_u64;
    let mut point_operations = 0_u64;
    let mut scan_operations = 0_u64;
    let mut point_dram_hits = 0_u64;
    let mut scan_dram_hits = 0_u64;
    let mut latencies_ns = Vec::new();
    let mut point_latencies_ns = Vec::new();
    let mut scan_latencies_ns = Vec::new();
    let mut first_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(worker)) => {
                operations = operations.saturating_add(worker.operations);
                point_operations = point_operations.saturating_add(worker.point_operations);
                scan_operations = scan_operations.saturating_add(worker.scan_operations);
                point_dram_hits = point_dram_hits.saturating_add(worker.point_dram_hits);
                scan_dram_hits = scan_dram_hits.saturating_add(worker.scan_dram_hits);
                latencies_ns.extend(worker.latencies_ns);
                point_latencies_ns.extend(worker.point_latencies_ns);
                scan_latencies_ns.extend(worker.scan_latencies_ns);
            }
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert_with(|| "a benchmark worker panicked".to_owned());
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(PhaseResult {
        operations,
        point_operations,
        scan_operations,
        point_dram_hits,
        scan_dram_hits,
        elapsed: phase_start.elapsed(),
        latencies_ns,
        point_latencies_ns,
        scan_latencies_ns,
    })
}

fn run_worker(
    manager: Arc<BufferManager>,
    swips: Arc<[Swip]>,
    zipf: Arc<ZipfSampler>,
    run: WorkerRun,
) -> Result<WorkerResult, String> {
    let seed_salt = match run.phase {
        Phase::Warmup => 0,
        Phase::Measurement => MEASUREMENT_SEED_SALT,
    };
    let seed = worker_seed(run.worker_id, seed_salt);
    let scan_start = run
        .worker_id
        .checked_mul(swips.len())
        .map(|value| value / run.worker_count)
        .unwrap_or(run.worker_id % swips.len());
    let mut workload = WorkloadState::new(seed, scan_start);
    let mut latency_sampler =
        LatencySampler::new(seed ^ SAMPLE_SEED_SALT, MAX_LATENCY_SAMPLES_PER_WORKER);
    let mut point_latency_sampler = LatencySampler::new(
        seed ^ SAMPLE_SEED_SALT.rotate_left(17),
        MAX_LATENCY_SAMPLES_PER_WORKER,
    );
    let mut scan_latency_sampler = LatencySampler::new(
        seed ^ SAMPLE_SEED_SALT.rotate_left(31),
        MAX_LATENCY_SAMPLES_PER_WORKER,
    );
    let mut operations = 0_u64;
    let mut point_operations = 0_u64;
    let mut scan_operations = 0_u64;
    let mut point_dram_hits = 0_u64;
    let mut scan_dram_hits = 0_u64;

    while Instant::now() < run.deadline {
        let point_access_percent = if run.scan_only {
            0
        } else {
            POINT_ACCESS_PERCENT
        };
        let access = workload.next_access(&zipf, point_access_percent);
        let measurement = matches!(run.phase, Phase::Measurement);
        let sample_overall = measurement && operations.is_multiple_of(SAMPLE_EVERY_OPERATIONS);
        let sample_kind = measurement
            && match access.hint {
                AccessHint::Normal => point_operations.is_multiple_of(SAMPLE_EVERY_OPERATIONS),
                AccessHint::Scan => scan_operations.is_multiple_of(SAMPLE_EVERY_OPERATIONS),
                AccessHint::Prefetch => unreachable!("workload never emits prefetch accesses"),
            };
        let operation_start = (sample_overall || sample_kind).then(Instant::now);

        let source = fix_and_verify(&manager, &swips, access, run.prefetch_scan)?;

        if let Some(operation_start) = operation_start {
            let latency_ns = duration_as_nanos_u64(operation_start.elapsed());
            if sample_overall {
                latency_sampler.record(latency_ns);
            }
            if sample_kind {
                match access.hint {
                    AccessHint::Normal => point_latency_sampler.record(latency_ns),
                    AccessHint::Scan => scan_latency_sampler.record(latency_ns),
                    AccessHint::Prefetch => {
                        unreachable!("workload never emits prefetch accesses")
                    }
                }
            }
        }
        match access.hint {
            AccessHint::Normal => {
                point_operations = point_operations.saturating_add(1);
                if source == FixSource::Dram {
                    point_dram_hits = point_dram_hits.saturating_add(1);
                }
            }
            AccessHint::Scan => {
                scan_operations = scan_operations.saturating_add(1);
                if source == FixSource::Dram {
                    scan_dram_hits = scan_dram_hits.saturating_add(1);
                }
            }
            AccessHint::Prefetch => unreachable!("workload never emits prefetch accesses"),
        }
        operations = operations.saturating_add(1);
    }

    Ok(WorkerResult {
        operations,
        point_operations,
        scan_operations,
        point_dram_hits,
        scan_dram_hits,
        latencies_ns: latency_sampler.into_values(),
        point_latencies_ns: point_latency_sampler.into_values(),
        scan_latencies_ns: scan_latency_sampler.into_values(),
    })
}

fn fix_and_verify(
    manager: &BufferManager,
    swips: &[Swip],
    access: AccessSelection,
    prefetch_scan: bool,
) -> Result<FixSource, String> {
    if prefetch_scan
        && access.hint == AccessHint::Scan
        && access.index.is_multiple_of(SCAN_PREFETCH_WINDOW)
    {
        let lookahead: [&Swip; SCAN_PREFETCH_WINDOW] =
            std::array::from_fn(|offset| &swips[(access.index + offset + 1) % swips.len()]);
        manager.prefetch(&lookahead);
    }

    let retry_deadline = Instant::now() + RESOURCE_RETRY_TIMEOUT;
    loop {
        let result = manager.fix_shared_with(&swips[access.index], access.hint);
        match result {
            Ok(guard) => {
                let (actual, scan_digest) = guard.read_with(|page| {
                    let digest = if access.hint == AccessHint::Scan {
                        full_page_digest(page)
                    } else {
                        0
                    };
                    (page[0], digest)
                });
                black_box(scan_digest);
                let expected = page_marker(access.index);
                if actual != expected {
                    return Err(format!(
                        "page verification failed for index {}: expected {expected:#04x}, got {actual:#04x}",
                        access.index
                    ));
                }
                return Ok(guard.source());
            }
            Err(TierBufError::PoolExhausted) if Instant::now() < retry_deadline => {
                thread::sleep(RESOURCE_RETRY_PAUSE);
            }
            Err(TierBufError::PoolExhausted) => {
                return Err(pool_exhausted_message(&format!(
                    "fixing logical page {}",
                    access.index
                )));
            }
            Err(TierBufError::Contended) if Instant::now() < retry_deadline => {
                thread::yield_now();
            }
            Err(error) => {
                return Err(format!(
                    "failed to fix logical page {} with {:?} hint: {error}",
                    access.index, access.hint
                ));
            }
        }
    }
}

fn full_page_digest(page: &[u8; PAGE_SIZE]) -> u64 {
    let mut first = 0xcbf2_9ce4_8422_2325_u64;
    let mut second = 0x6c62_272e_07bb_0142_u64;
    for byte in page {
        let value = u64::from(*byte);
        first = (first ^ value).wrapping_mul(0x0000_0100_0000_01b3);
        second = (second ^ value.rotate_left(1)).wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    first ^ second.rotate_left(29)
}

fn pool_exhausted_message(context: &str) -> String {
    format!(
        "DRAM pool exhausted while {context}; tierbuf's automatic cooling did not free a frame within {:.1}s. Ensure the cooling workers are integrated and running, or retry with --mock-latency-us 0 to diagnose storage-delay pressure",
        RESOURCE_RETRY_TIMEOUT.as_secs_f64()
    )
}

fn duration_as_nanos_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn page_marker(index: usize) -> u8 {
    let mixed = index.wrapping_mul(131).wrapping_add(17);
    u8::try_from(mixed % 251 + 1).expect("page marker is in the range 1..=251")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Warmup,
    Measurement,
}

#[derive(Clone, Copy, Debug)]
struct PhaseConfig {
    worker_count: usize,
    duration: Duration,
    phase: Phase,
    prefetch_scan: bool,
    scan_only: bool,
}

#[derive(Clone, Copy, Debug)]
struct WorkerRun {
    worker_id: usize,
    worker_count: usize,
    deadline: Instant,
    phase: Phase,
    prefetch_scan: bool,
    scan_only: bool,
}

#[derive(Debug)]
struct WorkerResult {
    operations: u64,
    point_operations: u64,
    scan_operations: u64,
    point_dram_hits: u64,
    scan_dram_hits: u64,
    latencies_ns: Vec<u64>,
    point_latencies_ns: Vec<u64>,
    scan_latencies_ns: Vec<u64>,
}

#[derive(Debug)]
struct PhaseResult {
    operations: u64,
    point_operations: u64,
    scan_operations: u64,
    point_dram_hits: u64,
    scan_dram_hits: u64,
    elapsed: Duration,
    latencies_ns: Vec<u64>,
    point_latencies_ns: Vec<u64>,
    scan_latencies_ns: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AccessSelection {
    index: usize,
    hint: AccessHint,
}

#[derive(Debug)]
struct WorkloadState {
    random: XorShift64,
    scan_cursor: usize,
}

impl WorkloadState {
    fn new(seed: u64, scan_start: usize) -> Self {
        Self {
            random: XorShift64::new(seed),
            scan_cursor: scan_start,
        }
    }

    fn next_access(&mut self, zipf: &ZipfSampler, point_access_percent: u64) -> AccessSelection {
        if self.random.next_u64() % 100 < point_access_percent {
            AccessSelection {
                index: zipf.sample(self.random.next_u64()),
                hint: AccessHint::Normal,
            }
        } else {
            let index = self.scan_cursor;
            self.scan_cursor = (self.scan_cursor + 1) % zipf.len();
            AccessSelection {
                index,
                hint: AccessHint::Scan,
            }
        }
    }
}

#[derive(Debug)]
struct ZipfSampler {
    cumulative: Box<[f64]>,
    total: f64,
}

impl ZipfSampler {
    fn new(item_count: usize) -> Result<Self, String> {
        if item_count == 0 {
            return Err("Zipf sampler requires at least one item".to_owned());
        }

        let mut total = 0.0;
        let mut cumulative = Vec::with_capacity(item_count);
        for rank in 1..=item_count {
            total += 1.0 / (rank as f64).powf(ZIPF_THETA);
            cumulative.push(total);
        }

        Ok(Self {
            cumulative: cumulative.into_boxed_slice(),
            total,
        })
    }

    fn len(&self) -> usize {
        self.cumulative.len()
    }

    fn sample(&self, random: u64) -> usize {
        let unit = random as f64 / (u64::MAX as f64 + 1.0);
        let target = unit * self.total;
        self.cumulative
            .partition_point(|cumulative| *cumulative <= target)
            .min(self.cumulative.len() - 1)
    }
}

#[derive(Clone, Copy, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }
}

fn worker_seed(worker_id: usize, salt: u64) -> u64 {
    let ordinal = u64::try_from(worker_id).unwrap_or(u64::MAX).wrapping_add(1);
    WORKLOAD_SEED ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ salt
}

#[derive(Debug)]
struct LatencySampler {
    seen: u64,
    values: Vec<u64>,
    random: XorShift64,
    capacity: usize,
}

impl LatencySampler {
    fn new(seed: u64, capacity: usize) -> Self {
        Self {
            seen: 0,
            values: Vec::with_capacity(capacity),
            random: XorShift64::new(seed),
            capacity,
        }
    }

    fn record(&mut self, latency_ns: u64) {
        self.seen = self.seen.saturating_add(1);
        if self.values.len() < self.capacity {
            self.values.push(latency_ns);
            return;
        }
        if self.capacity == 0 {
            return;
        }

        let candidate = self.random.next_u64() % self.seen;
        if let Ok(index) = usize::try_from(candidate)
            && index < self.capacity
        {
            self.values[index] = latency_ns;
        }
    }

    fn into_values(self) -> Vec<u64> {
        self.values
    }
}

fn percentile(samples: &mut [u64], requested: u32) -> Option<u64> {
    assert!(
        (1..=100).contains(&requested),
        "percentile must be between 1 and 100"
    );
    if samples.is_empty() {
        return None;
    }

    samples.sort_unstable();
    let numerator = samples.len().checked_mul(requested as usize)?;
    let rank = numerator.div_ceil(100).saturating_sub(1);
    samples.get(rank).copied()
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BenchRow {
    fraction: DramFraction,
    throughput_ops: f64,
    p50_us: f64,
    p99_us: f64,
    cost_usd_per_1e6ops: f64,
    point_ops: u64,
    point_throughput_ops: f64,
    point_p50_us: f64,
    point_p99_us: f64,
    scan_ops: u64,
    scan_throughput_ops: f64,
    scan_p50_us: f64,
    scan_p99_us: f64,
    dram_hit_rate: f64,
    lower_tier_hit_rate: f64,
    point_dram_hit_rate: f64,
    scan_dram_hit_rate: f64,
}

impl BenchRow {
    fn from_measurement(
        fraction: DramFraction,
        mut measured: PhaseResult,
        measured_cost_usd: f64,
        stats_before: &TierStats,
        stats_after: &TierStats,
    ) -> Result<Self, String> {
        if measured.operations == 0 {
            return Err(format!(
                "fraction {} completed no measured operations",
                fraction.label()
            ));
        }
        if measured.elapsed.is_zero() {
            return Err("measurement elapsed time was zero".to_owned());
        }

        let p50_ns = percentile(&mut measured.latencies_ns, 50)
            .ok_or_else(|| "measurement produced no latency samples".to_owned())?;
        let p99_ns = percentile(&mut measured.latencies_ns, 99)
            .ok_or_else(|| "measurement produced no latency samples".to_owned())?;
        let (point_p50_us, point_p99_us) = operation_latency_us(
            measured.point_operations,
            &mut measured.point_latencies_ns,
            "point",
        )?;
        let (scan_p50_us, scan_p99_us) = operation_latency_us(
            measured.scan_operations,
            &mut measured.scan_latencies_ns,
            "scan",
        )?;
        let classified_operations = measured
            .point_operations
            .checked_add(measured.scan_operations)
            .ok_or_else(|| "classified operation count overflowed u64".to_owned())?;
        if classified_operations != measured.operations {
            return Err(format!(
                "operation classification mismatch: total={}, point={}, scan={}",
                measured.operations, measured.point_operations, measured.scan_operations
            ));
        }
        if measured.point_dram_hits > measured.point_operations
            || measured.scan_dram_hits > measured.scan_operations
        {
            return Err("operation DRAM hit count exceeded its operation count".to_owned());
        }
        let (dram_hits, lower_tier_hits) = demand_hit_delta(stats_before, stats_after)?;
        let classified_hits = dram_hits
            .checked_add(lower_tier_hits)
            .ok_or_else(|| "demand hit count overflowed u64".to_owned())?;
        if classified_hits != measured.operations {
            return Err(format!(
                "tier hit classification mismatch: operations={}, DRAM hits={}, lower-tier hits={}",
                measured.operations, dram_hits, lower_tier_hits
            ));
        }
        let classified_dram_hits = measured
            .point_dram_hits
            .checked_add(measured.scan_dram_hits)
            .ok_or_else(|| "operation DRAM hit count overflowed u64".to_owned())?;
        if classified_dram_hits != dram_hits {
            return Err(format!(
                "operation DRAM hit classification mismatch: stats={dram_hits}, point={}, scan={}",
                measured.point_dram_hits, measured.scan_dram_hits
            ));
        }
        let operations = measured.operations as f64;
        let elapsed_seconds = measured.elapsed.as_secs_f64();

        Ok(Self {
            fraction,
            throughput_ops: operations / elapsed_seconds,
            p50_us: p50_ns as f64 / 1_000.0,
            p99_us: p99_ns as f64 / 1_000.0,
            cost_usd_per_1e6ops: measured_cost_usd * 1_000_000.0 / operations,
            point_ops: measured.point_operations,
            point_throughput_ops: measured.point_operations as f64 / elapsed_seconds,
            point_p50_us,
            point_p99_us,
            scan_ops: measured.scan_operations,
            scan_throughput_ops: measured.scan_operations as f64 / elapsed_seconds,
            scan_p50_us,
            scan_p99_us,
            dram_hit_rate: dram_hits as f64 / operations,
            lower_tier_hit_rate: lower_tier_hits as f64 / operations,
            point_dram_hit_rate: ratio_or_zero(measured.point_dram_hits, measured.point_operations),
            scan_dram_hit_rate: ratio_or_zero(measured.scan_dram_hits, measured.scan_operations),
        })
    }
}

fn ratio_or_zero(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn operation_latency_us(
    operations: u64,
    latencies_ns: &mut [u64],
    operation_name: &str,
) -> Result<(f64, f64), String> {
    if operations == 0 {
        return Ok((0.0, 0.0));
    }
    let p50_ns = percentile(latencies_ns, 50)
        .ok_or_else(|| format!("measurement produced no {operation_name} latency samples"))?;
    let p99_ns = percentile(latencies_ns, 99)
        .ok_or_else(|| format!("measurement produced no {operation_name} latency samples"))?;
    Ok((p50_ns as f64 / 1_000.0, p99_ns as f64 / 1_000.0))
}

fn demand_hit_delta(before: &TierStats, after: &TierStats) -> Result<(u64, u64), String> {
    if before.tiers.len() != after.tiers.len()
        || before
            .tiers
            .iter()
            .zip(&after.tiers)
            .any(|(before_tier, after_tier)| before_tier.name != after_tier.name)
    {
        return Err("tier stats changed shape during measurement".to_owned());
    }
    let dram_hits = after
        .dram_hits
        .checked_sub(before.dram_hits)
        .ok_or_else(|| "DRAM hit counter regressed during measurement".to_owned())?;
    let lower_tier_hits = before.tiers.iter().zip(&after.tiers).try_fold(
        0_u64,
        |total, (before_tier, after_tier)| {
            let delta = after_tier
                .demand_hits
                .checked_sub(before_tier.demand_hits)
                .ok_or_else(|| {
                    format!(
                        "demand hit counter for tier '{}' regressed during measurement",
                        after_tier.name
                    )
                })?;
            total
                .checked_add(delta)
                .ok_or_else(|| "lower-tier demand hit count overflowed u64".to_owned())
        },
    )?;
    Ok((dram_hits, lower_tier_hits))
}

fn format_csv_row(row: &BenchRow) -> String {
    format!(
        concat!(
            "{},{:.3},{:.3},{:.3},{:.12},",
            "{},{:.3},{:.3},{:.3},",
            "{},{:.3},{:.3},{:.3},",
            "{:.9},{:.9},{:.9},{:.9}\n"
        ),
        row.fraction.label(),
        row.throughput_ops,
        row.p50_us,
        row.p99_us,
        row.cost_usd_per_1e6ops,
        row.point_ops,
        row.point_throughput_ops,
        row.point_p50_us,
        row.point_p99_us,
        row.scan_ops,
        row.scan_throughput_ops,
        row.scan_p50_us,
        row.scan_p99_us,
        row.dram_hit_rate,
        row.lower_tier_hit_rate,
        row.point_dram_hit_rate,
        row.scan_dram_hit_rate
    )
}

struct CsvOutput {
    writer: BufWriter<File>,
}

impl CsvOutput {
    fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create output directory '{}': {error}",
                    parent.display()
                )
            })?;
        }

        let file = File::create(path)
            .map_err(|error| format!("failed to open '{}': {error}", path.display()))?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(CSV_HEADER.as_bytes())
            .map_err(|error| format!("failed to write '{}': {error}", path.display()))?;
        writer
            .flush()
            .map_err(|error| format!("failed to flush '{}': {error}", path.display()))?;

        Ok(Self { writer })
    }

    fn append(&mut self, row: &BenchRow) -> Result<(), String> {
        self.writer
            .write_all(format_csv_row(row).as_bytes())
            .map_err(|error| format!("failed to append benchmark CSV row: {error}"))?;
        self.writer
            .flush()
            .map_err(|error| format!("failed to flush benchmark CSV row: {error}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DramFraction {
    numerator: u8,
    denominator: u8,
}

impl DramFraction {
    const fn new(tenths: u8) -> Self {
        Self {
            numerator: tenths,
            denominator: 10,
        }
    }

    const fn ratio(numerator: u8, denominator: u8) -> Self {
        Self {
            numerator,
            denominator,
        }
    }

    fn label(self) -> String {
        match (self.numerator, self.denominator) {
            (1, 2) => "0.5".to_owned(),
            (1, 4) => "0.25".to_owned(),
            (1, 8) => "0.125".to_owned(),
            (1, 16) => "0.0625".to_owned(),
            (1, 32) => "0.03125".to_owned(),
            (tenths, 10) => format!("{}.{:01}", tenths / 10, tenths % 10),
            _ => format!("{}/{}", self.numerator, self.denominator),
        }
    }

    fn dram_pages(self, dataset_pages: usize) -> Result<usize, String> {
        dataset_pages
            .checked_mul(usize::from(self.numerator))
            .map(|pages| (pages / usize::from(self.denominator)).max(1))
            .ok_or_else(|| "DRAM page count overflowed usize".to_owned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DatasetLayout {
    page_count: usize,
    tier_capacity_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct S3BenchConfig {
    bucket: String,
    region: String,
    endpoint: Option<String>,
    key_prefix: String,
    compression: bool,
    capacity_mib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    dataset_mib: u64,
    warmup: Duration,
    measurement: Duration,
    workers: usize,
    mock_latency_us: u64,
    prefetch_scan: bool,
    prefetch_workers: usize,
    prefetch_in_flight: usize,
    scan_only: bool,
    file_tier: Option<PathBuf>,
    s3: Option<S3BenchConfig>,
    output: PathBuf,
    stats_output: PathBuf,
    fractions: Vec<DramFraction>,
    eviction_mode: EvictionMode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dataset_mib: DEFAULT_DATASET_MIB,
            warmup: DEFAULT_WARMUP,
            measurement: DEFAULT_MEASUREMENT,
            workers: DEFAULT_WORKERS,
            mock_latency_us: DEFAULT_MOCK_LATENCY_US,
            prefetch_scan: false,
            prefetch_workers: DEFAULT_PREFETCH_WORKERS,
            prefetch_in_flight: DEFAULT_PREFETCH_IN_FLIGHT,
            scan_only: false,
            file_tier: None,
            s3: None,
            output: PathBuf::from(DEFAULT_OUTPUT),
            stats_output: PathBuf::from(DEFAULT_STATS_OUTPUT),
            fractions: DRAM_FRACTIONS.to_vec(),
            eviction_mode: EvictionMode::Demand,
        }
    }
}

impl Config {
    fn quick() -> Self {
        Self {
            dataset_mib: 256,
            warmup: Duration::from_secs(1),
            measurement: Duration::from_secs(5),
            ..Self::default()
        }
    }

    fn ci() -> Self {
        Self {
            dataset_mib: 32,
            warmup: Duration::from_millis(100),
            measurement: Duration::from_millis(500),
            ..Self::default()
        }
    }

    fn dataset_layout(&self) -> Result<DatasetLayout, String> {
        let dataset_bytes = self
            .dataset_mib
            .checked_mul(MIB)
            .ok_or_else(|| "--dataset-mib is too large".to_owned())?;
        let tier_capacity_bytes = dataset_bytes
            .checked_mul(2)
            .ok_or_else(|| "--dataset-mib is too large for a 2x lower tier".to_owned())?;
        let dataset_bytes_usize = usize::try_from(dataset_bytes)
            .map_err(|_| "--dataset-mib does not fit this platform's address space".to_owned())?;
        let page_count = dataset_bytes_usize / PAGE_SIZE;
        if page_count == 0 || dataset_bytes_usize % PAGE_SIZE != 0 {
            return Err(
                "--dataset-mib must describe at least one complete tierbuf page".to_owned(),
            );
        }
        Ok(DatasetLayout {
            page_count,
            tier_capacity_bytes,
        })
    }

    fn validate(&self) -> Result<(), String> {
        if self.dataset_mib == 0 {
            return Err("--dataset-mib must be greater than zero".to_owned());
        }
        if self.warmup.is_zero() {
            return Err("--warmup-secs must be greater than zero".to_owned());
        }
        if self.measurement.is_zero() {
            return Err("--measure-secs must be greater than zero".to_owned());
        }
        if self.workers == 0 {
            return Err("--workers must be greater than zero".to_owned());
        }
        if !(1..=256).contains(&self.prefetch_workers) {
            return Err("--prefetch-workers must be within 1..=256".to_owned());
        }
        if self.prefetch_in_flight < self.prefetch_workers || self.prefetch_in_flight > 4096 {
            return Err("--prefetch-in-flight must be within prefetch-workers..=4096".to_owned());
        }
        if self.output.as_os_str().is_empty() {
            return Err("--output must not be empty".to_owned());
        }
        if self.stats_output.as_os_str().is_empty() {
            return Err("--stats-output must not be empty".to_owned());
        }
        if self.fractions.is_empty() {
            return Err("at least one DRAM fraction must be selected".to_owned());
        }
        if self
            .file_tier
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err("--file-tier must not be empty".to_owned());
        }
        if let Some(s3) = &self.s3 {
            if s3.bucket.is_empty() {
                return Err("--s3-bucket must not be empty".to_owned());
            }
            if s3.region.is_empty() {
                return Err("--s3-region must not be empty".to_owned());
            }
            if s3.endpoint.as_ref().is_some_and(String::is_empty) {
                return Err("--s3-endpoint must not be empty".to_owned());
            }
            if s3.key_prefix.is_empty() || s3.key_prefix.starts_with('/') {
                return Err("--s3-prefix must be non-empty and bucket-relative".to_owned());
            }
            if s3.capacity_mib == 0 {
                return Err("--s3-capacity-mib must be greater than zero".to_owned());
            }
            s3.capacity_mib
                .checked_mul(MIB)
                .ok_or_else(|| "--s3-capacity-mib is too large".to_owned())?;
        }
        self.dataset_layout().map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Profile {
    Quick,
    Ci,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CliAction {
    Help,
    Run(Box<Config>),
}

fn parse_cli<I, S>(arguments: I) -> Result<CliAction, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut arguments = arguments.into_iter().map(Into::into);
    let mut profile = None;
    let mut dataset_mib = None;
    let mut warmup = None;
    let mut measurement = None;
    let mut workers = None;
    let mut mock_latency_us = None;
    let mut prefetch_workers = None;
    let mut prefetch_in_flight = None;
    let mut prefetch_scan = false;
    let mut scan_only = false;
    let mut file_tier = None;
    let mut s3_bucket = None;
    let mut s3_region = None;
    let mut s3_endpoint = None;
    let mut s3_prefix = None;
    let mut s3_compression = None;
    let mut s3_capacity_mib = None;
    let mut output = None;
    let mut stats_output = None;
    let mut fraction = None;
    let mut eviction_mode = None;

    while let Some(argument) = arguments.next() {
        if argument == "-h" || argument == "--help" {
            return Ok(CliAction::Help);
        }
        if argument == "--quick" {
            set_profile(&mut profile, Profile::Quick)?;
            continue;
        }
        if argument == "--ci" {
            set_profile(&mut profile, Profile::Ci)?;
            continue;
        }
        if argument == "--prefetch-scan" {
            prefetch_scan = true;
            continue;
        }
        if argument == "--scan-only" {
            scan_only = true;
            continue;
        }

        let (flag, inline_value) = match argument.split_once('=') {
            Some((flag, value)) => (flag, Some(value.to_owned())),
            None => (argument.as_str(), None),
        };
        let mut next_value = || {
            inline_value
                .clone()
                .or_else(|| arguments.next())
                .ok_or_else(|| format!("{flag} requires a value"))
        };

        match flag {
            "--dataset-mib" => {
                dataset_mib = Some(parse_u64(&next_value()?, flag)?);
            }
            "--warmup-secs" => {
                warmup = Some(parse_duration(&next_value()?, flag)?);
            }
            "--measure-secs" => {
                measurement = Some(parse_duration(&next_value()?, flag)?);
            }
            "--workers" => {
                workers = Some(parse_usize(&next_value()?, flag)?);
            }
            "--mock-latency-us" => {
                mock_latency_us = Some(parse_u64(&next_value()?, flag)?);
            }
            "--prefetch-workers" => {
                prefetch_workers = Some(parse_usize(&next_value()?, flag)?);
            }
            "--prefetch-in-flight" => {
                prefetch_in_flight = Some(parse_usize(&next_value()?, flag)?);
            }
            "--file-tier" => {
                file_tier = Some(PathBuf::from(next_value()?));
            }
            "--s3-bucket" => {
                s3_bucket = Some(next_value()?);
            }
            "--s3-region" => {
                s3_region = Some(next_value()?);
            }
            "--s3-endpoint" => {
                s3_endpoint = Some(next_value()?);
            }
            "--s3-prefix" => {
                s3_prefix = Some(next_value()?);
            }
            "--s3-compression" => {
                s3_compression = Some(parse_s3_compression(&next_value()?, flag)?);
            }
            "--s3-capacity-mib" => {
                s3_capacity_mib = Some(parse_u64(&next_value()?, flag)?);
            }
            "--output" => {
                output = Some(PathBuf::from(next_value()?));
            }
            "--stats-output" => {
                stats_output = Some(PathBuf::from(next_value()?));
            }
            "--fraction" => {
                if fraction.is_some() {
                    return Err("--fraction may be specified only once".to_owned());
                }
                fraction = Some(parse_fraction(&next_value()?, flag)?);
            }
            "--eviction-mode" => {
                eviction_mode = Some(parse_eviction_mode(&next_value()?, flag)?);
            }
            _ => {
                return Err(format!(
                    "unknown argument '{argument}'; use --help for usage"
                ));
            }
        }
    }

    if s3_bucket.is_none() {
        for (flag, supplied) in [
            ("--s3-region", s3_region.is_some()),
            ("--s3-endpoint", s3_endpoint.is_some()),
            ("--s3-prefix", s3_prefix.is_some()),
            ("--s3-compression", s3_compression.is_some()),
            ("--s3-capacity-mib", s3_capacity_mib.is_some()),
        ] {
            if supplied {
                return Err(format!("{flag} requires --s3-bucket"));
            }
        }
    }

    let mut config = match profile {
        Some(Profile::Quick) => Config::quick(),
        Some(Profile::Ci) => Config::ci(),
        None => Config::default(),
    };
    if let Some(value) = dataset_mib {
        config.dataset_mib = value;
    }
    if let Some(value) = warmup {
        config.warmup = value;
    }
    if let Some(value) = measurement {
        config.measurement = value;
    }
    if let Some(value) = workers {
        config.workers = value;
    }
    if let Some(value) = mock_latency_us {
        config.mock_latency_us = value;
    }
    let has_s3 = s3_bucket.is_some();
    config.prefetch_workers = prefetch_workers.unwrap_or(if has_s3 {
        DEFAULT_S3_PREFETCH_WORKERS
    } else {
        DEFAULT_PREFETCH_WORKERS
    });
    config.prefetch_in_flight = prefetch_in_flight.unwrap_or(if has_s3 {
        DEFAULT_S3_PREFETCH_IN_FLIGHT
    } else {
        DEFAULT_PREFETCH_IN_FLIGHT
    });
    config.prefetch_scan = prefetch_scan;
    config.scan_only = scan_only;
    if let Some(value) = file_tier {
        config.file_tier = Some(value);
    }
    if let Some(bucket) = s3_bucket {
        let capacity_mib = match s3_capacity_mib {
            Some(capacity_mib) => capacity_mib,
            None => config.dataset_mib.checked_mul(2).ok_or_else(|| {
                "--dataset-mib is too large for the default S3 capacity".to_owned()
            })?,
        };
        config.s3 = Some(S3BenchConfig {
            bucket,
            region: s3_region.unwrap_or_else(|| DEFAULT_S3_REGION.to_owned()),
            endpoint: s3_endpoint,
            key_prefix: s3_prefix.unwrap_or_else(default_s3_prefix),
            compression: s3_compression.unwrap_or(true),
            capacity_mib,
        });
    }
    if let Some(value) = output {
        config.output = value;
    }
    if let Some(value) = stats_output {
        config.stats_output = value;
    }
    if let Some(value) = fraction {
        config.fractions = vec![value];
    }
    if let Some(value) = eviction_mode {
        config.eviction_mode = value;
    }
    config.validate()?;
    Ok(CliAction::Run(Box::new(config)))
}

fn parse_fraction(value: &str, flag: &str) -> Result<DramFraction, String> {
    let fraction = DRAM_FRACTIONS
        .iter()
        .copied()
        .find(|fraction| fraction.label() == value)
        .or_else(|| match value {
            "0.5" => Some(DramFraction::ratio(1, 2)),
            "0.25" => Some(DramFraction::ratio(1, 4)),
            "0.125" => Some(DramFraction::ratio(1, 8)),
            "0.0625" => Some(DramFraction::ratio(1, 16)),
            "0.03125" => Some(DramFraction::ratio(1, 32)),
            _ => None,
        });
    fraction.ok_or_else(|| {
        format!(
            "{flag} expects 1.0, 0.8, 0.6, 0.5, 0.4, 0.25, 0.2, 0.125, \
             0.1, 0.0625, or 0.03125; got '{value}'"
        )
    })
}

fn parse_s3_compression(value: &str, flag: &str) -> Result<bool, String> {
    match value {
        "on" => Ok(true),
        "off" => Ok(false),
        _ => Err(format!("{flag} expects 'on' or 'off'; got '{value}'")),
    }
}

fn default_s3_prefix() -> String {
    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("tierbuf-bench/{unix_secs}-{}/", std::process::id())
}

fn parse_eviction_mode(value: &str, flag: &str) -> Result<EvictionMode, String> {
    match value {
        "demand" => Ok(EvictionMode::Demand),
        "watermark" => Ok(EvictionMode::Watermark),
        _ => Err(format!(
            "{flag} expects 'demand' or 'watermark'; got '{value}'"
        )),
    }
}

fn set_profile(selected: &mut Option<Profile>, requested: Profile) -> Result<(), String> {
    match *selected {
        Some(current) if current != requested => {
            Err("--quick and --ci are mutually exclusive".to_owned())
        }
        _ => {
            *selected = Some(requested);
            Ok(())
        }
    }
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} expects a non-negative integer, got '{value}'"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} expects a non-negative integer, got '{value}'"))
}

fn parse_duration(value: &str, flag: &str) -> Result<Duration, String> {
    let seconds: f64 = value
        .parse()
        .map_err(|_| format!("{flag} expects seconds as a number, got '{value}'"))?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(format!("{flag} must be finite and greater than zero"));
    }
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| format!("{flag} is outside the supported duration range"))
}

fn print_usage() {
    println!(
        "\
Usage: tierbuf-bench [OPTIONS]

Profiles:
    --quick                 256 MiB, 1s warmup, 5s measurement
    --ci                    32 MiB, 100ms warmup, 500ms measurement

Options:
    --dataset-mib N         Logical dataset size in MiB (default: 4096)
    --warmup-secs SECONDS   Warmup duration; decimals are accepted (default: 10)
    --measure-secs SECONDS  Measurement duration; decimals are accepted (default: 30)
    --workers N             Worker threads (default: 4)
    --mock-latency-us N     MockTier read/write delay in microseconds (default: 80)
    --prefetch-scan         Prefetch eight pages ahead during sequential scans
    --prefetch-workers N    Background prefetch workers (default: S3 64, otherwise 4)
    --prefetch-in-flight N  Queued-plus-running prefetch limit (default: S3 256, otherwise 128)
    --scan-only             Run the sequential scan component only
    --file-tier PATH        Use a reusable FileTier; precedes S3 when both are set
    --s3-bucket NAME        Enable an S3 tier for this bucket
    --s3-region REGION      S3 signing region (default: us-east-1)
    --s3-endpoint URL       Custom path-style endpoint for MinIO or LocalStack
    --s3-prefix PREFIX      Object prefix (default: tierbuf-bench/{{unix_secs}}-{{pid}}/)
    --s3-compression MODE   S3 envelope compression: on (default) or off
    --s3-capacity-mib N     Logical S3 capacity in MiB (default: 2x dataset)
    --output PATH           CSV output path, replaced each run (default: results/curve.csv)
    --stats-output PATH     JSON stats output path (default: results/stats.json)
    --fraction FRACTION     Run one supported DRAM fraction instead of the default sweep
    --eviction-mode MODE    Background eviction mode: demand (default) or watermark
    -h, --help              Show this help

Explicit sizing and duration options override the selected profile."
    );
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    use tierbuf::PAGE_SIZE;
    use tierbuf::metrics::{TierCounters, TierStats};
    use tierbuf::policy::AccessHint;
    use tierbuf::pool::{BufConfig, BufferManager};
    use tierbuf::tier::mock::MockTier;
    use tierbuf::tier::s3::S3TierStats;
    use tierbuf::tier::s3::client::MemoryObjectApi;
    use tierbuf::tier::s3::credentials::CredentialSource;
    use tierbuf::tier::{LatencyProfile, TierBackend, TierOffset, WriteBudget};

    use super::{
        BenchRow, CSV_HEADER, CliAction, Config, CsvOutput, DramFraction, EvictionMode,
        POINT_ACCESS_PERCENT, PhaseResult, TierKind, WorkloadState, ZipfSampler, format_csv_row,
        fraction_stats_json, initialize_dataset_parallel, page_marker, parallel_collect_ordered,
        parse_cli, percentile, s3_client_config, s3_stats_json, smoke_object_api, tier_plan,
    };

    #[derive(Clone, Default)]
    struct WriteConcurrencyProbe {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    struct SlowWriteTier {
        inner: MockTier,
        probe: WriteConcurrencyProbe,
    }

    impl TierBackend for SlowWriteTier {
        fn name(&self) -> &str {
            self.inner.name()
        }

        fn read(&self, location: TierOffset, buffer: &mut [u8]) -> tierbuf::Result<()> {
            self.inner.read(location, buffer)
        }

        fn write(&self, buffer: &[u8]) -> tierbuf::Result<TierOffset> {
            let active = self.probe.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.probe.peak.fetch_max(active, Ordering::AcqRel);
            thread::sleep(Duration::from_millis(5));
            let result = self.inner.write(buffer);
            self.probe.active.fetch_sub(1, Ordering::AcqRel);
            result
        }

        fn free(&self, location: TierOffset) {
            self.inner.free(location);
        }

        fn capacity_bytes(&self) -> u64 {
            self.inner.capacity_bytes()
        }

        fn used_bytes(&self) -> u64 {
            self.inner.used_bytes()
        }

        fn price_gb_month(&self) -> f64 {
            self.inner.price_gb_month()
        }

        fn write_budget(&self) -> Option<&WriteBudget> {
            self.inner.write_budget()
        }

        fn latency(&self) -> LatencyProfile {
            self.inner.latency()
        }
    }

    fn run_config(arguments: &[&str]) -> Config {
        match parse_cli(arguments.iter().copied()).expect("valid CLI") {
            CliAction::Run(config) => *config,
            CliAction::Help => panic!("expected a runnable configuration"),
        }
    }

    #[test]
    fn cli_defaults_and_profiles_are_exact() {
        assert_eq!(run_config(&[]), Config::default());

        let quick = run_config(&["--quick"]);
        assert_eq!(quick.dataset_mib, 256);
        assert_eq!(quick.warmup, Duration::from_secs(1));
        assert_eq!(quick.measurement, Duration::from_secs(5));
        assert_eq!(quick.workers, 4);
        assert_eq!(quick.prefetch_workers, 4);
        assert_eq!(quick.prefetch_in_flight, 128);

        let ci = run_config(&["--ci"]);
        assert_eq!(ci.dataset_mib, 32);
        assert_eq!(ci.warmup, Duration::from_millis(100));
        assert_eq!(ci.measurement, Duration::from_millis(500));
        assert_eq!(ci.workers, 4);
    }

    #[test]
    fn cli_accepts_overrides_and_rejects_invalid_values() {
        let config = run_config(&[
            "--quick",
            "--dataset-mib=64",
            "--warmup-secs",
            "0.25",
            "--measure-secs=0.75",
            "--workers",
            "2",
            "--mock-latency-us=7",
            "--prefetch-scan",
            "--prefetch-workers=8",
            "--prefetch-in-flight=64",
            "--scan-only",
            "--file-tier",
            "/tmp/tierbuf-bench-test.bin",
            "--output",
            "custom.csv",
            "--stats-output",
            "custom.json",
            "--fraction",
            "0.6",
            "--eviction-mode",
            "watermark",
        ]);
        assert_eq!(config.dataset_mib, 64);
        assert_eq!(config.warmup, Duration::from_millis(250));
        assert_eq!(config.measurement, Duration::from_millis(750));
        assert_eq!(config.workers, 2);
        assert_eq!(config.mock_latency_us, 7);
        assert!(config.prefetch_scan);
        assert_eq!(config.prefetch_workers, 8);
        assert_eq!(config.prefetch_in_flight, 64);
        assert!(config.scan_only);
        assert_eq!(
            config.file_tier,
            Some(PathBuf::from("/tmp/tierbuf-bench-test.bin"))
        );
        assert_eq!(config.output, PathBuf::from("custom.csv"));
        assert_eq!(config.stats_output, PathBuf::from("custom.json"));
        assert_eq!(config.fractions, vec![DramFraction::new(6)]);
        assert_eq!(config.eviction_mode, EvictionMode::Watermark);

        for invalid in [
            vec!["--quick", "--ci"],
            vec!["--dataset-mib", "0"],
            vec!["--warmup-secs", "NaN"],
            vec!["--measure-secs", "0"],
            vec!["--workers", "0"],
            vec!["--prefetch-workers", "0"],
            vec!["--prefetch-workers", "129", "--prefetch-in-flight", "128"],
            vec!["--prefetch-in-flight", "4097"],
            vec!["--file-tier="],
            vec!["--output="],
            vec!["--stats-output="],
            vec!["--fraction", "0.333"],
            vec!["--eviction-mode", "periodic"],
            vec!["--unknown"],
            vec!["--workers"],
        ] {
            assert!(
                parse_cli(invalid.iter().copied()).is_err(),
                "arguments should be rejected: {invalid:?}"
            );
        }
    }

    #[test]
    fn s3_cli_flags_round_trip_and_select_prefetch_defaults() {
        let config = run_config(&[
            "--dataset-mib",
            "64",
            "--s3-bucket",
            "bench-bucket",
            "--s3-region",
            "ap-northeast-1",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-prefix",
            "custom/run",
            "--s3-compression",
            "off",
            "--s3-capacity-mib",
            "256",
        ]);
        let s3 = config.s3.as_ref().expect("S3 configuration");
        assert_eq!(s3.bucket, "bench-bucket");
        assert_eq!(s3.region, "ap-northeast-1");
        assert_eq!(s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(s3.key_prefix, "custom/run");
        assert!(!s3.compression);
        assert_eq!(s3.capacity_mib, 256);
        assert_eq!(config.prefetch_workers, 64);
        assert_eq!(config.prefetch_in_flight, 256);

        let defaults = run_config(&["--s3-bucket", "bench-bucket"]);
        let default_s3 = defaults.s3.as_ref().expect("default S3 configuration");
        assert_eq!(default_s3.region, "us-east-1");
        assert!(default_s3.endpoint.is_none());
        assert!(default_s3.compression);
        assert_eq!(default_s3.capacity_mib, defaults.dataset_mib * 2);
        assert!(default_s3.key_prefix.starts_with("tierbuf-bench/"));
        assert!(default_s3.key_prefix.ends_with('/'));

        let explicit_prefetch = run_config(&[
            "--s3-bucket",
            "bench-bucket",
            "--prefetch-workers",
            "12",
            "--prefetch-in-flight",
            "48",
        ]);
        assert_eq!(explicit_prefetch.prefetch_workers, 12);
        assert_eq!(explicit_prefetch.prefetch_in_flight, 48);
    }

    #[test]
    fn s3_only_flags_require_a_bucket_and_compression_is_strict() {
        for invalid in [
            vec!["--s3-region", "us-west-2"],
            vec!["--s3-endpoint", "http://127.0.0.1:9000"],
            vec!["--s3-prefix", "run/"],
            vec!["--s3-compression", "off"],
            vec!["--s3-capacity-mib", "32"],
            vec!["--s3-bucket", "bucket", "--s3-compression", "maybe"],
            vec!["--s3-bucket="],
            vec!["--s3-bucket", "bucket", "--s3-prefix="],
            vec!["--s3-bucket", "bucket", "--s3-capacity-mib", "0"],
        ] {
            assert!(
                parse_cli(invalid.iter().copied()).is_err(),
                "arguments should be rejected: {invalid:?}"
            );
        }
    }

    #[test]
    fn tier_plan_preserves_file_then_s3_order() {
        assert_eq!(tier_plan(&run_config(&[])), vec![TierKind::Mock]);
        assert_eq!(
            tier_plan(&run_config(&["--file-tier", "/tmp/tierbuf-file"])),
            vec![TierKind::File]
        );
        assert_eq!(
            tier_plan(&run_config(&["--s3-bucket", "bucket"])),
            vec![TierKind::S3]
        );
        assert_eq!(
            tier_plan(&run_config(&[
                "--file-tier",
                "/tmp/tierbuf-file",
                "--s3-bucket",
                "bucket",
            ])),
            vec![TierKind::File, TierKind::S3]
        );
    }

    #[test]
    fn s3_client_config_uses_auto_credentials() {
        let config = run_config(&[
            "--s3-bucket",
            "bucket",
            "--s3-region",
            "us-west-2",
            "--s3-endpoint",
            "http://localhost:9000",
        ]);
        let client = s3_client_config(config.s3.as_ref().expect("S3 configuration"));
        assert_eq!(client.bucket, "bucket");
        assert_eq!(client.region, "us-west-2");
        assert_eq!(client.endpoint.as_deref(), Some("http://localhost:9000"));
        assert!(matches!(client.credentials, CredentialSource::Auto));
    }

    #[test]
    fn demo_fractions_parse_and_size_exactly() {
        for (value, expected_pages) in [
            ("1.0", 1_024),
            ("0.5", 512),
            ("0.25", 256),
            ("0.125", 128),
            ("0.0625", 64),
            ("0.03125", 32),
        ] {
            let fraction = run_config(&["--fraction", value]).fractions[0];
            assert_eq!(fraction.label(), value);
            assert_eq!(
                fraction.dram_pages(1_024).expect("valid page count"),
                expected_pages
            );
        }
    }

    #[test]
    fn object_api_smoke_round_trips_without_network() {
        let api = MemoryObjectApi::new();
        smoke_object_api(&api, "bench").expect("smoke test");
        let stats = api.snapshot();
        assert_eq!(stats.put_requests, 1);
        assert_eq!(stats.get_requests, 1);
        assert_eq!(stats.delete_requests, 1);
    }

    #[test]
    fn parallel_collection_preserves_order_and_lowest_index_error() {
        let values = parallel_collect_ordered(32, 8, |index| {
            thread::sleep(Duration::from_micros(
                u64::try_from(31 - index).expect("small index") * 10,
            ));
            Ok(index * 3)
        })
        .expect("parallel collection");
        assert_eq!(values, (0..32).map(|index| index * 3).collect::<Vec<_>>());

        let error = parallel_collect_ordered::<usize, _>(32, 8, |index| match index {
            3 => {
                thread::sleep(Duration::from_millis(5));
                Err("logical page 3 failed".to_owned())
            }
            11 => Err("logical page 11 failed".to_owned()),
            _ => Ok(index),
        })
        .expect_err("injected errors must propagate");
        assert_eq!(error, "logical page 3 failed");
    }

    #[test]
    fn parallel_dataset_initialization_preserves_markers_and_swip_order() {
        let manager = BufferManager::new(BufConfig {
            dram_pool_bytes: 8 * PAGE_SIZE,
            tiers: vec![Box::new(
                MockTier::new(128 * PAGE_SIZE as u64).expect("mock tier"),
            )],
            prefetch_workers: 4,
            max_prefetch_in_flight: 16,
            ..BufConfig::default()
        })
        .expect("buffer manager");

        let swips = initialize_dataset_parallel(&manager, 64, 8).expect("parallel initialization");
        assert_eq!(swips.len(), 64);
        for (index, swip) in swips.iter().enumerate() {
            let guard = manager.fix_shared(swip).expect("initialized page");
            assert_eq!(guard.read_with(|page| page[0]), page_marker(index));
        }

        Arc::clone(&manager).shutdown().expect("clean shutdown");
    }

    #[test]
    fn parallel_dataset_initialization_drives_bounded_concurrent_tier_writes() {
        let probe = WriteConcurrencyProbe::default();
        let manager = BufferManager::new(BufConfig {
            dram_pool_bytes: 8 * PAGE_SIZE,
            cooling_ratio: 0.0,
            tiers: vec![Box::new(SlowWriteTier {
                inner: MockTier::new(128 * PAGE_SIZE as u64).expect("mock tier"),
                probe: probe.clone(),
            })],
            prefetch_workers: 4,
            max_prefetch_in_flight: 16,
            ..BufConfig::default()
        })
        .expect("buffer manager");

        let swips = initialize_dataset_parallel(&manager, 64, 8).expect("parallel initialization");
        assert_eq!(swips.len(), 64);
        let peak = probe.peak.load(Ordering::Acquire);
        assert!(peak > 1, "expected concurrent tier writes, observed {peak}");
        assert!(peak <= 8, "initializer exceeded its worker bound: {peak}");

        Arc::clone(&manager).shutdown().expect("clean shutdown");
    }

    #[test]
    fn s3_stats_json_reports_compression_and_zero_logical_as_null() {
        let value = s3_stats_json(S3TierStats {
            get_requests: 3,
            put_requests: 4,
            delete_requests: 1,
            request_failures: 0,
            stored_bytes: 32,
            logical_bytes: 64,
            bytes_uploaded: 96,
            bytes_downloaded: 64,
        });
        assert_eq!(value["get_requests"], 3);
        assert_eq!(value["compression_ratio"], 0.5);

        let empty = s3_stats_json(S3TierStats::default());
        assert!(empty["compression_ratio"].is_null());
    }

    #[test]
    fn fraction_stats_embed_s3_at_the_run_top_level() {
        let before = test_stats(1, 2);
        let after = test_stats(3, 5);
        let measured = PhaseResult {
            operations: 5,
            point_operations: 3,
            scan_operations: 2,
            point_dram_hits: 1,
            scan_dram_hits: 1,
            elapsed: Duration::from_secs(1),
            latencies_ns: Vec::new(),
            point_latencies_ns: Vec::new(),
            scan_latencies_ns: Vec::new(),
        };
        let value = fraction_stats_json(
            DramFraction::ratio(1, 2),
            &measured,
            &before,
            &after,
            Some(S3TierStats {
                stored_bytes: 16,
                logical_bytes: 64,
                ..S3TierStats::default()
            }),
        )
        .expect("valid stats delta");

        assert_eq!(value["fraction"], "0.5");
        assert_eq!(value["s3"]["stored_bytes"], 16);
        assert_eq!(value["s3"]["compression_ratio"], 0.25);
    }

    #[test]
    fn csv_output_replaces_a_previous_run() {
        let path =
            std::env::temp_dir().join(format!("tierbuf-bench-output-{}.csv", std::process::id()));
        fs::write(&path, "stale data\n").expect("seed stale output");
        let row = BenchRow {
            fraction: DramFraction::new(10),
            throughput_ops: 12.0,
            p50_us: 3.0,
            p99_us: 4.0,
            cost_usd_per_1e6ops: 5.0,
            point_ops: 8,
            point_throughput_ops: 8.0,
            point_p50_us: 2.0,
            point_p99_us: 3.0,
            scan_ops: 4,
            scan_throughput_ops: 4.0,
            scan_p50_us: 4.0,
            scan_p99_us: 5.0,
            dram_hit_rate: 0.75,
            lower_tier_hit_rate: 0.25,
            point_dram_hit_rate: 0.875,
            scan_dram_hit_rate: 0.5,
        };
        let mut output = CsvOutput::open(&path).expect("replace output");
        output.append(&row).expect("write row");
        drop(output);

        let contents = fs::read_to_string(&path).expect("read output");
        let _ = fs::remove_file(path);
        assert_eq!(contents, format!("{CSV_HEADER}{}", format_csv_row(&row)));
    }

    #[test]
    fn fixed_seed_produces_deterministic_index_selection() {
        let zipf = ZipfSampler::new(128).expect("valid sampler");
        let mut first = WorkloadState::new(0x1234_5678, 37);
        let mut second = WorkloadState::new(0x1234_5678, 37);

        let first_sequence = (0..256)
            .map(|_| first.next_access(&zipf, POINT_ACCESS_PERCENT))
            .collect::<Vec<_>>();
        let second_sequence = (0..256)
            .map(|_| second.next_access(&zipf, POINT_ACCESS_PERCENT))
            .collect::<Vec<_>>();

        assert_eq!(first_sequence, second_sequence);
        assert!(first_sequence.iter().all(|access| access.index < 128));
        assert!(
            first_sequence
                .iter()
                .any(|access| access.hint == AccessHint::Scan)
        );
        assert!(
            first_sequence
                .iter()
                .any(|access| access.hint == AccessHint::Normal)
        );
    }

    #[test]
    fn percentile_uses_nearest_rank_and_handles_empty_input() {
        assert_eq!(percentile(&mut [], 50), None);
        let mut samples = [40, 10, 30, 20];
        assert_eq!(percentile(&mut samples, 50), Some(20));
        assert_eq!(percentile(&mut samples, 99), Some(40));
        assert_eq!(percentile(&mut samples, 100), Some(40));
    }

    #[test]
    fn benchmark_row_separates_operation_types_and_demand_hit_tiers() {
        let before = test_stats(10, 5);
        let after = test_stats(16, 9);
        let measured = PhaseResult {
            operations: 10,
            point_operations: 7,
            scan_operations: 3,
            point_dram_hits: 5,
            scan_dram_hits: 1,
            elapsed: Duration::from_secs(2),
            latencies_ns: vec![1_000, 2_000, 3_000],
            point_latencies_ns: vec![1_000, 2_000],
            scan_latencies_ns: vec![4_000, 5_000],
        };

        let row =
            BenchRow::from_measurement(DramFraction::new(8), measured, 0.000_001, &before, &after)
                .expect("consistent measurement");

        assert_eq!(row.point_ops, 7);
        assert_eq!(row.scan_ops, 3);
        assert_eq!(row.point_throughput_ops, 3.5);
        assert_eq!(row.scan_throughput_ops, 1.5);
        assert_eq!(row.dram_hit_rate, 0.6);
        assert_eq!(row.lower_tier_hit_rate, 0.4);
        assert_eq!(row.point_dram_hit_rate, 5.0 / 7.0);
        assert_eq!(row.scan_dram_hit_rate, 1.0 / 3.0);
    }

    #[test]
    fn csv_row_format_is_stable_and_valid() {
        let row = BenchRow {
            fraction: DramFraction::new(8),
            throughput_ops: 1234.5,
            p50_us: 4.25,
            p99_us: 99.75,
            cost_usd_per_1e6ops: 0.000_012_345,
            point_ops: 700,
            point_throughput_ops: 864.15,
            point_p50_us: 1.25,
            point_p99_us: 12.5,
            scan_ops: 300,
            scan_throughput_ops: 370.35,
            scan_p50_us: 8.75,
            scan_p99_us: 150.25,
            dram_hit_rate: 0.8,
            lower_tier_hit_rate: 0.2,
            point_dram_hit_rate: 0.9,
            scan_dram_hit_rate: 0.566_666_667,
        };

        assert_eq!(
            format_csv_row(&row),
            concat!(
                "0.8,1234.500,4.250,99.750,0.000012345000,",
                "700,864.150,1.250,12.500,",
                "300,370.350,8.750,150.250,",
                "0.800000000,0.200000000,0.900000000,0.566666667\n"
            )
        );
    }

    fn test_stats(dram_hits: u64, lower_tier_hits: u64) -> TierStats {
        TierStats {
            dram_hits,
            faults: lower_tier_hits,
            evictions: 0,
            second_chances: 0,
            prefetch_submitted: 0,
            prefetch_hits: 0,
            prefetch_skipped: 0,
            budget_denied: 0,
            tiers: vec![TierCounters {
                name: "mock".to_owned(),
                demand_hits: lower_tier_hits,
                reads: lower_tier_hits,
                writes: 0,
                bytes_read: lower_tier_hits * PAGE_SIZE as u64,
                bytes_written: 0,
            }],
        }
    }
}
