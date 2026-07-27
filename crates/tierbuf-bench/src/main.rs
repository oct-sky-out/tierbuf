#![deny(missing_docs)]
//! Degradation-curve benchmark harness for tierbuf.

use std::env;
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tierbuf::policy::AccessHint;
use tierbuf::pool::{BufConfig, BufferManager, Economics};
use tierbuf::swip::Swip;
use tierbuf::tier::file::{FileTier, FileTierConfig};
use tierbuf::tier::mock::MockTier;
use tierbuf::tier::{LatencyProfile, TierBackend};
use tierbuf::{PAGE_SIZE, TierBufError};

const MIB: u64 = 1024 * 1024;
const DEFAULT_DATASET_MIB: u64 = 4 * 1024;
const DEFAULT_WARMUP: Duration = Duration::from_secs(10);
const DEFAULT_MEASUREMENT: Duration = Duration::from_secs(30);
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_MOCK_LATENCY_US: u64 = 80;
const DEFAULT_OUTPUT: &str = "results/curve.csv";
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
const RESOURCE_RETRY_PAUSE: Duration = Duration::from_micros(100);
const CSV_HEADER: &str = "fraction,throughput_ops,p50_us,p99_us,cost_usd_per_1e6ops\n";
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
    let mut output = CsvOutput::open(&config.output)?;

    println!(
        "tierbuf degradation curve: {} MiB, {} workers, {:.3}s warmup, {:.3}s measurement",
        config.dataset_mib,
        config.workers,
        config.warmup.as_secs_f64(),
        config.measurement.as_secs_f64()
    );

    for fraction in DRAM_FRACTIONS {
        eprintln!(
            "fraction {}: initializing {} pages in {} DRAM frames",
            fraction.label(),
            layout.page_count,
            fraction.dram_pages(layout.page_count)?
        );
        let row = run_fraction(config, layout, fraction)?;
        output.append(&row)?;
        println!("{}", format_csv_row(&row).trim_end());
    }

    Ok(())
}

fn run_fraction(
    config: &Config,
    layout: DatasetLayout,
    fraction: DramFraction,
) -> Result<BenchRow, String> {
    let manager = make_manager(config, layout, fraction)?;

    let benchmark_result = (|| {
        let swips: Arc<[Swip]> = initialize_dataset(&manager, layout.page_count)?.into();
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
        let stats = manager.stats();
        eprintln!(
            "fraction {}: hits={} faults={} evictions={} prefetch={}/{} skipped={}",
            fraction.label(),
            stats.dram_hits,
            stats.faults,
            stats.evictions,
            stats.prefetch_hits,
            stats.prefetch_submitted,
            stats.prefetch_skipped
        );

        BenchRow::from_measurement(fraction, measured, (cost_after - cost_before).max(0.0))
    })();

    let shutdown_result = manager
        .shutdown()
        .map_err(|error| format!("failed to shut down fraction {}: {error}", fraction.label()));

    match (benchmark_result, shutdown_result) {
        (Ok(row), Ok(())) => Ok(row),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(benchmark_error), Err(shutdown_error)) => {
            Err(format!("{benchmark_error}; additionally, {shutdown_error}"))
        }
    }
}

fn make_manager(
    config: &Config,
    layout: DatasetLayout,
    fraction: DramFraction,
) -> Result<Arc<BufferManager>, String> {
    let latency = LatencyProfile::new(
        config.mock_latency_us,
        config.mock_latency_us,
        MOCK_SEQUENTIAL_GIB_PER_SECOND,
    );
    let tier: Box<dyn TierBackend> = if let Some(path) = &config.file_tier {
        let mut file_config = FileTierConfig::new(layout.tier_capacity_bytes);
        file_config.name = "file".to_owned();
        file_config.price_gb_month = MOCK_PRICE_GIB_MONTH;
        file_config.latency = latency;
        Box::new(
            FileTier::open(path, file_config)
                .map_err(|error| format!("failed to create FileTier: {error}"))?,
        )
    } else {
        Box::new(
            MockTier::with_options(
                "mock",
                layout.tier_capacity_bytes,
                MOCK_PRICE_GIB_MONTH,
                latency,
                None,
            )
            .map_err(|error| format!("failed to create MockTier: {error}"))?,
        )
    };
    let dram_pages = fraction.dram_pages(layout.page_count)?;
    let dram_pool_bytes = dram_pages
        .checked_mul(PAGE_SIZE)
        .ok_or_else(|| "DRAM pool size overflowed usize".to_owned())?;

    BufferManager::new(BufConfig {
        dram_pool_bytes,
        cooling_ratio: 0.1,
        economics: Economics {
            dram_price_gb_month: DRAM_PRICE_GIB_MONTH,
            epoch: ECONOMIC_EPOCH,
        },
        tiers: vec![tier],
    })
    .map_err(|error| {
        format!(
            "failed to create buffer manager for fraction {}: {error}",
            fraction.label()
        )
    })
}

fn initialize_dataset(
    manager: &Arc<BufferManager>,
    page_count: usize,
) -> Result<Vec<Swip>, String> {
    let mut swips = Vec::with_capacity(page_count);

    for index in 0..page_count {
        let retry_deadline = Instant::now() + RESOURCE_RETRY_TIMEOUT;
        loop {
            match manager.allocate() {
                Ok(mut guard) => {
                    guard.write_with(|page| page[0] = page_marker(index));
                    swips.push(guard.swip());
                    break;
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

    Ok(swips)
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
    let mut latencies_ns = Vec::new();
    let mut first_error = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(worker)) => {
                operations = operations.saturating_add(worker.operations);
                latencies_ns.extend(worker.latencies_ns);
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
        elapsed: phase_start.elapsed(),
        latencies_ns,
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
    let mut operations = 0_u64;

    while Instant::now() < run.deadline {
        let point_access_percent = if run.scan_only {
            0
        } else {
            POINT_ACCESS_PERCENT
        };
        let access = workload.next_access(&zipf, point_access_percent);
        let should_sample = matches!(run.phase, Phase::Measurement)
            && operations.is_multiple_of(SAMPLE_EVERY_OPERATIONS);
        let operation_start = should_sample.then(Instant::now);

        fix_and_verify(&manager, &swips, access, run.prefetch_scan)?;

        if let Some(operation_start) = operation_start {
            latency_sampler.record(duration_as_nanos_u64(operation_start.elapsed()));
        }
        operations = operations.saturating_add(1);
    }

    Ok(WorkerResult {
        operations,
        latencies_ns: latency_sampler.into_values(),
    })
}

fn fix_and_verify(
    manager: &BufferManager,
    swips: &[Swip],
    access: AccessSelection,
    prefetch_scan: bool,
) -> Result<(), String> {
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
                return Ok(());
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
    latencies_ns: Vec<u64>,
}

#[derive(Debug)]
struct PhaseResult {
    operations: u64,
    elapsed: Duration,
    latencies_ns: Vec<u64>,
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
}

impl BenchRow {
    fn from_measurement(
        fraction: DramFraction,
        mut measured: PhaseResult,
        measured_cost_usd: f64,
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
        let operations = measured.operations as f64;

        Ok(Self {
            fraction,
            throughput_ops: operations / measured.elapsed.as_secs_f64(),
            p50_us: p50_ns as f64 / 1_000.0,
            p99_us: p99_ns as f64 / 1_000.0,
            cost_usd_per_1e6ops: measured_cost_usd * 1_000_000.0 / operations,
        })
    }
}

fn format_csv_row(row: &BenchRow) -> String {
    format!(
        "{},{:.3},{:.3},{:.3},{:.12}\n",
        row.fraction.label(),
        row.throughput_ops,
        row.p50_us,
        row.p99_us,
        row.cost_usd_per_1e6ops
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
    tenths: u8,
}

impl DramFraction {
    const fn new(tenths: u8) -> Self {
        Self { tenths }
    }

    fn label(self) -> String {
        format!("{}.{:01}", self.tenths / 10, self.tenths % 10)
    }

    fn dram_pages(self, dataset_pages: usize) -> Result<usize, String> {
        dataset_pages
            .checked_mul(usize::from(self.tenths))
            .map(|pages| (pages / 10).max(1))
            .ok_or_else(|| "DRAM page count overflowed usize".to_owned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DatasetLayout {
    page_count: usize,
    tier_capacity_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    dataset_mib: u64,
    warmup: Duration,
    measurement: Duration,
    workers: usize,
    mock_latency_us: u64,
    prefetch_scan: bool,
    scan_only: bool,
    file_tier: Option<PathBuf>,
    output: PathBuf,
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
            scan_only: false,
            file_tier: None,
            output: PathBuf::from(DEFAULT_OUTPUT),
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
            .ok_or_else(|| "--dataset-mib is too large for a 2x MockTier".to_owned())?;
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
        if self.output.as_os_str().is_empty() {
            return Err("--output must not be empty".to_owned());
        }
        if self
            .file_tier
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err("--file-tier must not be empty".to_owned());
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
    Run(Config),
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
    let mut prefetch_scan = false;
    let mut scan_only = false;
    let mut file_tier = None;
    let mut output = None;

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
            "--file-tier" => {
                file_tier = Some(PathBuf::from(next_value()?));
            }
            "--output" => {
                output = Some(PathBuf::from(next_value()?));
            }
            _ => {
                return Err(format!(
                    "unknown argument '{argument}'; use --help for usage"
                ));
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
    config.prefetch_scan = prefetch_scan;
    config.scan_only = scan_only;
    if let Some(value) = file_tier {
        config.file_tier = Some(value);
    }
    if let Some(value) = output {
        config.output = value;
    }
    config.validate()?;
    Ok(CliAction::Run(config))
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
    --scan-only             Run the sequential scan component only
    --file-tier PATH        Use one reusable FileTier path instead of MockTier
    --output PATH           CSV output path, replaced each run (default: results/curve.csv)
    -h, --help              Show this help

Explicit sizing and duration options override the selected profile."
    );
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::Duration;

    use tierbuf::policy::AccessHint;

    use super::{
        BenchRow, CSV_HEADER, CliAction, Config, CsvOutput, DramFraction, POINT_ACCESS_PERCENT,
        WorkloadState, ZipfSampler, format_csv_row, parse_cli, percentile,
    };

    fn run_config(arguments: &[&str]) -> Config {
        match parse_cli(arguments.iter().copied()).expect("valid CLI") {
            CliAction::Run(config) => config,
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
            "--scan-only",
            "--file-tier",
            "/tmp/tierbuf-bench-test.bin",
            "--output",
            "custom.csv",
        ]);
        assert_eq!(config.dataset_mib, 64);
        assert_eq!(config.warmup, Duration::from_millis(250));
        assert_eq!(config.measurement, Duration::from_millis(750));
        assert_eq!(config.workers, 2);
        assert_eq!(config.mock_latency_us, 7);
        assert!(config.prefetch_scan);
        assert!(config.scan_only);
        assert_eq!(
            config.file_tier,
            Some(PathBuf::from("/tmp/tierbuf-bench-test.bin"))
        );
        assert_eq!(config.output, PathBuf::from("custom.csv"));

        for invalid in [
            vec!["--quick", "--ci"],
            vec!["--dataset-mib", "0"],
            vec!["--warmup-secs", "NaN"],
            vec!["--measure-secs", "0"],
            vec!["--workers", "0"],
            vec!["--file-tier="],
            vec!["--output="],
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
    fn csv_row_format_is_stable_and_valid() {
        let row = BenchRow {
            fraction: DramFraction::new(8),
            throughput_ops: 1234.5,
            p50_us: 4.25,
            p99_us: 99.75,
            cost_usd_per_1e6ops: 0.000_012_345,
        };

        assert_eq!(
            format_csv_row(&row),
            "0.8,1234.500,4.250,99.750,0.000012345000\n"
        );
    }
}
