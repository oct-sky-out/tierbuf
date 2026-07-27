//! Exact operational counters and residency-cost accounting.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const SECONDS_PER_MONTH: f64 = 30.0 * 24.0 * 60.0 * 60.0;
const DRAM_TIER_NAME: &str = "dram";

/// Immutable counters for one lower storage tier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierCounters {
    /// Stable tier name supplied when the recorder was created.
    pub name: String,

    /// Completed demand fixes restored from this tier.
    ///
    /// Prefetch I/O is intentionally excluded so this counter can be used
    /// with `dram_hits` to calculate demand hit rates.
    pub demand_hits: u64,

    /// Completed page reads from this tier.
    pub reads: u64,

    /// Completed page writes to this tier.
    pub writes: u64,

    /// Bytes returned by completed reads.
    pub bytes_read: u64,

    /// Bytes persisted by completed writes.
    pub bytes_written: u64,
}

/// Immutable operational statistics for a buffer manager.
///
/// Snapshots are detached values and may be cloned, retained, or compared
/// without keeping the live recorder borrowed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierStats {
    /// Fix operations satisfied by an already-resident DRAM frame.
    pub dram_hits: u64,

    /// DRAM misses that entered page-fault processing.
    pub faults: u64,

    /// Frames successfully removed from DRAM.
    pub evictions: u64,

    /// Cooling frames restored to the hot state by a subsequent access.
    pub second_chances: u64,

    /// Page prefetch requests accepted for submission.
    pub prefetch_submitted: u64,

    /// Demand fixes satisfied by a page previously brought in by prefetch.
    pub prefetch_hits: u64,

    /// Prefetch requests skipped because they were ineligible or resources
    /// were unavailable.
    pub prefetch_skipped: u64,

    /// Demotion attempts denied by a tier's write budget.
    pub budget_denied: u64,

    /// Per-tier I/O counters in configured tier order.
    pub tiers: Vec<TierCounters>,
}

impl TierStats {
    /// Returns the number of DRAM misses.
    ///
    /// In this accounting model, a miss is recorded when it enters the fault
    /// path, so this is an alias for [`Self::faults`].
    #[must_use]
    pub const fn misses(&self) -> u64 {
        self.faults
    }

    /// Emits this snapshot through the `metrics` facade.
    ///
    /// Calling this method is optional. Constructing or snapshotting counters
    /// has no global-recorder side effect, which keeps library tests and
    /// embedders deterministic.
    pub fn emit_to_metrics(&self) {
        metrics::counter!("tierbuf_dram_hits_total").absolute(self.dram_hits);
        metrics::counter!("tierbuf_faults_total").absolute(self.faults);
        metrics::counter!("tierbuf_evictions_total").absolute(self.evictions);
        metrics::counter!("tierbuf_second_chances_total").absolute(self.second_chances);
        metrics::counter!("tierbuf_prefetch_submitted_total").absolute(self.prefetch_submitted);
        metrics::counter!("tierbuf_prefetch_hits_total").absolute(self.prefetch_hits);
        metrics::counter!("tierbuf_prefetch_skipped_total").absolute(self.prefetch_skipped);
        metrics::counter!("tierbuf_budget_denied_total").absolute(self.budget_denied);

        for tier in &self.tiers {
            metrics::counter!(
                "tierbuf_tier_demand_hits_total",
                "tier" => tier.name.clone()
            )
            .absolute(tier.demand_hits);
            metrics::counter!(
                "tierbuf_tier_reads_total",
                "tier" => tier.name.clone()
            )
            .absolute(tier.reads);
            metrics::counter!(
                "tierbuf_tier_writes_total",
                "tier" => tier.name.clone()
            )
            .absolute(tier.writes);
            metrics::counter!(
                "tierbuf_tier_bytes_read_total",
                "tier" => tier.name.clone()
            )
            .absolute(tier.bytes_read);
            metrics::counter!(
                "tierbuf_tier_bytes_written_total",
                "tier" => tier.name.clone()
            )
            .absolute(tier.bytes_written);
        }
    }
}

#[derive(Debug, Default)]
struct AtomicTierCounters {
    name: String,
    demand_hits: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

/// Live statistics recorder shared by buffer-manager components.
///
/// A standard read/write gate makes each multi-counter event and each snapshot
/// coherent while allowing concurrent recorders to proceed together. The
/// counters themselves remain atomic and saturate instead of wrapping. This
/// deliberately favors exact, deterministic snapshots over a lossy TLS
/// aggregation scheme in v0.1.
#[allow(dead_code)] // Consumed by pool, cooling, and prefetch integration phases.
#[derive(Debug)]
pub(crate) struct StatsRecorder {
    snapshot_gate: RwLock<()>,
    dram_hits: AtomicU64,
    faults: AtomicU64,
    evictions: AtomicU64,
    second_chances: AtomicU64,
    prefetch_submitted: AtomicU64,
    prefetch_hits: AtomicU64,
    prefetch_skipped: AtomicU64,
    budget_denied: AtomicU64,
    tiers: Box<[AtomicTierCounters]>,
}

#[allow(dead_code)] // Consumed by pool, cooling, and prefetch integration phases.
impl StatsRecorder {
    /// Creates a zeroed recorder for lower tiers in the supplied order.
    pub(crate) fn new<I, S>(tier_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let tiers = tier_names
            .into_iter()
            .map(|name| AtomicTierCounters {
                name: name.into(),
                ..AtomicTierCounters::default()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            snapshot_gate: RwLock::new(()),
            dram_hits: AtomicU64::new(0),
            faults: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            second_chances: AtomicU64::new(0),
            prefetch_submitted: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            prefetch_skipped: AtomicU64::new(0),
            budget_denied: AtomicU64::new(0),
            tiers,
        }
    }

    /// Records one resident DRAM hit.
    pub(crate) fn record_dram_hit(&self) {
        let _update = self.begin_update();
        saturating_add(&self.dram_hits, 1);
    }

    /// Records one DRAM miss entering fault processing.
    pub(crate) fn record_fault(&self) {
        let _update = self.begin_update();
        saturating_add(&self.faults, 1);
    }

    /// Records one completed eviction.
    pub(crate) fn record_eviction(&self) {
        let _update = self.begin_update();
        saturating_add(&self.evictions, 1);
    }

    /// Records one cooling-page second chance.
    pub(crate) fn record_second_chance(&self) {
        let _update = self.begin_update();
        saturating_add(&self.second_chances, 1);
    }

    /// Adds accepted prefetch submissions.
    pub(crate) fn record_prefetch_submitted(&self, count: u64) {
        let _update = self.begin_update();
        saturating_add(&self.prefetch_submitted, count);
    }

    /// Records one demand hit on a prefetched page.
    pub(crate) fn record_prefetch_hit(&self) {
        let _update = self.begin_update();
        saturating_add(&self.prefetch_hits, 1);
    }

    /// Adds prefetch requests skipped before submission.
    pub(crate) fn record_prefetch_skipped(&self, count: u64) {
        let _update = self.begin_update();
        saturating_add(&self.prefetch_skipped, count);
    }

    /// Records one write-budget rejection.
    pub(crate) fn record_budget_denied(&self) {
        let _update = self.begin_update();
        saturating_add(&self.budget_denied, 1);
    }

    /// Records one completed demand fix restored from a lower tier.
    ///
    /// # Panics
    ///
    /// Panics when `tier_index` is outside configured tier order.
    pub(crate) fn record_tier_demand_hit(&self, tier_index: usize) {
        let _update = self.begin_update();
        let tier = self.tier(tier_index);
        saturating_add(&tier.demand_hits, 1);
    }

    /// Records one completed lower-tier read and its byte count.
    ///
    /// # Panics
    ///
    /// Panics when `tier_index` is outside configured tier order.
    pub(crate) fn record_tier_read(&self, tier_index: usize, bytes: u64) {
        let _update = self.begin_update();
        let tier = self.tier(tier_index);
        saturating_add(&tier.reads, 1);
        saturating_add(&tier.bytes_read, bytes);
    }

    /// Records one completed lower-tier write and its byte count.
    ///
    /// # Panics
    ///
    /// Panics when `tier_index` is outside configured tier order.
    pub(crate) fn record_tier_write(&self, tier_index: usize, bytes: u64) {
        let _update = self.begin_update();
        let tier = self.tier(tier_index);
        saturating_add(&tier.writes, 1);
        saturating_add(&tier.bytes_written, bytes);
    }

    /// Returns a coherent, exact snapshot of all completed record operations.
    pub(crate) fn snapshot(&self) -> TierStats {
        let _snapshot = self.begin_snapshot();
        TierStats {
            dram_hits: load(&self.dram_hits),
            faults: load(&self.faults),
            evictions: load(&self.evictions),
            second_chances: load(&self.second_chances),
            prefetch_submitted: load(&self.prefetch_submitted),
            prefetch_hits: load(&self.prefetch_hits),
            prefetch_skipped: load(&self.prefetch_skipped),
            budget_denied: load(&self.budget_denied),
            tiers: self
                .tiers
                .iter()
                .map(|tier| TierCounters {
                    name: tier.name.clone(),
                    demand_hits: load(&tier.demand_hits),
                    reads: load(&tier.reads),
                    writes: load(&tier.writes),
                    bytes_read: load(&tier.bytes_read),
                    bytes_written: load(&tier.bytes_written),
                })
                .collect(),
        }
    }

    fn tier(&self, tier_index: usize) -> &AtomicTierCounters {
        self.tiers
            .get(tier_index)
            .unwrap_or_else(|| panic!("tier index {tier_index} is out of bounds"))
    }

    fn begin_update(&self) -> RwLockReadGuard<'_, ()> {
        self.snapshot_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn begin_snapshot(&self) -> RwLockWriteGuard<'_, ()> {
        self.snapshot_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Cost and residency accumulated for one storage tier.
#[derive(Clone, Debug, PartialEq)]
pub struct TierCost {
    /// Tier name. DRAM is reported as `"dram"`.
    pub name: String,

    /// Integrated residency in GiB-seconds.
    pub gib_seconds: f64,

    /// Residency cost accumulated using the price supplied at each tick.
    pub cost_usd: f64,

    /// Residency-weighted mean price in dollars per GiB-month.
    ///
    /// This is zero when no residency has been sampled.
    pub average_price_gib_month: f64,
}

/// Snapshot of cumulative storage-residency cost.
///
/// # Example
///
/// ```
/// use tierbuf::metrics::{CostReport, TierCost};
///
/// let report = CostReport {
///     tiers: vec![TierCost {
///         name: "dram".to_owned(),
///         gib_seconds: 1.0,
///         cost_usd: 0.25,
///         average_price_gib_month: 4.5,
///     }],
///     actual_cost_usd: 0.25,
///     all_dram_cost_usd: 1.0,
/// };
/// assert!(report.to_string().contains("all-DRAM $1.000000000 (0.250x)"));
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct CostReport {
    /// DRAM and lower-tier cost breakdown.
    pub tiers: Vec<TierCost>,

    /// Sum of actual DRAM and lower-tier residency cost.
    pub actual_cost_usd: f64,

    /// Counterfactual cost if every sampled resident byte had been priced at
    /// the configured DRAM rate.
    ///
    /// This deliberately does not deduplicate replicas across tiers.
    pub all_dram_cost_usd: f64,
}

impl CostReport {
    /// Returns actual cost divided by the all-DRAM counterfactual.
    ///
    /// Returns `None` before any non-zero residency has been sampled.
    #[must_use]
    pub fn actual_to_all_dram_ratio(&self) -> Option<f64> {
        if self.all_dram_cost_usd > 0.0 {
            Some(self.actual_cost_usd / self.all_dram_cost_usd)
        } else {
            None
        }
    }

    /// Emits cumulative costs and residency through the `metrics` facade.
    ///
    /// This is opt-in and does not install or mutate a global recorder.
    pub fn emit_to_metrics(&self) {
        metrics::gauge!("tierbuf_cost_actual_usd").set(self.actual_cost_usd);
        metrics::gauge!("tierbuf_cost_all_dram_usd").set(self.all_dram_cost_usd);
        for tier in &self.tiers {
            metrics::gauge!(
                "tierbuf_residency_gib_seconds",
                "tier" => tier.name.clone()
            )
            .set(tier.gib_seconds);
            metrics::gauge!(
                "tierbuf_residency_cost_usd",
                "tier" => tier.name.clone()
            )
            .set(tier.cost_usd);
        }
    }
}

impl fmt::Display for CostReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tierbuf cost: actual ${:.9}, all-DRAM ${:.9}",
            self.actual_cost_usd, self.all_dram_cost_usd
        )?;
        if let Some(ratio) = self.actual_to_all_dram_ratio() {
            write!(formatter, " ({ratio:.3}x)")?;
        }
        for tier in &self.tiers {
            write!(
                formatter,
                "\n  {}: {:.6} GiB·s, ${:.9}",
                tier.name, tier.gib_seconds, tier.cost_usd
            )?;
        }
        Ok(())
    }
}

/// One lower-tier sample supplied to [`ResidencyCost::tick`].
#[allow(dead_code)] // Consumed by the BufferManager epoch integration phase.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TierResidencySample<'a> {
    /// Stable tier name.
    pub(crate) name: &'a str,

    /// Bytes resident at the sampling instant.
    pub(crate) used_bytes: u64,

    /// Price in dollars per GiB-month for this interval.
    pub(crate) price_gib_month: f64,
}

#[allow(dead_code)] // Consumed by the BufferManager epoch integration phase.
impl<'a> TierResidencySample<'a> {
    /// Creates one lower-tier residency sample.
    pub(crate) const fn new(name: &'a str, used_bytes: u64, price_gib_month: f64) -> Self {
        Self {
            name,
            used_bytes,
            price_gib_month,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CostAccumulator {
    gib_seconds: f64,
    cost_usd: f64,
}

/// Mutable GiB-second accumulator sampled by the BufferManager epoch thread.
#[allow(dead_code)] // Consumed by the BufferManager epoch integration phase.
#[derive(Debug)]
pub(crate) struct ResidencyCost {
    dram_price_gib_month: f64,
    dram: CostAccumulator,
    lower_tiers: BTreeMap<String, CostAccumulator>,
    all_dram_cost_usd: f64,
}

#[allow(dead_code)] // Consumed by the BufferManager epoch integration phase.
impl ResidencyCost {
    /// Creates an empty accumulator with a fixed DRAM price.
    ///
    /// # Panics
    ///
    /// Panics if the price is negative, NaN, or infinite.
    pub(crate) fn new(dram_price_gib_month: f64) -> Self {
        assert_valid_price(dram_price_gib_month);
        Self {
            dram_price_gib_month,
            dram: CostAccumulator::default(),
            lower_tiers: BTreeMap::new(),
            all_dram_cost_usd: 0.0,
        }
    }

    /// Integrates one explicit residency interval.
    ///
    /// A 30-day month and 1 GiB = 2^30 bytes are used. Each lower-tier price
    /// is applied to this interval immediately, so later price changes do not
    /// retroactively reprice earlier residency.
    ///
    /// # Panics
    ///
    /// Panics if a lower-tier name is empty or a supplied price is negative,
    /// NaN, or infinite.
    pub(crate) fn tick(
        &mut self,
        elapsed: Duration,
        dram_used_bytes: u64,
        lower_tiers: &[TierResidencySample<'_>],
    ) {
        let elapsed_seconds = elapsed.as_secs_f64();
        if elapsed_seconds == 0.0 {
            return;
        }

        let dram_gib_seconds = gib_seconds(dram_used_bytes, elapsed_seconds);
        self.dram.gib_seconds += dram_gib_seconds;
        self.dram.cost_usd += monthly_cost(dram_gib_seconds, self.dram_price_gib_month);

        let mut all_dram_interval_gib_seconds = dram_gib_seconds;
        for sample in lower_tiers {
            assert!(!sample.name.is_empty(), "tier cost sample name is empty");
            assert_valid_price(sample.price_gib_month);

            let sample_gib_seconds = gib_seconds(sample.used_bytes, elapsed_seconds);
            let accumulator = self.lower_tiers.entry(sample.name.to_owned()).or_default();
            accumulator.gib_seconds += sample_gib_seconds;
            accumulator.cost_usd += monthly_cost(sample_gib_seconds, sample.price_gib_month);
            all_dram_interval_gib_seconds += sample_gib_seconds;
        }

        self.all_dram_cost_usd +=
            monthly_cost(all_dram_interval_gib_seconds, self.dram_price_gib_month);
    }

    /// Returns an immutable report of all intervals integrated so far.
    pub(crate) fn report(&self) -> CostReport {
        let mut tiers = Vec::with_capacity(self.lower_tiers.len() + 1);
        tiers.push(to_tier_cost(DRAM_TIER_NAME, self.dram));
        tiers.extend(
            self.lower_tiers
                .iter()
                .map(|(name, accumulator)| to_tier_cost(name, *accumulator)),
        );
        let actual_cost_usd = tiers.iter().map(|tier| tier.cost_usd).sum();

        CostReport {
            tiers,
            actual_cost_usd,
            all_dram_cost_usd: self.all_dram_cost_usd,
        }
    }
}

fn gib_seconds(bytes: u64, elapsed_seconds: f64) -> f64 {
    bytes as f64 / BYTES_PER_GIB * elapsed_seconds
}

fn monthly_cost(gib_seconds: f64, price_gib_month: f64) -> f64 {
    gib_seconds * price_gib_month / SECONDS_PER_MONTH
}

fn to_tier_cost(name: &str, accumulator: CostAccumulator) -> TierCost {
    let average_price_gib_month = if accumulator.gib_seconds == 0.0 {
        0.0
    } else {
        accumulator.cost_usd * SECONDS_PER_MONTH / accumulator.gib_seconds
    };
    TierCost {
        name: name.to_owned(),
        gib_seconds: accumulator.gib_seconds,
        cost_usd: accumulator.cost_usd,
        average_price_gib_month,
    }
}

fn assert_valid_price(price_gib_month: f64) {
    assert!(
        price_gib_month.is_finite() && !price_gib_month.is_sign_negative(),
        "tier price must be finite and greater than or equal to zero"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::{
        BYTES_PER_GIB, ResidencyCost, StatsRecorder, TierResidencySample, TierStats, monthly_cost,
    };

    fn assert_close(actual: f64, expected: f64) {
        let tolerance = expected.abs().max(1.0) * 1e-12;
        assert!(
            (actual - expected).abs() <= tolerance,
            "actual {actual} differs from expected {expected}"
        );
    }

    #[test]
    fn recorder_snapshot_contains_exact_deterministic_counts() {
        let recorder = StatsRecorder::new(["nvme", "cold"]);
        recorder.record_dram_hit();
        recorder.record_dram_hit();
        recorder.record_fault();
        recorder.record_eviction();
        recorder.record_second_chance();
        recorder.record_prefetch_submitted(3);
        recorder.record_prefetch_hit();
        recorder.record_prefetch_skipped(2);
        recorder.record_budget_denied();
        recorder.record_tier_demand_hit(0);
        recorder.record_tier_read(0, 64 * 1024);
        recorder.record_tier_read(0, 64 * 1024);
        recorder.record_tier_write(1, 64 * 1024);

        let expected = TierStats {
            dram_hits: 2,
            faults: 1,
            evictions: 1,
            second_chances: 1,
            prefetch_submitted: 3,
            prefetch_hits: 1,
            prefetch_skipped: 2,
            budget_denied: 1,
            tiers: vec![
                super::TierCounters {
                    name: "nvme".to_owned(),
                    demand_hits: 1,
                    reads: 2,
                    writes: 0,
                    bytes_read: 128 * 1024,
                    bytes_written: 0,
                },
                super::TierCounters {
                    name: "cold".to_owned(),
                    demand_hits: 0,
                    reads: 0,
                    writes: 1,
                    bytes_read: 0,
                    bytes_written: 64 * 1024,
                },
            ],
        };
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot, expected);
        assert_eq!(snapshot.misses(), 1);
        assert_eq!(snapshot.clone(), snapshot);
    }

    #[test]
    fn concurrent_updates_are_neither_lost_nor_torn() {
        const THREADS: u64 = 8;
        const ITERATIONS: u64 = 10_000;
        const BYTES: u64 = 4096;

        let recorder = Arc::new(StatsRecorder::new(["nvme"]));
        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let recorder = Arc::clone(&recorder);
                thread::spawn(move || {
                    for _ in 0..ITERATIONS {
                        recorder.record_dram_hit();
                        recorder.record_fault();
                        recorder.record_tier_demand_hit(0);
                        recorder.record_tier_read(0, BYTES);
                        recorder.record_tier_write(0, BYTES);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("counter worker should finish");
        }

        let expected_events = THREADS * ITERATIONS;
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.dram_hits, expected_events);
        assert_eq!(snapshot.faults, expected_events);
        assert_eq!(snapshot.tiers[0].demand_hits, expected_events);
        assert_eq!(snapshot.tiers[0].reads, expected_events);
        assert_eq!(snapshot.tiers[0].writes, expected_events);
        assert_eq!(snapshot.tiers[0].bytes_read, expected_events * BYTES);
        assert_eq!(snapshot.tiers[0].bytes_written, expected_events * BYTES);
    }

    #[test]
    fn two_tier_cost_is_lower_than_all_dram_at_chosen_prices() {
        let mut cost = ResidencyCost::new(6.0);
        cost.tick(
            Duration::from_secs(30 * 24 * 60 * 60),
            BYTES_PER_GIB as u64,
            &[
                TierResidencySample::new("nvme", 2 * BYTES_PER_GIB as u64, 0.6),
                TierResidencySample::new("cold", 4 * BYTES_PER_GIB as u64, 0.06),
            ],
        );

        let report = cost.report();
        assert_close(report.actual_cost_usd, 6.0 + 1.2 + 0.24);
        assert_close(report.all_dram_cost_usd, 7.0 * 6.0);
        assert!(report.actual_cost_usd < report.all_dram_cost_usd);
        assert_eq!(report.tiers[0].name, "dram");
        assert_eq!(report.tiers[1].name, "cold");
        assert_eq!(report.tiers[2].name, "nvme");
    }

    #[test]
    fn tick_integrates_elapsed_time_and_price_changes() {
        let mut cost = ResidencyCost::new(4.0);
        cost.tick(
            Duration::from_secs(10),
            0,
            &[TierResidencySample::new("nvme", BYTES_PER_GIB as u64, 1.0)],
        );
        cost.tick(
            Duration::from_secs(20),
            0,
            &[TierResidencySample::new("nvme", BYTES_PER_GIB as u64, 2.0)],
        );

        let report = cost.report();
        let nvme = report
            .tiers
            .iter()
            .find(|tier| tier.name == "nvme")
            .expect("nvme breakdown");
        assert_close(nvme.gib_seconds, 30.0);
        assert_close(
            nvme.cost_usd,
            monthly_cost(10.0, 1.0) + monthly_cost(20.0, 2.0),
        );
        assert_close(nvme.average_price_gib_month, 5.0 / 3.0);
    }

    #[test]
    fn display_includes_totals_ratio_and_tier_breakdown() {
        let mut cost = ResidencyCost::new(5.0);
        cost.tick(
            Duration::from_secs(60),
            BYTES_PER_GIB as u64,
            &[TierResidencySample::new("nvme", BYTES_PER_GIB as u64, 0.5)],
        );

        let rendered = cost.report().to_string();
        assert!(rendered.contains("tierbuf cost: actual $"));
        assert!(rendered.contains("all-DRAM $"));
        assert!(rendered.contains("x)"));
        assert!(rendered.contains("\n  dram: 60.000000 GiB·s"));
        assert!(rendered.contains("\n  nvme: 60.000000 GiB·s"));
    }
}
