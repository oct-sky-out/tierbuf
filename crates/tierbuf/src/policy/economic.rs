//! Five-minute-rule-inspired economic placement.
//!
//! Read request charges participate directly in the break-even interval.
//! Write request charges are carried through [`TierInfo`] and validated, but
//! are not yet included in placement because amortizing a one-time write
//! charge against an uncertain number of future reads needs a separate model.

use crate::frame::Frame;
use crate::policy::heat::{HEAT_ONE, HeatTracker};
use crate::policy::{AccessHint, AccessKind, PlacementPolicy, TierInfo};
use crate::swip::PageId;
use crate::{PAGE_SIZE, Result, TierBufError};

const GIB_BYTES: f64 = 1024.0 * 1024.0 * 1024.0;
const MONTH_SECONDS: f64 = 30.0 * 24.0 * 60.0 * 60.0;

/// Economic-policy configuration.
///
/// `cpu_cost_usd_per_us` converts read latency into an opportunity cost.
/// `read_opportunity_cost_usd` is a fixed per-read cost for submission,
/// queueing, and IOPS pressure not represented by latency. Both are explicit
/// modeling knobs rather than claims about a particular host.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EconomicConfig {
    /// DRAM price in dollars per GiB-month.
    pub dram_price_gb_month: f64,
    /// Duration of one heat epoch in seconds.
    pub epoch_seconds: f64,
    /// CPU opportunity cost in dollars per microsecond of read latency.
    pub cpu_cost_usd_per_us: f64,
    /// Fixed opportunity cost in dollars per lower-tier read.
    pub read_opportunity_cost_usd: f64,
}

impl Default for EconomicConfig {
    fn default() -> Self {
        Self {
            dram_price_gb_month: 4.5,
            epoch_seconds: 1.0,
            cpu_cost_usd_per_us: 1.0e-9,
            read_opportunity_cost_usd: 1.0e-7,
        }
    }
}

/// Heat-aware placement policy using DRAM and re-read opportunity costs.
#[derive(Debug)]
pub struct EconomicPolicy {
    config: EconomicConfig,
    heat: HeatTracker,
}

impl EconomicPolicy {
    /// Creates an economic policy after validating every numeric input.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::InvalidConfig`] when the epoch or DRAM price is
    /// zero, any value is NaN or infinite, or either opportunity cost is
    /// negative.
    pub fn new(config: EconomicConfig) -> Result<Self> {
        validate_config(config)?;
        Ok(Self {
            config,
            heat: HeatTracker::new(),
        })
    }

    /// Returns this policy's immutable economic configuration.
    #[must_use]
    pub const fn config(&self) -> EconomicConfig {
        self.config
    }

    /// Returns the policy's global heat tracker.
    #[must_use]
    pub const fn heat_tracker(&self) -> &HeatTracker {
        &self.heat
    }

    /// Returns the cost of keeping one page in DRAM for one second.
    #[must_use]
    pub fn dram_page_cost_per_second(&self) -> f64 {
        self.config.dram_price_gb_month * PAGE_SIZE as f64 / GIB_BYTES / MONTH_SECONDS
    }

    /// Returns the modeled opportunity cost of one read from `tier`.
    ///
    /// Invalid or overflowing latency data returns `None`.
    #[must_use]
    pub fn read_opportunity_cost(&self, tier: &TierInfo) -> Option<f64> {
        if !tier.read_latency_us.is_finite() || tier.read_latency_us < 0.0 {
            return None;
        }
        let cost = tier.read_latency_us * self.config.cpu_cost_usd_per_us
            + self.config.read_opportunity_cost_usd
            + tier.read_request_cost_usd;
        cost.is_finite().then_some(cost)
    }

    /// Returns the DRAM-versus-tier break-even reaccess interval in seconds.
    ///
    /// Reaccess sooner than this interval favors retaining the page in DRAM;
    /// reaccess later than it permits demotion to this tier.
    #[must_use]
    pub fn break_even_seconds(&self, tier: &TierInfo) -> Option<f64> {
        self.read_opportunity_cost(tier)
            .map(|read_cost| read_cost / self.dram_page_cost_per_second())
    }

    fn estimated_reaccess_seconds(&self, frame: &Frame) -> f64 {
        let raw_heat = self.heat.current_heat_raw(frame);
        if raw_heat == 0 {
            f64::INFINITY
        } else {
            self.config.epoch_seconds * f64::from(HEAT_ONE) / f64::from(raw_heat)
        }
    }

    fn is_eligible(&self, tier: &TierInfo) -> bool {
        tier.price_gb_month.is_finite()
            && tier.price_gb_month >= 0.0
            && tier.read_request_cost_usd.is_finite()
            && tier.read_request_cost_usd >= 0.0
            && tier.write_request_cost_usd.is_finite()
            && tier.write_request_cost_usd >= 0.0
            && self.read_opportunity_cost(tier).is_some()
            && tier
                .write_budget_remaining_bytes
                .is_none_or(|remaining| remaining >= PAGE_SIZE as u64)
    }
}

impl Default for EconomicPolicy {
    fn default() -> Self {
        Self::new(EconomicConfig::default()).expect("default economic config must be valid")
    }
}

impl PlacementPolicy for EconomicPolicy {
    fn on_access(&self, frame: &Frame, kind: AccessKind) {
        match kind {
            AccessKind::Read | AccessKind::Write => {
                self.heat.on_access(frame);
            }
        }
    }

    fn on_epoch(&self) {
        self.heat.on_epoch();
    }

    fn demotion_target(&self, frame: &Frame, tiers: &[TierInfo]) -> Option<usize> {
        let fastest = tiers
            .iter()
            .filter(|tier| self.is_eligible(tier))
            .min_by(|left, right| left.read_latency_us.total_cmp(&right.read_latency_us))?;

        let reaccess_seconds = self.estimated_reaccess_seconds(frame);
        tiers
            .iter()
            .filter(|tier| self.is_eligible(tier))
            .filter(|tier| {
                self.break_even_seconds(tier)
                    .is_some_and(|break_even| reaccess_seconds >= break_even)
            })
            .min_by(|left, right| left.price_gb_month.total_cmp(&right.price_gb_month))
            .map_or(Some(fastest.index), |tier| Some(tier.index))
    }

    fn admit_to_dram(&self, _pid: PageId, hint: AccessHint) -> bool {
        !matches!(hint, AccessHint::Scan)
    }
}

fn validate_config(config: EconomicConfig) -> Result<()> {
    if !config.dram_price_gb_month.is_finite() || config.dram_price_gb_month <= 0.0 {
        return Err(invalid_config(
            "DRAM price must be finite and greater than zero",
        ));
    }
    if !config.epoch_seconds.is_finite() || config.epoch_seconds <= 0.0 {
        return Err(invalid_config(
            "epoch seconds must be finite and greater than zero",
        ));
    }
    if !config.cpu_cost_usd_per_us.is_finite() || config.cpu_cost_usd_per_us < 0.0 {
        return Err(invalid_config(
            "CPU opportunity cost must be finite and non-negative",
        ));
    }
    if !config.read_opportunity_cost_usd.is_finite() || config.read_opportunity_cost_usd < 0.0 {
        return Err(invalid_config(
            "read opportunity cost must be finite and non-negative",
        ));
    }
    Ok(())
}

fn invalid_config(message: &'static str) -> TierBufError {
    TierBufError::InvalidConfig(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{EconomicConfig, EconomicPolicy};
    use crate::PAGE_SIZE;
    use crate::frame::Frame;
    use crate::policy::heat::HEAT_ONE;
    use crate::policy::{AccessHint, AccessKind, PlacementPolicy, TierInfo};
    use crate::swip::PageId;

    const FAST_INDEX: usize = 7;
    const CHEAP_INDEX: usize = 11;

    fn config(dram_price_gb_month: f64) -> EconomicConfig {
        EconomicConfig {
            dram_price_gb_month,
            epoch_seconds: 1.0,
            cpu_cost_usd_per_us: 1.0e-9,
            read_opportunity_cost_usd: 1.0e-7,
        }
    }

    fn tiers(fast_budget: Option<u64>, cheap_budget: Option<u64>) -> [TierInfo; 2] {
        [
            TierInfo::new(FAST_INDEX, "fast", 0.10, 80.0, 0.0, 0.0, fast_budget),
            TierInfo::new(CHEAP_INDEX, "cheap", 0.01, 2_000.0, 0.0, 0.0, cheap_budget),
        ]
    }

    fn raw_for_interval(seconds: Option<f64>) -> u32 {
        seconds.map_or(0, |seconds| {
            (f64::from(HEAT_ONE) / seconds)
                .round()
                .clamp(1.0, f64::from(u32::MAX)) as u32
        })
    }

    #[test]
    fn formulas_match_documented_units() {
        let policy = EconomicPolicy::new(config(4.5)).expect("valid config");
        let tier = TierInfo::new(0, "nvme", 0.1, 80.0, 0.0, 0.0, None);

        let expected_dram = 4.5 * PAGE_SIZE as f64 / (1024.0 * 1024.0 * 1024.0) / (30.0 * 86_400.0);
        assert!((policy.dram_page_cost_per_second() - expected_dram).abs() < 1.0e-24);
        assert_eq!(policy.read_opportunity_cost(&tier), Some(1.8e-7));
        let expected_break_even = 1.8e-7 / expected_dram;
        assert!(
            (policy.break_even_seconds(&tier).expect("valid tier") - expected_break_even).abs()
                < 1.0e-9
        );
    }

    #[test]
    fn request_cost_lengthens_break_even() {
        let policy = EconomicPolicy::new(config(4.5)).expect("valid config");
        let without_request_cost = TierInfo::new(0, "free-requests", 0.02, 1_000.0, 0.0, 0.0, None);
        let with_request_cost =
            TierInfo::new(1, "metered-requests", 0.02, 1_000.0, 4.0e-7, 0.0, None);

        assert!(
            policy
                .break_even_seconds(&with_request_cost)
                .expect("valid metered tier")
                > policy
                    .break_even_seconds(&without_request_cost)
                    .expect("valid unmetered tier")
        );
    }

    #[test]
    fn warm_pages_prefer_low_request_cost_tier() {
        let policy = EconomicPolicy::new(config(4.5)).expect("valid config");
        let frame = Frame::new();
        let tiers = [
            TierInfo::new(0, "nvme", 0.08, 80.0, 0.0, 0.0, None),
            TierInfo::new(1, "s3", 0.023, 30_000.0, 4.0e-7, 5.0e-6, None),
        ];

        frame.set_heat_and_epoch(raw_for_interval(Some(10_000.0)), 0);
        assert_eq!(policy.demotion_target(&frame, &tiers), Some(0));

        frame.set_heat_and_epoch(0, 0);
        assert_eq!(policy.demotion_target(&frame, &tiers), Some(1));
    }

    #[test]
    fn invalid_request_costs_disqualify_tier() {
        let policy = EconomicPolicy::new(config(4.5)).expect("valid config");
        let frame = Frame::new();
        frame.set_heat_and_epoch(0, 0);
        let tiers = [
            TierInfo::new(0, "invalid", 0.001, 10.0, f64::NAN, 0.0, None),
            TierInfo::new(1, "valid", 0.1, 80.0, 0.0, 0.0, None),
        ];

        assert_eq!(policy.demotion_target(&frame, &tiers), Some(1));
    }

    #[test]
    fn invalid_numeric_configuration_is_rejected() {
        let mut candidate = config(4.5);
        candidate.epoch_seconds = 0.0;
        assert!(EconomicPolicy::new(candidate).is_err());
        candidate.epoch_seconds = f64::NAN;
        assert!(EconomicPolicy::new(candidate).is_err());

        candidate = config(f64::NAN);
        assert!(EconomicPolicy::new(candidate).is_err());
        candidate = config(4.5);
        candidate.cpu_cost_usd_per_us = -1.0;
        assert!(EconomicPolicy::new(candidate).is_err());
        candidate = config(4.5);
        candidate.read_opportunity_cost_usd = f64::INFINITY;
        assert!(EconomicPolicy::new(candidate).is_err());
    }

    #[test]
    fn table_driven_demotion_covers_heat_and_budget_boundaries() {
        struct Case {
            name: &'static str,
            interval_seconds: Option<f64>,
            fast_budget: Option<u64>,
            cheap_budget: Option<u64>,
            expected: Option<usize>,
        }

        let page = PAGE_SIZE as u64;
        let cases = [
            Case {
                name: "very hot",
                interval_seconds: Some(0.01),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "one second",
                interval_seconds: Some(1.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "one hundred seconds",
                interval_seconds: Some(100.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "one thousand seconds",
                interval_seconds: Some(1_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "two thousand seconds",
                interval_seconds: Some(2_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "five thousand seconds",
                interval_seconds: Some(5_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "ten thousand seconds",
                interval_seconds: Some(10_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "fifty thousand seconds",
                interval_seconds: Some(50_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(CHEAP_INDEX),
            },
            Case {
                name: "one hundred thousand seconds",
                interval_seconds: Some(100_000.0),
                fast_budget: None,
                cheap_budget: None,
                expected: Some(CHEAP_INDEX),
            },
            Case {
                name: "zero heat",
                interval_seconds: None,
                fast_budget: None,
                cheap_budget: None,
                expected: Some(CHEAP_INDEX),
            },
            Case {
                name: "fast denied",
                interval_seconds: Some(1.0),
                fast_budget: Some(0),
                cheap_budget: None,
                expected: Some(CHEAP_INDEX),
            },
            Case {
                name: "cheap denied",
                interval_seconds: Some(50_000.0),
                fast_budget: None,
                cheap_budget: Some(page - 1),
                expected: Some(FAST_INDEX),
            },
            Case {
                name: "both denied",
                interval_seconds: Some(1.0),
                fast_budget: Some(page - 1),
                cheap_budget: Some(0),
                expected: None,
            },
            Case {
                name: "exact page budget",
                interval_seconds: Some(1.0),
                fast_budget: Some(page),
                cheap_budget: Some(0),
                expected: Some(FAST_INDEX),
            },
        ];
        assert!(cases.len() >= 12);

        let policy = EconomicPolicy::new(config(4.5)).expect("valid config");
        let frame = Frame::new();
        for case in cases {
            frame.set_heat_and_epoch(raw_for_interval(case.interval_seconds), 0);
            assert_eq!(
                policy.demotion_target(&frame, &tiers(case.fast_budget, case.cheap_budget)),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn tenfold_dram_price_moves_borderline_page_to_cheaper_tier() {
        let baseline = EconomicPolicy::new(config(4.5)).expect("valid config");
        let expensive = EconomicPolicy::new(config(45.0)).expect("valid config");
        let frame = Frame::new();
        frame.set_heat_and_epoch(raw_for_interval(Some(5_000.0)), 0);
        let candidates = tiers(None, None);

        assert_eq!(
            baseline.demotion_target(&frame, &candidates),
            Some(FAST_INDEX)
        );
        assert_eq!(
            expensive.demotion_target(&frame, &candidates),
            Some(CHEAP_INDEX)
        );
    }

    #[test]
    fn access_updates_heat_and_epoch_only_advances_global_clock() {
        let policy = EconomicPolicy::default();
        let frame = Frame::new();

        policy.on_access(&frame, AccessKind::Read);
        assert_eq!(frame.heat_and_epoch(), (HEAT_ONE, 0));
        policy.on_epoch();
        assert_eq!(frame.heat_and_epoch(), (HEAT_ONE, 0));
        assert_eq!(policy.heat_tracker().current_epoch(), 1);
        policy.on_access(&frame, AccessKind::Write);
        assert_eq!(frame.heat_and_epoch(), (HEAT_ONE + HEAT_ONE / 2, 1));
    }

    #[test]
    fn scan_admission_is_bypassed_but_normal_and_prefetch_are_admitted() {
        let policy = EconomicPolicy::default();
        let pid = PageId::new(9).expect("valid pid");

        assert!(policy.admit_to_dram(pid, AccessHint::Normal));
        assert!(policy.admit_to_dram(pid, AccessHint::Prefetch));
        assert!(!policy.admit_to_dram(pid, AccessHint::Scan));
    }

    #[test]
    fn placement_policy_is_object_safe() {
        fn accept_object(_policy: &dyn PlacementPolicy) {}

        let policy = EconomicPolicy::default();
        accept_object(&policy);
    }
}
