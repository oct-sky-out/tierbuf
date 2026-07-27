//! Placement-policy interfaces and built-in economic policy.

use crate::frame::Frame;
use crate::swip::PageId;

pub mod economic;
pub mod heat;

pub use economic::{EconomicConfig, EconomicPolicy};
pub use heat::{HEAT_FRACTION_BITS, HEAT_ONE, HeatTracker};

/// The kind of access observed by a placement policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessKind {
    /// A page was read.
    Read,
    /// A page was modified.
    Write,
}

/// A caller-provided admission hint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AccessHint {
    /// Ordinary point or range access.
    #[default]
    Normal,
    /// Sequential scan traffic that should not displace reusable pages.
    Scan,
    /// Speculative access submitted ahead of demand.
    Prefetch,
}

/// Policy-relevant information about one lower storage tier.
#[derive(Clone, Debug, PartialEq)]
pub struct TierInfo {
    /// Stable tier index used by the buffer manager.
    pub index: usize,
    /// Human-readable tier name.
    pub name: String,
    /// Storage price in dollars per GiB-month.
    pub price_gb_month: f64,
    /// Representative page-read latency in microseconds.
    pub read_latency_us: f64,
    /// Remaining write budget in bytes, or `None` for an unlimited tier.
    pub write_budget_remaining_bytes: Option<u64>,
}

impl TierInfo {
    /// Creates policy information for one tier.
    #[must_use]
    pub fn new(
        index: usize,
        name: impl Into<String>,
        price_gb_month: f64,
        read_latency_us: f64,
        write_budget_remaining_bytes: Option<u64>,
    ) -> Self {
        Self {
            index,
            name: name.into(),
            price_gb_month,
            read_latency_us,
            write_budget_remaining_bytes,
        }
    }
}

/// Runtime-pluggable page placement policy.
///
/// The interface has no generic methods or associated types, so callers may
/// store it as `Box<dyn PlacementPolicy>`.
pub trait PlacementPolicy: Send + Sync + 'static {
    /// Records a resident-frame access.
    fn on_access(&self, frame: &Frame, kind: AccessKind);

    /// Advances policy time by one epoch.
    fn on_epoch(&self);

    /// Chooses a lower-tier index for a demoted frame.
    ///
    /// Returns `None` when every tier is invalid or lacks one page of write
    /// budget.
    fn demotion_target(&self, frame: &Frame, tiers: &[TierInfo]) -> Option<usize>;

    /// Returns whether a faulted page should be admitted to DRAM.
    fn admit_to_dram(&self, pid: PageId, hint: AccessHint) -> bool;
}
