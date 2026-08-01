//! Buffer-pool assembly, coalesced faults, and background tiering.
//!
//! Resident page handles take the tagged Swip fast path without consulting the
//! page directory. Directory lookup and lower-tier I/O occur only after an
//! evicted handle enters its per-page fault-generation coordinator.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::atomic::{try_update_u64, try_update_usize};
use crate::cooling::{CoolingQueue, CoolingTicket};
use crate::frame::{Frame, FrameTable};
use crate::latch::{ExclusiveRaw, SharedRaw};
use crate::metrics::{CostReport, ResidencyCost, StatsRecorder, TierResidencySample, TierStats};
use crate::policy::{
    AccessHint, AccessKind, EconomicConfig, EconomicPolicy, PlacementPolicy, TierInfo,
};
use crate::swip::{PageId, ResidentAddr, Swip, SwipState};
use crate::tier::{TierBackend, TierOffset};
#[cfg(all(target_os = "linux", feature = "uring"))]
use crate::uring::UringReader;
use crate::{PAGE_SIZE, Result, TierBufError};

static NEXT_MANAGER_ID: AtomicU64 = AtomicU64::new(1);
const MAX_COOLING_SAMPLE: usize = 64;
const COOLER_INTERVAL: Duration = Duration::from_millis(2);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const PREFETCH_WORKERS: usize = 4;
const MAX_PREFETCH_IN_FLIGHT: usize = 128;
const PREFETCH_RECEIVE_TIMEOUT: Duration = Duration::from_millis(5);
#[cfg(all(target_os = "linux", feature = "uring"))]
const URING_ENTRIES: u32 = 128;

/// Economic inputs shared by placement-policy implementations.
#[derive(Clone, Copy, Debug)]
pub struct Economics {
    /// DRAM price in dollars per GiB-month.
    pub dram_price_gb_month: f64,
    /// Heat-decay and cost-integration epoch.
    pub epoch: Duration,
}

impl Default for Economics {
    fn default() -> Self {
        Self {
            dram_price_gb_month: 4.5,
            epoch: Duration::from_secs(1),
        }
    }
}

/// Configuration used to construct a [`BufferManager`].
///
/// The default leaves [`Self::tiers`] empty, so it cannot be used directly to
/// construct a manager. It is intended for struct-update syntax after callers
/// provide at least one lower storage tier.
pub struct BufConfig {
    /// Bytes reserved for fixed-size DRAM page frames.
    pub dram_pool_bytes: usize,
    /// Fraction of DRAM frames eventually reserved for the cooling stage.
    pub cooling_ratio: f64,
    /// Condition used by the background cooler.
    pub eviction_mode: EvictionMode,
    /// Economic policy inputs.
    pub economics: Economics,
    /// Lower storage tiers, ordered from fastest to the authoritative tier.
    pub tiers: Vec<Box<dyn TierBackend>>,
    /// Number of background prefetch worker threads. Defaults to 4.
    pub prefetch_workers: usize,
    /// Maximum queued-plus-running prefetch requests. Defaults to 128.
    pub max_prefetch_in_flight: usize,
}

impl Default for BufConfig {
    fn default() -> Self {
        Self {
            dram_pool_bytes: 64 * PAGE_SIZE,
            cooling_ratio: 0.1,
            eviction_mode: EvictionMode::Demand,
            economics: Economics::default(),
            tiers: Vec::new(),
            prefetch_workers: PREFETCH_WORKERS,
            max_prefetch_in_flight: MAX_PREFETCH_IN_FLIGHT,
        }
    }
}

/// Background-eviction activation policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvictionMode {
    /// Evict below the low watermark only after a demand fault-in in the
    /// current economic epoch.
    #[default]
    Demand,
    /// Preserve the original behavior of maintaining the low watermark
    /// regardless of recent demand.
    Watermark,
}

/// Storage source that satisfied one completed fix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixSource {
    /// The page was already resident in DRAM.
    Dram,
    /// The page was restored from a configured lower tier.
    LowerTier,
}

/// A page's backing location below DRAM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Location {
    /// Index into [`BufConfig::tiers`].
    pub tier_index: usize,
    /// Byte offset within that tier.
    pub offset: TierOffset,
}

/// Concurrent logical-page to backing-location directory.
///
/// Entries remain present while pages are resident. This lets a clean page be
/// evicted again without rewriting or losing its backing location. Lookups are
/// counted so tests can enforce that resident fixes use only the tagged path.
#[derive(Debug, Default)]
pub struct PageDirectory {
    entries: RwLock<HashMap<PageId, Location>>,
    lookups: AtomicU64,
}

impl PageDirectory {
    fn lookup(&self, pid: PageId) -> Option<Location> {
        self.lookups.fetch_add(1, Ordering::Relaxed);
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pid)
            .copied()
    }

    fn insert(&self, pid: PageId, location: Location) -> Option<Location> {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pid, location)
    }

    fn backing(&self, pid: PageId) -> Option<Location> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pid)
            .copied()
    }

    fn restore(&self, pid: PageId, location: Option<Location>) {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(location) = location {
            entries.insert(pid, location);
        } else {
            entries.remove(&pid);
        }
    }

    #[cfg(test)]
    fn contains_without_lookup(&self, pid: PageId) -> bool {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&pid)
    }

    #[cfg(test)]
    fn lookup_count(&self) -> u64 {
        self.lookups.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct PageControl {
    swip: Swip,
    fault: FaultCoordinator,
}

#[derive(Debug, Default)]
struct FaultCoordinator {
    state: Mutex<FaultState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct FaultState {
    next_generation: u64,
    active: Option<ActiveFault>,
    completed: Option<CompletedFault>,
}

#[derive(Debug)]
struct ActiveFault {
    generation: u64,
    waiters: usize,
}

#[derive(Debug)]
struct CompletedFault {
    generation: u64,
    outcome: FaultOutcome,
    remaining_waiters: usize,
}

#[derive(Clone, Debug)]
enum FaultOutcome {
    Success(Option<usize>),
    Failure(ReplayableFaultError),
}

#[derive(Clone, Debug)]
enum ReplayableFaultError {
    Io {
        kind: io::ErrorKind,
        message: String,
    },
    PoolExhausted,
    TierExhausted {
        tier: String,
    },
    InvalidPid(u64),
    DuplicateHandle(u64),
    InvalidConfig(String),
    Contended,
    Retry,
    ShuttingDown,
}

enum FaultTurn<'a> {
    Leader(FaultLeader<'a>),
    Follower(Result<Option<usize>>),
}

struct FaultLeader<'a> {
    coordinator: &'a FaultCoordinator,
    generation: u64,
    active: bool,
}

impl FaultCoordinator {
    fn enter(&self) -> FaultTurn<'_> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        loop {
            if let Some(active) = state.active.as_mut() {
                let generation = active.generation;
                active.waiters = active
                    .waiters
                    .checked_add(1)
                    .expect("fault waiter count cannot overflow");

                loop {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(completed) = state
                        .completed
                        .as_mut()
                        .filter(|completed| completed.generation == generation)
                    else {
                        continue;
                    };

                    let outcome = completed.outcome.clone();
                    completed.remaining_waiters = completed
                        .remaining_waiters
                        .checked_sub(1)
                        .expect("completed fault must retain every registered waiter");
                    if completed.remaining_waiters == 0 {
                        state.completed = None;
                        self.changed.notify_all();
                    }
                    return FaultTurn::Follower(outcome.into_result());
                }
            }

            // A caller arriving after completion belongs to a later,
            // independent attempt. Keep the completed generation alive until
            // every registered waiter has replayed its exact outcome.
            if state.completed.is_some() {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }

            let generation = state.next_generation;
            state.next_generation = state.next_generation.wrapping_add(1);
            state.active = Some(ActiveFault {
                generation,
                waiters: 0,
            });
            return FaultTurn::Leader(FaultLeader {
                coordinator: self,
                generation,
                active: true,
            });
        }
    }

    fn complete(&self, generation: u64, outcome: FaultOutcome) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(active) = state.active.take() else {
            return;
        };
        debug_assert_eq!(active.generation, generation);
        if active.waiters != 0 {
            state.completed = Some(CompletedFault {
                generation,
                outcome,
                remaining_waiters: active.waiters,
            });
        }
        self.changed.notify_all();
    }
}

impl FaultOutcome {
    fn from_result(result: &Result<Option<usize>>) -> Self {
        match result {
            Ok(tier_index) => Self::Success(*tier_index),
            Err(error) => Self::Failure(ReplayableFaultError::from(error)),
        }
    }

    fn into_result(self) -> Result<Option<usize>> {
        match self {
            Self::Success(tier_index) => Ok(tier_index),
            Self::Failure(error) => Err(error.into()),
        }
    }
}

impl From<&TierBufError> for ReplayableFaultError {
    fn from(error: &TierBufError) -> Self {
        match error {
            TierBufError::Io(source) => Self::Io {
                kind: source.kind(),
                message: source.to_string(),
            },
            TierBufError::PoolExhausted => Self::PoolExhausted,
            TierBufError::TierExhausted { tier } => Self::TierExhausted { tier: tier.clone() },
            TierBufError::InvalidPid(pid) => Self::InvalidPid(*pid),
            TierBufError::DuplicateHandle(pid) => Self::DuplicateHandle(*pid),
            TierBufError::InvalidConfig(message) => Self::InvalidConfig(message.clone()),
            TierBufError::Contended => Self::Contended,
            TierBufError::Retry => Self::Retry,
            TierBufError::ShuttingDown => Self::ShuttingDown,
        }
    }
}

impl From<ReplayableFaultError> for TierBufError {
    fn from(error: ReplayableFaultError) -> Self {
        match error {
            ReplayableFaultError::Io { kind, message } => Self::Io(io::Error::new(kind, message)),
            ReplayableFaultError::PoolExhausted => Self::PoolExhausted,
            ReplayableFaultError::TierExhausted { tier } => Self::TierExhausted { tier },
            ReplayableFaultError::InvalidPid(pid) => Self::InvalidPid(pid),
            ReplayableFaultError::DuplicateHandle(pid) => Self::DuplicateHandle(pid),
            ReplayableFaultError::InvalidConfig(message) => Self::InvalidConfig(message),
            ReplayableFaultError::Contended => Self::Contended,
            ReplayableFaultError::Retry => Self::Retry,
            ReplayableFaultError::ShuttingDown => Self::ShuttingDown,
        }
    }
}

impl FaultLeader<'_> {
    fn finish(mut self, result: &Result<Option<usize>>) {
        self.active = false;
        self.coordinator
            .complete(self.generation, FaultOutcome::from_result(result));
    }
}

impl Drop for FaultLeader<'_> {
    fn drop(&mut self) {
        if self.active {
            self.coordinator.complete(
                self.generation,
                FaultOutcome::Failure(ReplayableFaultError::Contended),
            );
        }
    }
}

#[derive(Debug, Default)]
struct WorkerSignal {
    generation: Mutex<u64>,
    wakeup: Condvar,
}

impl WorkerSignal {
    fn notify(&self) {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        self.wakeup.notify_all();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvictionOutcome {
    Freed,
    Stale,
    Busy,
}

struct PrefetchTask {
    pid: PageId,
    swip: Swip,
    checkout: Option<OwnedFreeFrame>,
    _reservation: PrefetchReservation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrefetchMarker {
    resident: ResidentAddr,
    generation: u32,
}

struct PrefetchReservation {
    pid: PageId,
    in_flight: Arc<AtomicUsize>,
    pending: Arc<Mutex<HashSet<PageId>>>,
}

impl PrefetchReservation {
    fn new(pid: PageId, in_flight: Arc<AtomicUsize>, pending: Arc<Mutex<HashSet<PageId>>>) -> Self {
        Self {
            pid,
            in_flight,
            pending,
        }
    }
}

impl Drop for PrefetchReservation {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.pid);
        let previous = self.in_flight.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "prefetch in-flight count cannot underflow");
    }
}

/// A fixed-page buffer manager with synchronous fixes and background tiering.
///
/// The manager owns canonical Swips for every allocated page. Clones of those
/// handles may move freely, but independently constructed handles are rejected
/// before their resident address can be used.
pub struct BufferManager {
    id: u64,
    frames: Arc<FrameTable>,
    tiers: Vec<Box<dyn TierBackend>>,
    policy: Arc<dyn PlacementPolicy>,
    stats: StatsRecorder,
    residency_cost: Mutex<ResidencyCost>,
    directory: PageDirectory,
    pages: RwLock<HashMap<PageId, Arc<PageControl>>>,
    next_pid: AtomicU64,
    cooling: CoolingQueue,
    cooling_target: usize,
    low_watermark: usize,
    eviction_mode: EvictionMode,
    fault_ins_this_epoch: AtomicU64,
    exhausted_frame_request: AtomicBool,
    sample_cursor: AtomicU64,
    stopping: Arc<AtomicBool>,
    epoch: Duration,
    worker_signal: Arc<WorkerSignal>,
    cooler_signal: Arc<WorkerSignal>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    prefetch_sender: SyncSender<PrefetchTask>,
    prefetch_receiver: Arc<Mutex<Receiver<PrefetchTask>>>,
    prefetch_workers: usize,
    max_prefetch_in_flight: usize,
    prefetch_in_flight: Arc<AtomicUsize>,
    prefetch_pending: Arc<Mutex<HashSet<PageId>>>,
    prefetched: Mutex<HashMap<PageId, PrefetchMarker>>,
    #[cfg(all(target_os = "linux", feature = "uring"))]
    uring_reader: Option<UringReader>,
    operation_gate: RwLock<()>,
    shutdown_lock: Mutex<()>,
}

impl BufferManager {
    /// Creates a buffer manager and its fixed DRAM frame table.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::InvalidConfig`] when page-pool sizing, cooling,
    /// economic, or tier invariants are invalid. Mapping and background-worker
    /// creation failures are returned as [`TierBufError::Io`].
    pub fn new(config: BufConfig) -> Result<Arc<Self>> {
        let defaults = EconomicConfig::default();
        let policy = EconomicPolicy::new(EconomicConfig {
            dram_price_gb_month: config.economics.dram_price_gb_month,
            epoch_seconds: config.economics.epoch.as_secs_f64(),
            cpu_cost_usd_per_us: defaults.cpu_cost_usd_per_us,
            read_opportunity_cost_usd: defaults.read_opportunity_cost_usd,
        })?;
        Self::new_with_policy(config, Arc::new(policy))
    }

    /// Creates a buffer manager using a caller-supplied placement policy.
    ///
    /// The policy is shared with the manager and may retain its own atomic
    /// state. Cost accounting continues to use [`BufConfig::economics`].
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::InvalidConfig`] when page-pool sizing, cooling,
    /// economic, or tier invariants are invalid. Mapping and background-worker
    /// creation failures are returned as [`TierBufError::Io`].
    pub fn new_with_policy(
        config: BufConfig,
        policy: Arc<dyn PlacementPolicy>,
    ) -> Result<Arc<Self>> {
        validate_config(&config)?;
        let id = allocate_manager_id()?;
        let frames = Arc::new(FrameTable::new(config.dram_pool_bytes)?);
        let frame_count = frames.frame_count();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let cooling_target = ((frame_count as f64) * config.cooling_ratio).ceil() as usize;
        let low_watermark = frame_count.div_ceil(10).max(1);
        let stats = StatsRecorder::new(config.tiers.iter().map(|tier| tier.name()));
        let residency_cost = Mutex::new(ResidencyCost::new(config.economics.dram_price_gb_month));
        let epoch = config.economics.epoch;
        let prefetch_workers = config.prefetch_workers;
        let max_prefetch_in_flight = config.max_prefetch_in_flight;
        let (prefetch_sender, prefetch_receiver) = sync_channel(max_prefetch_in_flight);

        let manager = Arc::new(Self {
            id,
            frames,
            tiers: config.tiers,
            policy,
            stats,
            residency_cost,
            directory: PageDirectory::default(),
            pages: RwLock::new(HashMap::new()),
            next_pid: AtomicU64::new(0),
            cooling: CoolingQueue::default(),
            cooling_target,
            low_watermark,
            eviction_mode: config.eviction_mode,
            fault_ins_this_epoch: AtomicU64::new(0),
            exhausted_frame_request: AtomicBool::new(false),
            sample_cursor: AtomicU64::new(0),
            stopping: Arc::new(AtomicBool::new(false)),
            epoch,
            worker_signal: Arc::new(WorkerSignal::default()),
            cooler_signal: Arc::new(WorkerSignal::default()),
            workers: Mutex::new(Vec::with_capacity(2 + prefetch_workers)),
            prefetch_sender,
            prefetch_receiver: Arc::new(Mutex::new(prefetch_receiver)),
            prefetch_workers,
            max_prefetch_in_flight,
            prefetch_in_flight: Arc::new(AtomicUsize::new(0)),
            prefetch_pending: Arc::new(Mutex::new(HashSet::new())),
            prefetched: Mutex::new(HashMap::new()),
            #[cfg(all(target_os = "linux", feature = "uring"))]
            uring_reader: UringReader::new(URING_ENTRIES).ok(),
            operation_gate: RwLock::new(()),
            shutdown_lock: Mutex::new(()),
        });
        Self::start_workers(&manager)?;
        Ok(manager)
    }

    /// Allocates a zeroed logical page and returns it exclusively pinned.
    ///
    /// The returned guard exposes the new [`PageId`] and a clone of its
    /// canonical [`Swip`]. A newly allocated page is dirty because it does not
    /// yet have a lower-tier backing location.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::PoolExhausted`] when no DRAM frame is free.
    pub fn allocate(&self) -> Result<ExclusiveGuard<'_>> {
        self.ensure_running()?;
        let _operation = self
            .operation_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_running()?;
        let mut checkout = self.acquire_frame()?;
        let index = checkout.index;
        let pid = self.allocate_pid()?;
        let swip = Swip::from_pid(pid);
        assert!(
            swip.try_bind_owner(self.id),
            "a fresh Swip must accept its manager identity"
        );

        self.frames.write_with(index, |page| page.fill(0));
        let frame = self.frames.frame(index);
        frame.install(pid, swip.clone());
        frame.set_dirty(true);
        checkout.publish_pin();
        let resident = self.frames.resident_addr(index);
        if swip.try_swizzle(pid, resident).is_err() {
            frame.unpin();
            self.clear_unpublished_frame(index);
            return Err(TierBufError::Contended);
        }

        let latch = frame.latch().lock_exclusive();
        assert!(
            self.resident_is_exact(&swip, pid, resident, frame),
            "newly installed frame must revalidate"
        );

        let control = Arc::new(PageControl {
            swip: swip.clone(),
            fault: FaultCoordinator::default(),
        });
        let previous = self
            .pages
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pid, control);
        assert!(previous.is_none(), "allocated PageId must be unique");

        checkout.commit();
        Ok(ExclusiveGuard {
            manager: self,
            index,
            pid,
            swip,
            latch,
        })
    }

    /// Fixes a page for shared access.
    ///
    /// Resident pages do not consult [`PageDirectory`]. An evicted page is
    /// synchronously faulted through its backing tier.
    ///
    /// # Errors
    ///
    /// Returns an invalid-page or duplicate-handle error for non-canonical
    /// handles, a storage error when faulting fails, or
    /// [`TierBufError::PoolExhausted`] when no fault frame is available.
    pub fn fix_shared<'a>(&'a self, swip: &Swip) -> Result<SharedGuard<'a>> {
        self.fix_shared_with(swip, AccessHint::Normal)
    }

    /// Fixes a page for shared access with an admission hint.
    ///
    /// When the policy declines DRAM admission after a fault, the synchronous
    /// core still uses a real frame to avoid an unsound transient buffer. It
    /// returns the pinned guard and marks its Swip `Cooling`; the frame becomes
    /// immediately evictable after the guard is dropped.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::fix_shared`].
    pub fn fix_shared_with<'a>(&'a self, swip: &Swip, hint: AccessHint) -> Result<SharedGuard<'a>> {
        self.ensure_running()?;
        let _operation = self
            .operation_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_running()?;
        let pid = self.validate_handle(swip)?;
        let mut faulted = false;
        let mut faulted_tier = None;
        loop {
            self.ensure_running()?;
            match swip.load() {
                SwipState::Hot(resident) | SwipState::Cooling(resident) => {
                    let Some(index) = self.frames.index_of_resident(resident) else {
                        return Err(TierBufError::InvalidPid(pid.get()));
                    };
                    let frame = self.frames.frame(index);
                    if !frame.try_pin() {
                        thread::yield_now();
                        continue;
                    }

                    let latch = frame.latch().lock_shared();
                    if self.resident_is_exact(swip, pid, resident, frame) {
                        if swip.try_resurrect(resident).is_ok() {
                            self.stats.record_second_chance();
                        }
                        self.policy.on_access(frame, AccessKind::Read);
                        if !faulted
                            && matches!(hint, AccessHint::Normal | AccessHint::Scan)
                            && self.consume_prefetched(pid, resident, frame.generation())
                        {
                            self.stats.record_prefetch_hit();
                        }
                        if faulted {
                            if let Some(tier_index) = faulted_tier {
                                self.stats.record_tier_demand_hit(tier_index);
                            } else {
                                self.stats.record_dram_hit();
                            }
                            if !self.policy.admit_to_dram(pid, hint)
                                && swip.try_mark_cooling(resident).is_ok()
                            {
                                self.enqueue_ticket(index, pid, resident);
                            }
                        } else {
                            self.stats.record_dram_hit();
                        }
                        return Ok(SharedGuard {
                            manager: self,
                            index,
                            pid,
                            swip: swip.clone(),
                            source: if faulted_tier.is_some() {
                                FixSource::LowerTier
                            } else {
                                FixSource::Dram
                            },
                            latch,
                        });
                    }

                    drop(latch);
                    frame.unpin();
                    thread::yield_now();
                }
                SwipState::Evicted(observed) => {
                    if observed != pid {
                        return Err(TierBufError::InvalidPid(observed.get()));
                    }
                    if !faulted {
                        self.stats.record_fault();
                        faulted = true;
                    }
                    if let Some(tier_index) = self.fault(swip, pid)? {
                        faulted_tier.get_or_insert(tier_index);
                    }
                }
            }
        }
    }

    /// Fixes a page for exclusive access.
    ///
    /// The page becomes dirty only when [`ExclusiveGuard::write_with`] is
    /// called.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::fix_shared`].
    pub fn fix_exclusive<'a>(&'a self, swip: &Swip) -> Result<ExclusiveGuard<'a>> {
        self.ensure_running()?;
        let _operation = self
            .operation_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_running()?;
        let pid = self.validate_handle(swip)?;
        let mut faulted = false;
        let mut faulted_tier = None;
        loop {
            self.ensure_running()?;
            match swip.load() {
                SwipState::Hot(resident) | SwipState::Cooling(resident) => {
                    let Some(index) = self.frames.index_of_resident(resident) else {
                        return Err(TierBufError::InvalidPid(pid.get()));
                    };
                    let frame = self.frames.frame(index);
                    if !frame.try_pin() {
                        thread::yield_now();
                        continue;
                    }

                    let latch = frame.latch().lock_exclusive();
                    if self.resident_is_exact(swip, pid, resident, frame) {
                        if swip.try_resurrect(resident).is_ok() {
                            self.stats.record_second_chance();
                        }
                        self.policy.on_access(frame, AccessKind::Write);
                        if faulted {
                            if let Some(tier_index) = faulted_tier {
                                self.stats.record_tier_demand_hit(tier_index);
                            } else {
                                self.stats.record_dram_hit();
                            }
                        } else {
                            if self.consume_prefetched(pid, resident, frame.generation()) {
                                self.stats.record_prefetch_hit();
                            }
                            self.stats.record_dram_hit();
                        }
                        return Ok(ExclusiveGuard {
                            manager: self,
                            index,
                            pid,
                            swip: swip.clone(),
                            latch,
                        });
                    }

                    drop(latch);
                    frame.unpin();
                    thread::yield_now();
                }
                SwipState::Evicted(observed) => {
                    if observed != pid {
                        return Err(TierBufError::InvalidPid(observed.get()));
                    }
                    if !faulted {
                        self.stats.record_fault();
                        faulted = true;
                    }
                    if let Some(tier_index) = self.fault(swip, pid)? {
                        faulted_tier.get_or_insert(tier_index);
                    }
                }
            }
        }
    }

    /// Captures a race-free page snapshot for optimistic validation.
    ///
    /// Snapshot creation briefly uses the ordinary shared fix path. The
    /// returned guard owns its 64-KiB copy and retains neither a pin nor a
    /// latch, so concurrent writers never race with reads of the snapshot.
    ///
    /// # Errors
    ///
    /// Returns the same fix and fault errors as [`Self::fix_shared`].
    pub fn fix_optimistic<'a>(&'a self, swip: &Swip) -> Result<OptimisticGuard<'a>> {
        let shared = self.fix_shared(swip)?;
        let index = shared.index;
        let pid = shared.pid;
        let resident = self.frames.resident_addr(index);
        let version = self
            .frames
            .frame(index)
            .latch()
            .optimistic()
            .map_err(|_| TierBufError::Contended)?;
        let snapshot = shared.read_with(|page| page.to_vec().into_boxed_slice());
        drop(shared);

        Ok(OptimisticGuard {
            manager: self,
            pid,
            swip: swip.clone(),
            resident,
            version,
            snapshot,
        })
    }

    /// Best-effort schedules evicted pages for background admission to DRAM.
    ///
    /// Submission never waits for queue space or I/O. Non-canonical,
    /// resident, policy-ineligible, duplicate, or resource-constrained
    /// requests are counted as skipped. At most
    /// [`BufConfig::max_prefetch_in_flight`] requests from one call may be
    /// accepted, and the same configured limit applies to queued-plus-running
    /// requests across the manager.
    pub fn prefetch(&self, swips: &[&Swip]) {
        let mut submitted = 0_u64;
        let mut skipped = 0_u64;
        let mut seen = HashSet::with_capacity(swips.len().min(self.max_prefetch_in_flight));

        for &swip in swips {
            if self.stopping.load(Ordering::Acquire)
                || submitted >= self.max_prefetch_in_flight as u64
            {
                skipped = skipped.saturating_add(1);
                continue;
            }
            let Some(pid) = self.canonical_evicted_pid(swip) else {
                skipped = skipped.saturating_add(1);
                continue;
            };
            if !seen.insert(pid) || !self.policy.admit_to_dram(pid, AccessHint::Prefetch) {
                skipped = skipped.saturating_add(1);
                continue;
            }

            let inserted = self
                .prefetch_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(pid);
            if !inserted {
                skipped = skipped.saturating_add(1);
                continue;
            }
            if try_update_usize(
                &self.prefetch_in_flight,
                Ordering::AcqRel,
                Ordering::Acquire,
                |current| (current < self.max_prefetch_in_flight).then_some(current + 1),
            )
            .is_err()
            {
                self.prefetch_pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&pid);
                skipped = skipped.saturating_add(1);
                continue;
            }

            let reservation = PrefetchReservation::new(
                pid,
                Arc::clone(&self.prefetch_in_flight),
                Arc::clone(&self.prefetch_pending),
            );
            let Some(checkout) = OwnedFreeFrame::try_checkout(Arc::clone(&self.frames)) else {
                drop(reservation);
                skipped = skipped.saturating_add(1);
                continue;
            };
            let task = PrefetchTask {
                pid,
                swip: swip.clone(),
                checkout: Some(checkout),
                _reservation: reservation,
            };
            if self.stopping.load(Ordering::Acquire) {
                drop(task);
                skipped = skipped.saturating_add(1);
                continue;
            }
            match self.prefetch_sender.try_send(task) {
                Ok(()) => submitted = submitted.saturating_add(1),
                Err(TrySendError::Full(_task) | TrySendError::Disconnected(_task)) => {
                    skipped = skipped.saturating_add(1);
                }
            }
        }

        self.stats.record_prefetch_submitted(submitted);
        self.stats.record_prefetch_skipped(skipped);
    }

    /// Returns an exact snapshot of completed buffer-manager operations.
    #[must_use]
    pub fn stats(&self) -> TierStats {
        self.stats.snapshot()
    }

    /// Returns cumulative DRAM and lower-tier residency cost.
    #[must_use]
    pub fn cost_report(&self) -> CostReport {
        self.residency_cost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .report()
    }

    /// Stops background work, flushes every resident page, and rejects new
    /// allocation and fix operations.
    ///
    /// Dirty pages are demoted through the same generation-checked eviction
    /// protocol as foreground pressure. The method waits briefly for guards
    /// that were pinned before shutdown began; if they do not drain, it
    /// returns [`TierBufError::Contended`] instead of deadlocking.
    ///
    /// # Errors
    ///
    /// Returns a storage error when a dirty page cannot be persisted,
    /// [`TierBufError::Contended`] when an outstanding guard does not drain,
    /// or an I/O error when a background or io_uring worker cannot shut down
    /// cleanly.
    pub fn shutdown(self: Arc<Self>) -> Result<()> {
        let _shutdown = self
            .shutdown_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.stopping.store(true, Ordering::Release);
        self.worker_signal.notify();
        self.cooler_signal.notify();
        let _operations = self
            .operation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let worker_result = self.join_workers();
        #[cfg(all(target_os = "linux", feature = "uring"))]
        let uring_result = self
            .uring_reader
            .as_ref()
            .map_or(Ok(()), UringReader::shutdown);
        #[cfg(not(all(target_os = "linux", feature = "uring")))]
        let uring_result: Result<()> = Ok(());
        let flush_result = self.flush_resident_pages();
        flush_result.and(worker_result).and(uring_result)
    }

    /// Advances policy time, write budgets, and residency-cost integration.
    #[allow(dead_code)] // Called by the epoch thread in the cooling phase.
    pub(crate) fn tick_epoch(&self, elapsed: Duration) {
        self.fault_ins_this_epoch.store(0, Ordering::Release);
        self.policy.on_epoch();
        for tier in &self.tiers {
            if let Some(budget) = tier.write_budget() {
                budget.tick(elapsed);
            }
        }

        let resident_frames = (0..self.frames.frame_count())
            .filter(|&index| self.frames.frame(index).pid().is_valid())
            .count();
        let dram_used_bytes = u64::try_from(resident_frames)
            .expect("resident frame count must fit u64")
            .checked_mul(PAGE_SIZE as u64)
            .expect("DRAM residency bytes must fit u64");
        let tier_samples = self
            .tiers
            .iter()
            .map(|tier| {
                TierResidencySample::new(tier.name(), tier.used_bytes(), tier.price_gb_month())
            })
            .collect::<Vec<_>>();

        self.residency_cost
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tick(elapsed, dram_used_bytes, &tier_samples);
    }

    fn start_workers(manager: &Arc<Self>) -> Result<()> {
        let mut workers = Vec::with_capacity(2 + manager.prefetch_workers);
        let cooler_manager = Arc::downgrade(manager);
        let cooler_stopping = Arc::clone(&manager.stopping);
        let cooler_signal = Arc::clone(&manager.cooler_signal);
        let cooler = match thread::Builder::new()
            .name("tierbuf-cooler".into())
            .spawn(move || {
                let mut observed_signal = 0;
                loop {
                    let Some(manager) = cooler_manager.upgrade() else {
                        break;
                    };
                    if cooler_stopping.load(Ordering::Acquire) {
                        break;
                    }
                    manager.background_cooling_step();
                    drop(manager);

                    if wait_for_worker(
                        &cooler_signal,
                        &cooler_stopping,
                        COOLER_INTERVAL,
                        &mut observed_signal,
                    ) {
                        break;
                    }
                }
            }) {
            Ok(worker) => worker,
            Err(error) => return Err(Self::worker_start_error(manager, workers, error)),
        };
        workers.push(cooler);

        let epoch_manager = Arc::downgrade(manager);
        let epoch_stopping = Arc::clone(&manager.stopping);
        let epoch_signal = Arc::clone(&manager.worker_signal);
        let epoch = manager.epoch;
        let epoch_worker =
            match thread::Builder::new()
                .name("tierbuf-epoch".into())
                .spawn(move || {
                    let mut observed_signal = 0;
                    loop {
                        if wait_for_worker(
                            &epoch_signal,
                            &epoch_stopping,
                            epoch,
                            &mut observed_signal,
                        ) {
                            break;
                        }
                        let Some(manager) = epoch_manager.upgrade() else {
                            break;
                        };
                        if epoch_stopping.load(Ordering::Acquire) {
                            break;
                        }
                        manager.tick_epoch(epoch);
                    }
                }) {
                Ok(worker) => worker,
                Err(error) => return Err(Self::worker_start_error(manager, workers, error)),
            };
        workers.push(epoch_worker);

        for worker_index in 0..manager.prefetch_workers {
            let prefetch_manager = Arc::downgrade(manager);
            let prefetch_stopping = Arc::clone(&manager.stopping);
            let receiver = Arc::clone(&manager.prefetch_receiver);
            let worker = match thread::Builder::new()
                .name(format!("tierbuf-prefetch-{worker_index}"))
                .spawn(move || {
                    loop {
                        let Some(mut task) = receive_prefetch_task(&receiver, &prefetch_stopping)
                        else {
                            break;
                        };
                        if prefetch_stopping.load(Ordering::Acquire) {
                            continue;
                        }
                        let Some(manager) = prefetch_manager.upgrade() else {
                            break;
                        };
                        let _ = manager.prefetch_fault(&mut task);
                    }
                }) {
                Ok(worker) => worker,
                Err(error) => return Err(Self::worker_start_error(manager, workers, error)),
            };
            workers.push(worker);
        }
        manager
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(workers);
        Ok(())
    }

    fn worker_start_error(
        manager: &Self,
        workers: Vec<JoinHandle<()>>,
        error: io::Error,
    ) -> TierBufError {
        manager.stopping.store(true, Ordering::Release);
        manager.worker_signal.notify();
        manager.cooler_signal.notify();
        for worker in workers {
            let _ = worker.join();
        }
        TierBufError::Io(error)
    }

    fn join_workers(&self) -> Result<()> {
        let current = thread::current().id();
        let workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut panicked = false;
        for worker in workers {
            if worker.thread().id() == current {
                continue;
            }
            panicked |= worker.join().is_err();
        }
        if panicked {
            Err(TierBufError::Io(io::Error::other(
                "a tierbuf background worker panicked",
            )))
        } else {
            Ok(())
        }
    }

    fn ensure_running(&self) -> Result<()> {
        if self.stopping.load(Ordering::Acquire) {
            Err(TierBufError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    fn canonical_evicted_pid(&self, swip: &Swip) -> Option<PageId> {
        let pid = swip.pid();
        if !swip.belongs_to(self.id)
            || !matches!(swip.load(), SwipState::Evicted(observed) if observed == pid)
        {
            return None;
        }
        self.pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pid)
            .filter(|control| control.swip.shares_state_with(swip))
            .map(|_| pid)
    }

    fn consume_prefetched(&self, pid: PageId, resident: ResidentAddr, generation: u32) -> bool {
        let marker = self
            .prefetched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&pid);
        marker
            == Some(PrefetchMarker {
                resident,
                generation,
            })
    }

    fn acquire_frame(&self) -> Result<FreeFrame<'_>> {
        self.ensure_running()?;
        if let Some(index) = self.frames.pop_free() {
            return Ok(FreeFrame::from_index(&self.frames, index));
        }
        self.exhausted_frame_request.store(true, Ordering::Release);
        self.cooler_signal.notify();

        let windows = self.frames.frame_count().div_ceil(MAX_COOLING_SAMPLE);
        let mut last_error = None;
        for _ in 0..windows.saturating_add(2) {
            self.ensure_running()?;
            if let Some(index) = self.frames.pop_free() {
                return Ok(FreeFrame::from_index(&self.frames, index));
            }

            self.enqueue_cold_candidates(true);
            let attempts = self.cooling.len();
            for _ in 0..attempts {
                let Some(ticket) = self.cooling.pop() else {
                    break;
                };
                match self.evict_ticket(ticket) {
                    Ok(EvictionOutcome::Freed) => {
                        if let Some(index) = self.frames.pop_free() {
                            return Ok(FreeFrame::from_index(&self.frames, index));
                        }
                    }
                    Ok(EvictionOutcome::Busy) => {
                        self.cooling.push_unique(ticket);
                    }
                    Ok(EvictionOutcome::Stale) => {}
                    Err(error) => last_error = Some(error),
                }
            }
            thread::yield_now();
        }

        last_error.map_or(Err(TierBufError::PoolExhausted), Err)
    }

    fn enqueue_cold_candidates(&self, force_one: bool) {
        let desired = if force_one {
            self.cooling
                .len()
                .saturating_add(1)
                .max(self.cooling_target)
        } else {
            self.cooling_target
        };
        if desired == 0 || self.cooling.len() >= desired {
            return;
        }

        let frame_count = self.frames.frame_count();
        let sample_count = frame_count.min(MAX_COOLING_SAMPLE);
        let advance = u64::try_from(sample_count).expect("sample count must fit u64");
        let start = usize::try_from(
            self.sample_cursor.fetch_add(advance, Ordering::AcqRel)
                % u64::try_from(frame_count).expect("frame count must fit u64"),
        )
        .expect("sample cursor must fit usize");
        let mut candidates = (0..sample_count)
            .map(|offset| (start + offset) % frame_count)
            .filter_map(|index| {
                let frame = self.frames.frame(index);
                (frame.pid().is_valid() && frame.pin_count() == 0 && !frame.is_evicting())
                    .then_some((frame.heat_and_epoch().0, index))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|&(heat, index)| (heat, index));

        for (_, index) in candidates {
            if self.cooling.len() >= desired {
                break;
            }
            let frame = self.frames.frame(index);
            if frame.pin_count() != 0 || frame.is_evicting() {
                continue;
            }
            let pid = frame.pid();
            let resident = self.frames.resident_addr(index);
            let Some(owner) = frame.owner_swip() else {
                continue;
            };
            match owner.load() {
                SwipState::Hot(observed) if observed == resident => {
                    if owner.try_mark_cooling(resident).is_ok() {
                        self.enqueue_ticket(index, pid, resident);
                    }
                }
                SwipState::Cooling(observed) if observed == resident => {
                    self.enqueue_ticket(index, pid, resident);
                }
                SwipState::Hot(_) | SwipState::Cooling(_) | SwipState::Evicted(_) => {}
            }
        }
    }

    fn enqueue_ticket(&self, index: usize, pid: PageId, resident: ResidentAddr) {
        let frame = self.frames.frame(index);
        if frame.pid() == pid
            && self.frames.resident_addr(index) == resident
            && frame.owner_swip().is_some_and(|owner| {
                matches!(
                    owner.load(),
                    SwipState::Cooling(observed) if observed == resident
                )
            })
        {
            self.cooling.push_unique(CoolingTicket {
                frame_index: index,
                pid,
                generation: frame.generation(),
                resident,
            });
        }
    }

    fn background_cooling_step(&self) {
        if self.stopping.load(Ordering::Acquire)
            || self.frames.free_count() >= self.low_watermark
            || self.cooling_target == 0
        {
            return;
        }
        let exhausted_request = self.exhausted_frame_request.swap(false, Ordering::AcqRel);
        if self.eviction_mode == EvictionMode::Demand
            && self.fault_ins_this_epoch.load(Ordering::Acquire) == 0
            && !exhausted_request
        {
            return;
        }

        if self.cooling.len() < self.cooling_target {
            self.enqueue_cold_candidates(false);
        }

        let attempts = self.cooling.len();
        for _ in 0..attempts {
            if self.frames.free_count() >= self.low_watermark {
                break;
            }
            let Some(ticket) = self.cooling.pop() else {
                break;
            };
            match self.evict_ticket(ticket) {
                Ok(EvictionOutcome::Busy) => {
                    self.cooling.push_unique(ticket);
                }
                Ok(EvictionOutcome::Freed | EvictionOutcome::Stale) | Err(_) => {}
            }
        }
    }

    fn exact_cooling_owner(&self, ticket: CoolingTicket) -> Option<Swip> {
        if ticket.frame_index >= self.frames.frame_count()
            || self.frames.resident_addr(ticket.frame_index) != ticket.resident
        {
            return None;
        }
        let frame = self.frames.frame(ticket.frame_index);
        if frame.pid() != ticket.pid || frame.generation() != ticket.generation {
            return None;
        }
        let owner = frame.owner_swip()?;
        if owner.pid() != ticket.pid
            || !matches!(
                owner.load(),
                SwipState::Cooling(observed) if observed == ticket.resident
            )
        {
            return None;
        }
        let canonical = self
            .pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&ticket.pid)
            .is_some_and(|control| control.swip.shares_state_with(&owner));
        canonical.then_some(owner)
    }

    fn evict_ticket(&self, ticket: CoolingTicket) -> Result<EvictionOutcome> {
        let Some(owner) = self.exact_cooling_owner(ticket) else {
            return Ok(EvictionOutcome::Stale);
        };
        let frame = self.frames.frame(ticket.frame_index);
        if !frame.try_claim_eviction() {
            return Ok(EvictionOutcome::Busy);
        }
        let reservation = EvictionReservation::new(frame);
        let latch = frame.latch().lock_exclusive();

        let still_exact = self
            .exact_cooling_owner(ticket)
            .is_some_and(|observed| observed.shares_state_with(&owner));
        if !still_exact {
            let same_occupant = frame.pid() == ticket.pid
                && frame.generation() == ticket.generation
                && frame
                    .owner_swip()
                    .is_some_and(|observed| observed.shares_state_with(&owner));
            if same_occupant {
                let _ = owner.try_resurrect(ticket.resident);
            }
            drop(latch);
            return Ok(EvictionOutcome::Stale);
        }

        let old_location = self.directory.backing(ticket.pid);
        let was_dirty = frame.is_dirty();
        let needs_write = was_dirty || old_location.is_none();
        let mut new_location = None;
        if needs_write {
            let tier_info = self.live_tier_info();
            let Some(start) = self
                .policy
                .demotion_target(frame, &tier_info)
                .filter(|&index| index < self.tiers.len())
            else {
                let _ = owner.try_resurrect(ticket.resident);
                drop(latch);
                return Err(TierBufError::TierExhausted {
                    tier: "no eligible lower tier".to_owned(),
                });
            };
            let mut last_error = None;

            for tier_index in start..self.tiers.len() {
                let tier = &self.tiers[tier_index];
                if tier
                    .write_budget()
                    .is_some_and(|budget| !budget.try_consume(PAGE_SIZE as u64))
                {
                    self.stats.record_budget_denied();
                    last_error = Some(TierBufError::TierExhausted {
                        tier: tier.name().to_owned(),
                    });
                    continue;
                }

                match self
                    .frames
                    .write_latched_with(ticket.frame_index, &latch, |page| tier.write(&page[..]))
                {
                    Ok(offset) => {
                        self.stats.record_tier_write(tier_index, PAGE_SIZE as u64);
                        new_location = Some(Location { tier_index, offset });
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }

            if new_location.is_none() {
                let _ = owner.try_resurrect(ticket.resident);
                drop(latch);
                return Err(last_error.unwrap_or_else(|| TierBufError::TierExhausted {
                    tier: self
                        .tiers
                        .last()
                        .map_or_else(|| "unknown".into(), |tier| tier.name().to_owned()),
                }));
            }
        }

        let replacement = new_location
            .or(old_location)
            .expect("dirty-or-unbacked eviction must write; clean eviction must have backing");
        let replaced = new_location.map(|location| self.directory.insert(ticket.pid, location));
        frame.set_dirty(false);

        if owner.try_unswizzle(ticket.resident, ticket.pid).is_err() {
            if let Some(previous) = replaced {
                self.directory.restore(ticket.pid, previous);
                if previous != Some(replacement) {
                    let tier = &self.tiers[replacement.tier_index];
                    tier.free(replacement.offset);
                }
            }
            frame.set_dirty(was_dirty);
            let _ = owner.try_resurrect(ticket.resident);
            drop(latch);
            return Ok(EvictionOutcome::Stale);
        }
        self.consume_prefetched(ticket.pid, ticket.resident, ticket.generation);
        drop(latch);

        frame.clear_after_eviction();
        reservation.release();
        self.frames
            .push_free(ticket.frame_index)
            .expect("an evicted frame must fit back in its free queue");
        self.stats.record_eviction();

        if let Some(old) = old_location.filter(|old| *old != replacement)
            && new_location.is_some()
            && let Some(old_tier) = self.tiers.get(old.tier_index)
        {
            old_tier.free(old.offset);
        }
        Ok(EvictionOutcome::Freed)
    }

    fn live_tier_info(&self) -> Vec<TierInfo> {
        self.tiers
            .iter()
            .enumerate()
            .map(|(index, tier)| {
                let request_costs = tier.request_costs();
                TierInfo::new(
                    index,
                    tier.name(),
                    tier.price_gb_month(),
                    tier.latency().read_us_p50 as f64,
                    request_costs.read_usd,
                    request_costs.write_usd,
                    tier.write_budget().map(|budget| budget.available_bytes()),
                )
            })
            .collect()
    }

    fn flush_resident_pages(&self) -> Result<()> {
        let deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
        loop {
            let mut resident = 0_usize;
            let mut made_progress = false;
            for index in 0..self.frames.frame_count() {
                let frame = self.frames.frame(index);
                let pid = frame.pid();
                if !pid.is_valid() {
                    continue;
                }
                resident += 1;
                let resident_addr = self.frames.resident_addr(index);
                let Some(owner) = frame.owner_swip() else {
                    continue;
                };
                match owner.load() {
                    SwipState::Hot(observed) if observed == resident_addr => {
                        if owner.try_mark_cooling(resident_addr).is_err() {
                            continue;
                        }
                    }
                    SwipState::Cooling(observed) if observed == resident_addr => {}
                    SwipState::Hot(_) | SwipState::Cooling(_) | SwipState::Evicted(_) => {
                        continue;
                    }
                }
                let ticket = CoolingTicket {
                    frame_index: index,
                    pid,
                    generation: frame.generation(),
                    resident: resident_addr,
                };
                if self.evict_ticket(ticket)? == EvictionOutcome::Freed {
                    made_progress = true;
                }
            }

            if resident == 0 {
                return Ok(());
            }
            if !made_progress && Instant::now() >= deadline {
                return Err(TierBufError::Contended);
            }
            thread::yield_now();
        }
    }

    fn validate_handle(&self, swip: &Swip) -> Result<PageId> {
        let pid = swip.pid();
        if swip.belongs_to(self.id) {
            return Ok(pid);
        }

        let pages = self
            .pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pages.contains_key(&pid) {
            Err(TierBufError::DuplicateHandle(pid.get()))
        } else {
            Err(TierBufError::InvalidPid(pid.get()))
        }
    }

    fn allocate_pid(&self) -> Result<PageId> {
        let raw = try_update_u64(
            &self.next_pid,
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| (current <= PageId::MAX).then_some(current + 1),
        )
        .map_err(|_| {
            TierBufError::InvalidConfig("logical page identifier space exhausted".into())
        })?;
        PageId::new(raw).ok_or(TierBufError::InvalidPid(raw))
    }

    fn resident_is_exact(
        &self,
        swip: &Swip,
        pid: PageId,
        resident: ResidentAddr,
        frame: &Frame,
    ) -> bool {
        let state_matches = matches!(
            swip.load(),
            SwipState::Hot(observed) | SwipState::Cooling(observed)
                if observed == resident
        );
        let owner_matches = frame
            .owner_swip()
            .is_some_and(|owner| owner.shares_state_with(swip));
        state_matches && frame.pid() == pid && owner_matches
    }

    fn prefetch_fault(&self, task: &mut PrefetchTask) -> Result<bool> {
        self.ensure_running()?;
        let control = self
            .pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&task.pid)
            .cloned()
            .ok_or(TierBufError::InvalidPid(task.pid.get()))?;
        if !control.swip.shares_state_with(&task.swip) {
            return Err(TierBufError::DuplicateHandle(task.pid.get()));
        }

        let leader = match control.fault.enter() {
            FaultTurn::Follower(result) => return result.map(|_| false),
            FaultTurn::Leader(leader) => leader,
        };
        let result = self.perform_prefetch_fault(task);
        leader.finish(&result);
        result.map(|tier_index| tier_index.is_some())
    }

    fn perform_prefetch_fault(&self, task: &mut PrefetchTask) -> Result<Option<usize>> {
        self.ensure_running()?;
        match task.swip.load() {
            SwipState::Hot(_) | SwipState::Cooling(_) => return Ok(None),
            SwipState::Evicted(observed) if observed == task.pid => {}
            SwipState::Evicted(observed) => {
                return Err(TierBufError::InvalidPid(observed.get()));
            }
        }

        let location = self
            .directory
            .lookup(task.pid)
            .ok_or(TierBufError::InvalidPid(task.pid.get()))?;
        let tier = self.tiers.get(location.tier_index).ok_or_else(|| {
            TierBufError::InvalidConfig(format!(
                "page {} refers to missing tier {}",
                task.pid, location.tier_index
            ))
        })?;
        let mut checkout = task
            .checkout
            .take()
            .expect("accepted prefetch must retain its frame");
        let index = checkout.index;

        self.stats.record_fault();
        self.frames.write_with(index, |page| {
            self.read_backing_page(tier.as_ref(), location.offset, &mut page[..])
        })?;
        self.stats
            .record_tier_read(location.tier_index, PAGE_SIZE as u64);
        let frame = self.frames.frame(index);
        frame.install(task.pid, task.swip.clone());
        checkout.publish_pin();
        let resident = self.frames.resident_addr(index);
        let marker = PrefetchMarker {
            resident,
            generation: frame.generation(),
        };
        let previous = self
            .prefetched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.pid, marker);
        debug_assert!(
            previous.is_none(),
            "an evicted page cannot retain a prior prefetch marker"
        );
        if task.swip.try_swizzle(task.pid, resident).is_err() {
            let mut prefetched = self
                .prefetched
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if prefetched.get(&task.pid) == Some(&marker) {
                prefetched.remove(&task.pid);
            }
            frame.unpin();
            self.clear_unpublished_frame(index);
            return Err(TierBufError::Contended);
        }

        checkout.commit();
        frame.unpin();
        Ok(Some(location.tier_index))
    }

    fn fault(&self, swip: &Swip, pid: PageId) -> Result<Option<usize>> {
        self.ensure_running()?;
        let control = self
            .pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pid)
            .cloned()
            .ok_or(TierBufError::InvalidPid(pid.get()))?;
        if !control.swip.shares_state_with(swip) {
            return Err(TierBufError::DuplicateHandle(pid.get()));
        }

        let leader = match control.fault.enter() {
            FaultTurn::Follower(result) => return result,
            FaultTurn::Leader(leader) => leader,
        };
        let result = self.perform_demand_fault(swip, pid);
        leader.finish(&result);
        result
    }

    fn perform_demand_fault(&self, swip: &Swip, pid: PageId) -> Result<Option<usize>> {
        self.ensure_running()?;
        match swip.load() {
            SwipState::Hot(_) | SwipState::Cooling(_) => return Ok(None),
            SwipState::Evicted(observed) if observed == pid => {}
            SwipState::Evicted(observed) => {
                return Err(TierBufError::InvalidPid(observed.get()));
            }
        }

        let location = self
            .directory
            .lookup(pid)
            .ok_or(TierBufError::InvalidPid(pid.get()))?;
        let tier = self.tiers.get(location.tier_index).ok_or_else(|| {
            TierBufError::InvalidConfig(format!(
                "page {pid} refers to missing tier {}",
                location.tier_index
            ))
        })?;
        if self.eviction_mode == EvictionMode::Demand {
            self.fault_ins_this_epoch.fetch_add(1, Ordering::Release);
        }
        let mut checkout = self.acquire_frame()?;
        let index = checkout.index;

        self.frames.write_with(index, |page| {
            self.read_backing_page(tier.as_ref(), location.offset, &mut page[..])
        })?;
        self.stats
            .record_tier_read(location.tier_index, PAGE_SIZE as u64);
        let frame = self.frames.frame(index);
        frame.install(pid, swip.clone());
        checkout.publish_pin();
        let resident = self.frames.resident_addr(index);
        if swip.try_swizzle(pid, resident).is_err() {
            frame.unpin();
            self.clear_unpublished_frame(index);
            return Err(TierBufError::Contended);
        }

        checkout.commit();
        frame.unpin();
        Ok(Some(location.tier_index))
    }

    fn read_backing_page(
        &self,
        tier: &dyn TierBackend,
        location: TierOffset,
        buffer: &mut [u8],
    ) -> Result<()> {
        #[cfg(all(target_os = "linux", feature = "uring"))]
        if let (Some(reader), Some(raw_fd)) = (&self.uring_reader, tier.raw_fd()) {
            return reader.read(raw_fd, location, buffer);
        }

        tier.read(location, buffer)
    }

    fn clear_unpublished_frame(&self, index: usize) {
        let frame = self.frames.frame(index);
        assert!(
            frame.try_claim_eviction(),
            "unpublished frame must be unpinned"
        );
        frame.clear_after_eviction();
        frame.release_eviction();
    }

    #[cfg(test)]
    fn directory_lookup_count(&self) -> u64 {
        self.directory.lookup_count()
    }

    #[cfg(test)]
    fn manual_evict(&self, swip: &Swip) -> Result<()> {
        let pid = self.validate_handle(swip)?;
        let resident = match swip.load() {
            SwipState::Hot(resident) | SwipState::Cooling(resident) => resident,
            SwipState::Evicted(_) => return Ok(()),
        };
        let index = self
            .frames
            .index_of_resident(resident)
            .ok_or(TierBufError::InvalidPid(pid.get()))?;
        let frame = self.frames.frame(index);
        match swip.load() {
            SwipState::Hot(observed) if observed == resident => {
                swip.try_mark_cooling(resident)
                    .map_err(|_| TierBufError::Contended)?;
            }
            SwipState::Cooling(observed) if observed == resident => {}
            _ => return Err(TierBufError::Contended),
        }

        let ticket = CoolingTicket {
            frame_index: index,
            pid,
            generation: frame.generation(),
            resident,
        };
        match self.evict_ticket(ticket)? {
            EvictionOutcome::Freed => Ok(()),
            EvictionOutcome::Stale | EvictionOutcome::Busy => {
                let _ = swip.try_resurrect(resident);
                Err(TierBufError::Contended)
            }
        }
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.worker_signal.notify();
        self.cooler_signal.notify();
        let pages = self
            .pages
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (&pid, control) in pages.iter() {
            loop {
                match control.swip.load() {
                    SwipState::Hot(resident) => {
                        let _ = control.swip.try_mark_cooling(resident);
                    }
                    SwipState::Cooling(resident) => {
                        let _ = control.swip.try_unswizzle(resident, pid);
                        break;
                    }
                    SwipState::Evicted(_) => break,
                }
            }
        }
    }
}

/// An owned page snapshot with deferred optimistic validation.
///
/// The snapshot contains exactly [`PAGE_SIZE`] bytes copied under a shared
/// latch. It holds no pin and cannot race with subsequent frame mutation.
pub struct OptimisticGuard<'a> {
    manager: &'a BufferManager,
    pid: PageId,
    swip: Swip,
    resident: ResidentAddr,
    version: u64,
    snapshot: Box<[u8]>,
}

impl OptimisticGuard<'_> {
    /// Returns the snapshotted logical page identifier.
    #[must_use]
    pub fn pid(&self) -> PageId {
        self.pid
    }

    /// Returns a clone of the canonical tagged page handle.
    #[must_use]
    pub fn swip(&self) -> Swip {
        self.swip.clone()
    }

    /// Runs `read` on the owned snapshot, then validates its source frame.
    ///
    /// The closure runs before validation. If it performs externally visible
    /// side effects, those effects cannot be rolled back when this method
    /// returns [`TierBufError::Retry`]. Callers should therefore keep the
    /// closure side-effect free or make it idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Retry`] if a writer changed the source latch,
    /// the page was evicted, or its resident frame identity changed after the
    /// snapshot was captured.
    pub fn read_with<R>(&self, read: impl FnOnce(&[u8]) -> R) -> Result<R> {
        let result = read(&self.snapshot);
        let Some(index) = self.manager.frames.index_of_resident(self.resident) else {
            return Err(TierBufError::Retry);
        };
        let frame = self.manager.frames.frame(index);
        if self
            .manager
            .resident_is_exact(&self.swip, self.pid, self.resident, frame)
            && frame.latch().validate(self.version)
        {
            Ok(result)
        } else {
            Err(TierBufError::Retry)
        }
    }
}

/// A shared, pinned page guard.
///
/// Page bytes are available only through [`Self::read_with`], so references
/// cannot escape the shared latch lifetime.
pub struct SharedGuard<'a> {
    manager: &'a BufferManager,
    index: usize,
    pid: PageId,
    swip: Swip,
    source: FixSource,
    latch: SharedRaw<'a>,
}

impl SharedGuard<'_> {
    /// Returns the guarded logical page identifier.
    #[must_use]
    pub fn pid(&self) -> PageId {
        self.pid
    }

    /// Returns a clone of the canonical tagged page handle.
    #[must_use]
    pub fn swip(&self) -> Swip {
        self.swip.clone()
    }

    /// Returns whether this fix was served from DRAM or a lower tier.
    #[must_use]
    pub const fn source(&self) -> FixSource {
        self.source
    }

    /// Runs `read` with an immutable view of the complete fixed-size page.
    pub fn read_with<R>(&self, read: impl FnOnce(&[u8; PAGE_SIZE]) -> R) -> R {
        self.manager
            .frames
            .read_latched_with(self.index, &self.latch, read)
    }
}

impl Drop for SharedGuard<'_> {
    fn drop(&mut self) {
        self.manager.frames.frame(self.index).unpin();
    }
}

/// An exclusive, pinned page guard.
///
/// Calling [`Self::write_with`] marks the frame dirty before invoking the
/// closure. References to page bytes cannot escape either closure API.
pub struct ExclusiveGuard<'a> {
    manager: &'a BufferManager,
    index: usize,
    pid: PageId,
    swip: Swip,
    latch: ExclusiveRaw<'a>,
}

impl ExclusiveGuard<'_> {
    /// Returns the guarded logical page identifier.
    #[must_use]
    pub fn pid(&self) -> PageId {
        self.pid
    }

    /// Returns a clone of the canonical tagged page handle.
    #[must_use]
    pub fn swip(&self) -> Swip {
        self.swip.clone()
    }

    /// Runs `read` with an immutable view of the complete fixed-size page.
    pub fn read_with<R>(&self, read: impl FnOnce(&[u8; PAGE_SIZE]) -> R) -> R {
        self.manager
            .frames
            .write_latched_with(self.index, &self.latch, |page| read(page))
    }

    /// Runs `write` with a mutable view of the complete fixed-size page.
    ///
    /// The frame is marked dirty before the closure runs, including when the
    /// closure unwinds.
    pub fn write_with<R>(&mut self, write: impl FnOnce(&mut [u8; PAGE_SIZE]) -> R) -> R {
        self.manager.frames.frame(self.index).set_dirty(true);
        self.manager
            .frames
            .write_latched_with(self.index, &self.latch, write)
    }
}

impl Drop for ExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.manager.frames.frame(self.index).unpin();
    }
}

struct FreeFrame<'a> {
    frames: &'a FrameTable,
    index: usize,
    active: bool,
    reserved: bool,
}

impl<'a> FreeFrame<'a> {
    fn from_index(frames: &'a FrameTable, index: usize) -> Self {
        let frame = frames.frame(index);
        while !frame.try_claim_eviction() {
            // A fixer may have loaded the old resident Swip immediately
            // before eviction, then transiently pin this now-free frame. Its
            // mandatory post-latch revalidation will fail and remove that
            // pin. Holding off publication here keeps the frame unavailable
            // to both stale fixers and stale cooling workers until then.
            thread::yield_now();
        }
        Self {
            frames,
            index,
            active: true,
            reserved: true,
        }
    }

    fn publish_pin(&mut self) {
        assert!(self.active && self.reserved);
        self.frames.frame(self.index).publish_reserved_pin();
        self.reserved = false;
    }

    fn commit(mut self) {
        assert!(!self.reserved, "a committed frame must own its first pin");
        self.active = false;
    }
}

impl Drop for FreeFrame<'_> {
    fn drop(&mut self) {
        if self.active {
            if self.reserved {
                self.frames.frame(self.index).release_eviction();
            }
            self.frames
                .push_free(self.index)
                .expect("checked-out frame must fit back in its free queue");
        }
    }
}

struct OwnedFreeFrame {
    frames: Arc<FrameTable>,
    index: usize,
    active: bool,
    reserved: bool,
}

impl OwnedFreeFrame {
    fn try_checkout(frames: Arc<FrameTable>) -> Option<Self> {
        let index = frames.pop_free()?;
        if !frames.frame(index).try_claim_eviction() {
            frames
                .push_free(index)
                .expect("temporarily pinned free frame must return to its queue");
            return None;
        }
        Some(Self {
            frames,
            index,
            active: true,
            reserved: true,
        })
    }

    fn publish_pin(&mut self) {
        assert!(self.active && self.reserved);
        self.frames.frame(self.index).publish_reserved_pin();
        self.reserved = false;
    }

    fn commit(mut self) {
        assert!(!self.reserved, "a committed frame must own its first pin");
        self.active = false;
    }
}

impl Drop for OwnedFreeFrame {
    fn drop(&mut self) {
        if self.active {
            if self.reserved {
                self.frames.frame(self.index).release_eviction();
            }
            self.frames
                .push_free(self.index)
                .expect("prefetch frame must fit back in its free queue");
        }
    }
}

struct EvictionReservation<'a> {
    frame: &'a Frame,
    active: bool,
}

impl<'a> EvictionReservation<'a> {
    const fn new(frame: &'a Frame) -> Self {
        Self {
            frame,
            active: true,
        }
    }

    fn release(mut self) {
        self.frame.release_eviction();
        self.active = false;
    }
}

impl Drop for EvictionReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.frame.release_eviction();
        }
    }
}

fn receive_prefetch_task(
    receiver: &Mutex<Receiver<PrefetchTask>>,
    stopping: &AtomicBool,
) -> Option<PrefetchTask> {
    loop {
        let receiver = receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stopping.load(Ordering::Acquire) {
            return receiver.try_recv().ok();
        }
        match receiver.recv_timeout(PREFETCH_RECEIVE_TIMEOUT) {
            Ok(task) => return Some(task),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return None,
        }
    }
}

fn wait_for_worker(
    signal: &WorkerSignal,
    stopping: &AtomicBool,
    timeout: Duration,
    observed_generation: &mut u64,
) -> bool {
    if stopping.load(Ordering::Acquire) {
        return true;
    }
    let guard = signal
        .generation
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if stopping.load(Ordering::Acquire) {
        return true;
    }
    if *guard != *observed_generation {
        *observed_generation = *guard;
        return false;
    }
    let _waited = signal
        .wakeup
        .wait_timeout_while(guard, timeout, |generation| {
            !stopping.load(Ordering::Acquire) && *generation == *observed_generation
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *observed_generation = *_waited.0;
    stopping.load(Ordering::Acquire)
}

fn validate_config(config: &BufConfig) -> Result<()> {
    if config.dram_pool_bytes == 0 || !config.dram_pool_bytes.is_multiple_of(PAGE_SIZE) {
        return Err(TierBufError::InvalidConfig(
            "dram_pool_bytes must be a non-zero multiple of PAGE_SIZE".into(),
        ));
    }
    if !config.cooling_ratio.is_finite() || !(0.0..=1.0).contains(&config.cooling_ratio) {
        return Err(TierBufError::InvalidConfig(
            "cooling_ratio must be finite and within 0.0..=1.0".into(),
        ));
    }
    if !config.economics.dram_price_gb_month.is_finite()
        || config.economics.dram_price_gb_month.is_sign_negative()
    {
        return Err(TierBufError::InvalidConfig(
            "DRAM price must be finite and non-negative".into(),
        ));
    }
    if config.economics.epoch.is_zero() {
        return Err(TierBufError::InvalidConfig(
            "economics epoch must be greater than zero".into(),
        ));
    }
    if !(1..=256).contains(&config.prefetch_workers) {
        return Err(TierBufError::InvalidConfig(
            "prefetch_workers must be within 1..=256".into(),
        ));
    }
    if config.max_prefetch_in_flight < config.prefetch_workers
        || config.max_prefetch_in_flight > 4096
    {
        return Err(TierBufError::InvalidConfig(
            "max_prefetch_in_flight must be within prefetch_workers..=4096".into(),
        ));
    }
    if config.tiers.is_empty() {
        return Err(TierBufError::InvalidConfig(
            "at least one lower storage tier is required".into(),
        ));
    }
    Ok(())
}

fn allocate_manager_id() -> Result<u64> {
    try_update_u64(
        &NEXT_MANAGER_ID,
        Ordering::AcqRel,
        Ordering::Acquire,
        |current| current.checked_add(1),
    )
    .map_err(|_| TierBufError::InvalidConfig("buffer-manager identity space exhausted".into()))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier, Condvar, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::frame::{Frame, FrameTable};
    use crate::policy::{AccessHint, AccessKind, PlacementPolicy, TierInfo};
    use crate::swip::{Swip, SwipState};
    use crate::tier::mock::MockTier;
    use crate::tier::{LatencyProfile, TierBackend, TierOffset, WriteBudget};
    use crate::{PAGE_SIZE, TierBufError};

    use super::{
        BufConfig, BufferManager, CoolingTicket, Economics, EvictionMode, EvictionOutcome,
        FixSource, FreeFrame, MAX_PREFETCH_IN_FLIGHT, PREFETCH_WORKERS,
    };

    #[derive(Debug)]
    struct WarmFirstPolicy;

    #[derive(Debug)]
    struct NoTierPolicy;

    #[derive(Debug, Default)]
    struct ReadFailureInjection {
        attempts: AtomicUsize,
        released: Mutex<bool>,
        started: Condvar,
        release: Condvar,
    }

    impl ReadFailureInjection {
        fn wait_until_started(&self, timeout: Duration) {
            let released = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (_released, waited) = self
                .started
                .wait_timeout_while(released, timeout, |_| {
                    self.attempts.load(Ordering::Acquire) == 0
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!waited.timed_out(), "injected read did not start");
        }

        fn release_failure(&self) {
            *self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            self.release.notify_all();
        }
    }

    #[derive(Debug)]
    struct BlockingFailureTier {
        inner: MockTier,
        injection: Arc<ReadFailureInjection>,
    }

    impl BlockingFailureTier {
        fn new(injection: Arc<ReadFailureInjection>) -> Self {
            Self {
                inner: MockTier::new((PAGE_SIZE * 8) as u64).expect("valid mock tier"),
                injection,
            }
        }
    }

    impl TierBackend for BlockingFailureTier {
        fn name(&self) -> &str {
            "blocking-failure"
        }

        fn read(&self, location: TierOffset, buffer: &mut [u8]) -> crate::Result<()> {
            let attempt = self.injection.attempts.fetch_add(1, Ordering::AcqRel) + 1;
            if attempt == 1 {
                let mut released = self
                    .injection
                    .released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.injection.started.notify_all();
                while !*released {
                    released = self
                        .injection
                        .release
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "coalesced injected read failure",
                )
                .into());
            }
            self.inner.read(location, buffer)
        }

        fn write(&self, buffer: &[u8]) -> crate::Result<TierOffset> {
            self.inner.write(buffer)
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

    impl PlacementPolicy for WarmFirstPolicy {
        fn on_access(&self, _frame: &Frame, _kind: AccessKind) {}

        fn on_epoch(&self) {}

        fn demotion_target(&self, _frame: &Frame, _tiers: &[TierInfo]) -> Option<usize> {
            Some(0)
        }

        fn admit_to_dram(&self, _pid: crate::swip::PageId, _hint: AccessHint) -> bool {
            true
        }
    }

    impl PlacementPolicy for NoTierPolicy {
        fn on_access(&self, _frame: &Frame, _kind: AccessKind) {}

        fn on_epoch(&self) {}

        fn demotion_target(&self, _frame: &Frame, _tiers: &[TierInfo]) -> Option<usize> {
            None
        }

        fn admit_to_dram(&self, _pid: crate::swip::PageId, _hint: AccessHint) -> bool {
            true
        }
    }

    fn manager(frame_count: usize, read_latency_us: u64) -> Arc<BufferManager> {
        manager_with_mode(frame_count, read_latency_us, EvictionMode::Demand)
    }

    fn manager_with_mode(
        frame_count: usize,
        read_latency_us: u64,
        eviction_mode: EvictionMode,
    ) -> Arc<BufferManager> {
        let tier = MockTier::with_options(
            "mock",
            (PAGE_SIZE * frame_count.saturating_mul(4).max(16)) as u64,
            0.0,
            LatencyProfile::new(read_latency_us, 0, 0.0),
            None,
        )
        .expect("valid mock tier");
        BufferManager::new(BufConfig {
            dram_pool_bytes: frame_count * PAGE_SIZE,
            cooling_ratio: 0.1,
            eviction_mode,
            economics: Economics::default(),
            tiers: vec![Box::new(tier) as Box<dyn TierBackend>],
            ..BufConfig::default()
        })
        .expect("valid buffer manager")
    }

    fn stop_background_workers(manager: &BufferManager) {
        manager.stopping.store(true, Ordering::Release);
        manager.worker_signal.notify();
        manager.cooler_signal.notify();
        manager.join_workers().expect("background workers stop");
        manager.stopping.store(false, Ordering::Release);
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "condition did not become true within {timeout:?}"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn valid_prefetch_config() -> BufConfig {
        let tier = MockTier::new((PAGE_SIZE * 128) as u64).expect("valid mock tier");
        BufConfig {
            tiers: vec![Box::new(tier) as Box<dyn TierBackend>],
            ..BufConfig::default()
        }
    }

    #[test]
    fn default_config_matches_previous_constants() {
        let config = BufConfig::default();
        assert_eq!(config.prefetch_workers, PREFETCH_WORKERS);
        assert_eq!(config.max_prefetch_in_flight, MAX_PREFETCH_IN_FLIGHT);
    }

    #[test]
    fn prefetch_config_bounds_are_validated() {
        let mut zero_workers = valid_prefetch_config();
        zero_workers.prefetch_workers = 0;
        assert!(matches!(
            BufferManager::new(zero_workers),
            Err(TierBufError::InvalidConfig(message))
                if message.contains("prefetch_workers must be within 1..=256")
        ));

        let mut too_few_in_flight = valid_prefetch_config();
        too_few_in_flight.prefetch_workers = 4;
        too_few_in_flight.max_prefetch_in_flight = 3;
        assert!(matches!(
            BufferManager::new(too_few_in_flight),
            Err(TierBufError::InvalidConfig(message))
                if message.contains("prefetch_workers..=4096")
        ));

        let mut too_many_in_flight = valid_prefetch_config();
        too_many_in_flight.max_prefetch_in_flight = 4097;
        assert!(matches!(
            BufferManager::new(too_many_in_flight),
            Err(TierBufError::InvalidConfig(message))
                if message.contains("prefetch_workers..=4096")
        ));
    }

    #[test]
    fn many_workers_spawn_and_join() {
        let mut config = valid_prefetch_config();
        config.prefetch_workers = 32;
        let manager = BufferManager::new(config).expect("32 prefetch workers are valid");
        Arc::clone(&manager)
            .shutdown()
            .expect("32 prefetch workers join cleanly");
        drop(manager);
    }

    #[test]
    fn free_checkout_waits_out_stale_fix_and_eviction_claims() {
        let frames = Arc::new(FrameTable::new(PAGE_SIZE).expect("one-frame table"));
        let index = frames.pop_free().expect("initial free frame");
        let frame = frames.frame(index);
        assert!(frame.try_pin(), "simulate a stale fast-path fixer");

        let stale_fix_frames = Arc::clone(&frames);
        let stale_fix = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            stale_fix_frames.frame(index).unpin();
        });
        let checkout = FreeFrame::from_index(&frames, index);
        assert!(frames.frame(index).is_evicting());
        drop(checkout);
        stale_fix.join().expect("stale fixer");

        let index = frames.pop_free().expect("returned free frame");
        assert!(
            frames.frame(index).try_claim_eviction(),
            "simulate a cooling worker that prevalidated the old occupant"
        );
        let stale_cooling_frames = Arc::clone(&frames);
        let stale_cooling = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            stale_cooling_frames.frame(index).release_eviction();
        });
        let checkout = FreeFrame::from_index(&frames, index);
        assert!(frames.frame(index).is_evicting());
        drop(checkout);
        stale_cooling.join().expect("stale cooling worker");

        assert_eq!(frames.pop_free(), Some(index));
    }

    #[test]
    fn allocate_write_and_hot_fix_round_trip() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| {
            page[0] = 0x5a;
            page[PAGE_SIZE - 1] = 0xa5;
        });
        drop(allocated);

        let shared = manager.fix_shared(&swip).expect("hot shared fix");
        assert_eq!(shared.pid(), pid);
        shared.read_with(|page| {
            assert_eq!(page[0], 0x5a);
            assert_eq!(page[PAGE_SIZE - 1], 0xa5);
        });
    }

    #[test]
    fn manual_evict_and_fault_restore_page_and_keep_backing_entry() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[..8].copy_from_slice(b"tierbuf!"));
        drop(allocated);

        manager.manual_evict(&swip).expect("manual eviction");
        assert_eq!(swip.load(), SwipState::Evicted(pid));
        assert_eq!(manager.frames.free_count(), 2);
        assert!(manager.directory.contains_without_lookup(pid));

        let lookups_before = manager.directory_lookup_count();
        let shared = manager.fix_shared(&swip).expect("synchronous fault");
        shared.read_with(|page| assert_eq!(&page[..8], b"tierbuf!"));
        drop(shared);
        assert_eq!(manager.directory_lookup_count(), lookups_before + 1);
        assert!(manager.directory.contains_without_lookup(pid));
    }

    #[test]
    fn resident_hot_path_never_looks_up_directory() {
        let manager = manager(2, 0);
        let allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        drop(allocated);
        let before = manager.directory_lookup_count();

        for _ in 0..100 {
            let shared = manager.fix_shared(&swip).expect("hot shared fix");
            shared.read_with(|page| assert_eq!(page[0], 0));
        }
        let exclusive = manager.fix_exclusive(&swip).expect("hot exclusive fix");
        exclusive.read_with(|page| assert_eq!(page[0], 0));
        drop(exclusive);

        assert_eq!(manager.directory_lookup_count(), before);
    }

    #[test]
    fn independently_constructed_duplicate_handle_is_rejected() {
        let manager = manager(1, 0);
        let allocated = manager.allocate().expect("page allocation");
        let duplicate = Swip::from_pid(allocated.pid());
        drop(allocated);

        let error = match manager.fix_shared(&duplicate) {
            Ok(_) => panic!("duplicate handle must fail"),
            Err(error) => error,
        };
        assert!(matches!(error, TierBufError::DuplicateHandle(_)));
    }

    #[test]
    fn concurrent_same_page_fault_installs_exactly_one_resident_frame() {
        const THREADS: usize = 8;

        let manager = manager(4, 2_000);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x7f);
        drop(allocated);
        manager.manual_evict(&swip).expect("manual eviction");
        let lookups_before = manager.directory_lookup_count();
        let start = Arc::new(Barrier::new(THREADS));

        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let manager = Arc::clone(&manager);
                let swip = swip.clone();
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    let guard = manager.fix_shared(&swip).expect("concurrent fault");
                    guard.read_with(|page| page[0])
                })
            })
            .collect();

        for worker in workers {
            assert_eq!(worker.join().expect("fault worker should finish"), 0x7f);
        }

        let resident_count = (0..manager.frames.frame_count())
            .filter(|&index| manager.frames.frame(index).pid() == pid)
            .count();
        assert_eq!(resident_count, 1, "I5: one pid has one resident frame");
        assert_eq!(manager.frames.free_count(), 3);
        assert_eq!(manager.directory_lookup_count(), lookups_before + 1);
    }

    #[test]
    fn concurrent_fault_waiters_replay_one_error_then_later_call_retries() {
        const THREADS: usize = 8;

        let injection = Arc::new(ReadFailureInjection::default());
        let manager = BufferManager::new(BufConfig {
            dram_pool_bytes: 2 * PAGE_SIZE,
            cooling_ratio: 0.0,
            eviction_mode: EvictionMode::Demand,
            economics: Economics::default(),
            tiers: vec![Box::new(BlockingFailureTier::new(Arc::clone(&injection)))],
            ..BufConfig::default()
        })
        .expect("valid manager");
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0xb7);
        drop(allocated);
        manager.manual_evict(&swip).expect("manual eviction");
        let lookups_before = manager.directory_lookup_count();
        let page_control = manager
            .pages
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pid)
            .cloned()
            .expect("allocated page control");
        let start = Arc::new(Barrier::new(THREADS));

        let workers = (0..THREADS)
            .map(|_| {
                let manager = Arc::clone(&manager);
                let swip = swip.clone();
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    match manager.fix_shared(&swip) {
                        Ok(_) => panic!("injected fault generation must fail"),
                        Err(TierBufError::Io(source)) => (source.kind(), source.to_string()),
                        Err(other) => panic!("expected injected I/O error, got {other:?}"),
                    }
                })
            })
            .collect::<Vec<_>>();

        injection.wait_until_started(Duration::from_secs(1));
        wait_until(Duration::from_secs(1), || {
            page_control
                .fault
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .as_ref()
                .is_some_and(|active| active.waiters == THREADS - 1)
        });
        assert_eq!(
            injection.attempts.load(Ordering::Acquire),
            1,
            "all waiters must share the leader's backing read"
        );
        injection.release_failure();

        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().expect("fault waiter should finish"))
            .collect::<Vec<_>>();
        assert!(
            outcomes.iter().all(|outcome| {
                outcome.0 == io::ErrorKind::TimedOut
                    && outcome.1 == "coalesced injected read failure"
            }),
            "leader and every waiter must receive equivalent errors: {outcomes:?}"
        );
        assert_eq!(injection.attempts.load(Ordering::Acquire), 1);
        assert_eq!(manager.frames.free_count(), 2);
        assert_eq!(manager.directory_lookup_count(), lookups_before + 1);

        let retry = manager
            .fix_shared(&swip)
            .expect("a later independent call may retry");
        retry.read_with(|page| assert_eq!(page[0], 0xb7));
        drop(retry);
        assert_eq!(injection.attempts.load(Ordering::Acquire), 2);
        assert_eq!(manager.directory_lookup_count(), lookups_before + 2);
    }

    #[test]
    fn stable_optimistic_snapshot_validates_successfully() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x31);
        drop(allocated);

        let optimistic = manager.fix_optimistic(&swip).expect("optimistic snapshot");
        assert_eq!(optimistic.pid(), pid);
        assert!(optimistic.swip().shares_state_with(&swip));
        assert_eq!(
            optimistic
                .read_with(|snapshot| snapshot[0])
                .expect("unchanged snapshot must validate"),
            0x31
        );
    }

    #[test]
    fn optimistic_snapshot_retries_after_writer_then_new_snapshot_succeeds() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 1);
        drop(allocated);

        let stale = manager
            .fix_optimistic(&swip)
            .expect("first optimistic snapshot");
        let mut writer = manager.fix_exclusive(&swip).expect("exclusive writer");
        writer.write_with(|page| page[0] = 2);
        drop(writer);

        assert!(matches!(
            stale.read_with(|snapshot| snapshot[0]),
            Err(TierBufError::Retry)
        ));
        let retry = manager
            .fix_optimistic(&swip)
            .expect("retry optimistic snapshot");
        assert_eq!(
            retry
                .read_with(|snapshot| snapshot[0])
                .expect("retry must validate"),
            2
        );
    }

    #[test]
    fn statistics_distinguish_hot_hit_fault_and_completed_tier_io() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x44);
        drop(allocated);

        let resident = manager.fix_shared(&swip).expect("resident hit");
        assert_eq!(resident.source(), FixSource::Dram);
        drop(resident);
        manager.manual_evict(&swip).expect("manual eviction");
        let faulted = manager.fix_shared(&swip).expect("faulted fix");
        assert_eq!(faulted.source(), FixSource::LowerTier);
        drop(faulted);

        let stats = manager.stats();
        assert_eq!(stats.dram_hits, 1);
        assert_eq!(stats.faults, 1);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.tiers.len(), 1);
        assert_eq!(stats.tiers[0].name, "mock");
        assert_eq!(stats.tiers[0].demand_hits, 1);
        assert_eq!(stats.tiers[0].reads, 1);
        assert_eq!(stats.tiers[0].writes, 1);
        assert_eq!(stats.tiers[0].bytes_read, PAGE_SIZE as u64);
        assert_eq!(stats.tiers[0].bytes_written, PAGE_SIZE as u64);
    }

    #[test]
    fn epoch_tick_integrates_dram_and_lower_tier_residency_cost() {
        const HOUR: Duration = Duration::from_secs(60 * 60);

        let manager = manager(2, 0);
        let allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        drop(allocated);

        manager.tick_epoch(HOUR);
        let first = manager.cost_report();
        let expected_gib_seconds =
            PAGE_SIZE as f64 / (1024.0 * 1024.0 * 1024.0) * HOUR.as_secs_f64();
        let dram = first
            .tiers
            .iter()
            .find(|tier| tier.name == "dram")
            .expect("DRAM cost entry");
        assert!((dram.gib_seconds - expected_gib_seconds).abs() < 1.0e-12);
        assert!(first.actual_cost_usd > 0.0);

        manager.manual_evict(&swip).expect("manual eviction");
        manager.tick_epoch(HOUR);
        let second = manager.cost_report();
        let lower = second
            .tiers
            .iter()
            .find(|tier| tier.name == "mock")
            .expect("mock tier cost entry");
        assert!((lower.gib_seconds - expected_gib_seconds).abs() < 1.0e-12);
        assert!(second.all_dram_cost_usd > second.actual_cost_usd);
    }

    #[test]
    fn scan_fault_uses_safe_frame_then_marks_it_cooling() {
        let manager = manager(2, 0);
        let allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        drop(allocated);
        manager.manual_evict(&swip).expect("manual eviction");

        let scan = manager
            .fix_shared_with(&swip, AccessHint::Scan)
            .expect("scan fault");
        scan.read_with(|page| assert_eq!(page[0], 0));
        assert!(matches!(
            swip.load(),
            SwipState::Cooling(_) if scan.pid() == pid
        ));
    }

    #[test]
    fn foreground_pressure_evicts_and_faults_more_pages_than_dram_holds() {
        let manager = manager(2, 0);
        let mut handles = Vec::new();
        for value in 0_u8..8 {
            let mut guard = manager.allocate().expect("pressure allocation");
            guard.write_with(|page| {
                page[0] = value;
                page[PAGE_SIZE - 1] = !value;
            });
            handles.push(guard.swip());
            drop(guard);
        }

        for _ in 0..2 {
            for (value, swip) in (0_u8..8).zip(&handles) {
                let guard = manager.fix_shared(swip).expect("pressure fault");
                guard.read_with(|page| {
                    assert_eq!(page[0], value);
                    assert_eq!(page[PAGE_SIZE - 1], !value);
                });
            }
        }

        let stats = manager.stats();
        assert!(stats.evictions >= 6);
        assert!(stats.faults >= 8);
        assert!(
            manager.frames.free_count() <= manager.low_watermark,
            "pressure may leave at most the configured low-watermark reserve"
        );
    }

    #[test]
    fn resident_working_set_churn_depends_on_eviction_mode() {
        fn run_epochs(mode: EvictionMode) -> u64 {
            let manager = manager_with_mode(10, 0, mode);
            stop_background_workers(&manager);
            for _ in 0..10 {
                drop(manager.allocate().expect("resident working-set page"));
            }

            let before = manager.stats().evictions;
            for _ in 0..10 {
                manager.tick_epoch(Duration::from_millis(1));
                manager.background_cooling_step();
            }
            manager.stats().evictions - before
        }

        assert_eq!(run_epochs(EvictionMode::Demand), 0);
        assert!(run_epochs(EvictionMode::Watermark) > 0);
    }

    #[test]
    fn exhausted_frame_request_notifies_and_activates_cooler() {
        let manager = manager(1, 0);
        stop_background_workers(&manager);
        let pinned = manager.allocate().expect("pinned frame");
        let evictions_before = manager.stats().evictions;
        let before = *manager
            .cooler_signal
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        assert!(matches!(
            manager.allocate(),
            Err(TierBufError::PoolExhausted)
        ));
        let after = *manager
            .cooler_signal
            .generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(after > before);
        drop(pinned);
        manager.background_cooling_step();
        assert_eq!(manager.stats().evictions, evictions_before + 1);
    }

    #[test]
    fn pinned_frame_is_never_selected_for_eviction() {
        let manager = manager(1, 0);
        // A one-frame pool has exactly one cooling ticket. The background
        // cooler competes for that ticket, and `acquire_frame` only samples the
        // queue `windows + 2` times, so a cooler that holds the ticket across
        // all three attempts makes the reclaiming allocate below fail
        // spuriously. Demand eviction alone proves the invariant.
        stop_background_workers(&manager);
        let mut pinned = manager.allocate().expect("pinned allocation");
        let swip = pinned.swip();
        pinned.write_with(|page| page[0] = 0x6d);

        assert!(matches!(
            manager.allocate(),
            Err(TierBufError::PoolExhausted)
        ));
        assert!(matches!(swip.load(), SwipState::Hot(_)));
        pinned.read_with(|page| assert_eq!(page[0], 0x6d));

        drop(pinned);
        let replacement = manager
            .allocate()
            .expect("unpinned frame should become evictable");
        assert!(matches!(swip.load(), SwipState::Evicted(_)));
        drop(replacement);
    }

    #[test]
    fn cooling_revisit_gets_one_second_chance_and_stales_fifo_ticket() {
        let manager = manager(2, 0);
        let allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        drop(allocated);

        let resident = match swip.load() {
            SwipState::Hot(resident) => resident,
            _ => panic!("new page must be hot"),
        };
        let index = manager
            .frames
            .index_of_resident(resident)
            .expect("resident frame");
        swip.try_mark_cooling(resident)
            .expect("exact hot-to-cooling transition");
        manager.enqueue_ticket(index, pid, resident);
        let ticket = manager.cooling.pop().expect("queued cooling ticket");

        let before = manager.stats().second_chances;
        drop(manager.fix_shared(&swip).expect("second-chance fix"));
        assert!(matches!(swip.load(), SwipState::Hot(observed) if observed == resident));
        assert_eq!(manager.stats().second_chances, before + 1);
        assert_eq!(
            manager.evict_ticket(ticket).expect("stale ticket check"),
            EvictionOutcome::Stale
        );
        assert!(matches!(swip.load(), SwipState::Hot(_)));
    }

    #[test]
    fn stale_generation_ticket_is_a_quiet_no_op() {
        let manager = manager(2, 0);
        let allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        drop(allocated);

        let resident = match swip.load() {
            SwipState::Hot(resident) => resident,
            _ => panic!("new page must be hot"),
        };
        let index = manager
            .frames
            .index_of_resident(resident)
            .expect("resident frame");
        let frame = manager.frames.frame(index);
        swip.try_mark_cooling(resident)
            .expect("exact hot-to-cooling transition");
        let stale = CoolingTicket {
            frame_index: index,
            pid,
            generation: frame.generation().wrapping_sub(1),
            resident,
        };
        let free_before = manager.frames.free_count();

        assert_eq!(
            manager
                .evict_ticket(stale)
                .expect("stale generation must not fail"),
            EvictionOutcome::Stale
        );
        assert_eq!(manager.frames.free_count(), free_before);
        assert!(matches!(
            swip.load(),
            SwipState::Cooling(observed) if observed == resident
        ));
        swip.try_resurrect(resident)
            .expect("test cleanup should restore hot state");
    }

    #[test]
    fn zero_budget_warm_tier_falls_through_to_unlimited_cold_tier() {
        let warm = MockTier::with_options(
            "warm",
            (PAGE_SIZE * 4) as u64,
            0.1,
            LatencyProfile::new(10, 10, 1.0),
            Some(WriteBudget::from_daily_allowance(0)),
        )
        .expect("valid warm tier");
        let cold = MockTier::with_options(
            "cold",
            (PAGE_SIZE * 4) as u64,
            0.01,
            LatencyProfile::new(100, 100, 1.0),
            None,
        )
        .expect("valid cold tier");
        let manager = BufferManager::new_with_policy(
            BufConfig {
                dram_pool_bytes: PAGE_SIZE,
                cooling_ratio: 0.1,
                eviction_mode: EvictionMode::Demand,
                economics: Economics::default(),
                tiers: vec![Box::new(warm), Box::new(cold)],
                ..BufConfig::default()
            },
            Arc::new(WarmFirstPolicy),
        )
        .expect("valid manager");

        let mut allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0xc7);
        drop(allocated);
        manager
            .manual_evict(&swip)
            .expect("budget fallback eviction");

        let stats = manager.stats();
        assert_eq!(stats.budget_denied, 1);
        assert_eq!(stats.tiers[0].writes, 0);
        assert_eq!(stats.tiers[1].writes, 1);
        assert_eq!(manager.tiers[0].used_bytes(), 0);
        assert_eq!(manager.tiers[1].used_bytes(), PAGE_SIZE as u64);
        let restored = manager.fix_shared(&swip).expect("cold-tier fault");
        restored.read_with(|page| assert_eq!(page[0], 0xc7));
    }

    #[test]
    fn missing_demotion_target_does_not_write_tier_zero() {
        let tier = MockTier::new((PAGE_SIZE * 4) as u64).expect("valid mock tier");
        let manager = BufferManager::new_with_policy(
            BufConfig {
                dram_pool_bytes: PAGE_SIZE,
                tiers: vec![Box::new(tier)],
                ..BufConfig::default()
            },
            Arc::new(NoTierPolicy),
        )
        .expect("valid manager");
        let allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        drop(allocated);

        let error = manager
            .manual_evict(&swip)
            .expect_err("policy rejection must prevent write-back");

        assert!(
            matches!(
                error,
                TierBufError::TierExhausted { ref tier }
                    if tier == "no eligible lower tier"
            ),
            "unexpected eviction error: {error}"
        );
        assert_eq!(manager.tiers[0].used_bytes(), 0);
    }

    #[test]
    fn shutdown_flushes_dirty_pages_and_rejects_later_work() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[..6].copy_from_slice(b"latest"));
        drop(allocated);

        Arc::clone(&manager).shutdown().expect("clean shutdown");
        assert_eq!(swip.load(), SwipState::Evicted(pid));
        let location = manager
            .directory
            .backing(pid)
            .expect("shutdown must commit backing");
        let mut page = vec![0_u8; PAGE_SIZE];
        manager.tiers[location.tier_index]
            .read(location.offset, &mut page)
            .expect("flushed backing read");
        assert_eq!(&page[..6], b"latest");
        assert!(matches!(
            manager.allocate(),
            Err(TierBufError::ShuttingDown)
        ));
        assert!(matches!(
            manager.fix_shared(&swip),
            Err(TierBufError::ShuttingDown)
        ));
    }

    #[test]
    fn deterministic_multithread_pressure_finishes_without_deadlock() {
        const THREADS: usize = 4;
        const ITERATIONS: usize = 40;

        let manager = manager(4, 0);
        let mut handles = Vec::new();
        for value in 0_u8..8 {
            let mut guard = manager.allocate().expect("initial allocation");
            guard.write_with(|page| page[0] = value);
            handles.push(guard.swip());
            drop(guard);
        }
        let handles = Arc::<[Swip]>::from(handles);
        let start = Arc::new(Barrier::new(THREADS));
        let (done_tx, done_rx) = mpsc::channel();

        let workers = (0..THREADS)
            .map(|worker| {
                let manager = Arc::clone(&manager);
                let handles = Arc::clone(&handles);
                let start = Arc::clone(&start);
                let done_tx = done_tx.clone();
                thread::spawn(move || {
                    start.wait();
                    for iteration in 0..ITERATIONS {
                        let index = (worker * 3 + iteration) % handles.len();
                        loop {
                            match manager.fix_exclusive(&handles[index]) {
                                Ok(mut guard) => {
                                    guard.write_with(|page| {
                                        page[1] = page[1].wrapping_add(1);
                                    });
                                    break;
                                }
                                Err(TierBufError::PoolExhausted | TierBufError::Contended) => {
                                    thread::yield_now();
                                }
                                Err(error) => panic!("unexpected stress error: {error}"),
                            }
                        }
                    }
                    done_tx.send(()).expect("completion receiver");
                })
            })
            .collect::<Vec<_>>();
        drop(done_tx);

        for _ in 0..THREADS {
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("stress worker must not deadlock");
        }
        for worker in workers {
            worker.join().expect("stress worker");
        }
        Arc::clone(&manager).shutdown().expect("stress shutdown");
    }

    #[test]
    fn prefetch_loads_evicted_page_and_next_demand_consumes_hit_marker() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x9a);
        drop(allocated);
        manager.manual_evict(&swip).expect("manual eviction");

        let before = manager.stats();
        manager.prefetch(&[&swip]);
        wait_until(Duration::from_secs(1), || {
            manager.prefetch_in_flight.load(Ordering::Acquire) == 0
                && manager
                    .prefetched
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(&pid)
        });
        assert!(matches!(
            swip.load(),
            SwipState::Hot(_) | SwipState::Cooling(_)
        ));
        let prefetched = manager.stats();
        assert_eq!(prefetched.prefetch_submitted, before.prefetch_submitted + 1);
        assert_eq!(prefetched.faults, before.faults + 1);
        assert_eq!(prefetched.tiers[0].demand_hits, before.tiers[0].demand_hits);

        let guard = manager.fix_shared(&swip).expect("prefetched demand hit");
        guard.read_with(|page| assert_eq!(page[0], 0x9a));
        drop(guard);
        let demanded = manager.stats();
        assert_eq!(demanded.faults, prefetched.faults);
        assert_eq!(demanded.prefetch_hits, prefetched.prefetch_hits + 1);
        assert_eq!(
            demanded.tiers[0].demand_hits,
            prefetched.tiers[0].demand_hits
        );
        assert_eq!(demanded.dram_hits, prefetched.dram_hits + 1);
    }

    #[test]
    fn prefetch_marker_does_not_survive_eviction_and_later_refault() {
        let manager = manager(2, 0);
        let mut allocated = manager.allocate().expect("page allocation");
        let pid = allocated.pid();
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x6c);
        drop(allocated);
        manager.manual_evict(&swip).expect("initial eviction");

        manager.prefetch(&[&swip]);
        wait_until(Duration::from_secs(1), || {
            manager.prefetch_in_flight.load(Ordering::Acquire) == 0
                && manager
                    .prefetched
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(&pid)
        });
        manager
            .manual_evict(&swip)
            .expect("evict prefetched generation");
        assert!(
            !manager
                .prefetched
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&pid)
        );

        let before = manager.stats();
        drop(manager.fix_shared(&swip).expect("ordinary refault"));
        drop(manager.fix_shared(&swip).expect("later resident hit"));
        let after = manager.stats();
        assert_eq!(after.prefetch_hits, before.prefetch_hits);
    }

    #[test]
    fn prefetch_skips_immediately_when_every_frame_is_pinned() {
        let manager = manager(1, 0);
        let victim = manager.allocate().expect("victim allocation");
        let swip = victim.swip();
        drop(victim);
        manager.manual_evict(&swip).expect("victim eviction");
        let pinned = manager.allocate().expect("pinned replacement");

        let before = manager.stats();
        let started = Instant::now();
        manager.prefetch(&[&swip]);
        assert!(started.elapsed() < Duration::from_millis(100));
        let after = manager.stats();
        assert_eq!(after.prefetch_submitted, before.prefetch_submitted);
        assert_eq!(after.prefetch_skipped, before.prefetch_skipped + 1);
        assert_eq!(manager.prefetch_in_flight.load(Ordering::Acquire), 0);
        assert!(matches!(swip.load(), SwipState::Evicted(_)));
        drop(pinned);
    }

    #[test]
    fn failed_prefetch_read_returns_frame_and_later_demand_retries() {
        let tier = MockTier::with_options(
            "failing",
            (PAGE_SIZE * 8) as u64,
            0.0,
            LatencyProfile::default(),
            None,
        )
        .expect("valid mock tier");
        tier.fail_nth_read(Some(1)).expect("valid one-shot failure");
        let manager = BufferManager::new(BufConfig {
            dram_pool_bytes: 2 * PAGE_SIZE,
            cooling_ratio: 0.1,
            eviction_mode: EvictionMode::Demand,
            economics: Economics::default(),
            tiers: vec![Box::new(tier)],
            ..BufConfig::default()
        })
        .expect("valid manager");
        let mut allocated = manager.allocate().expect("page allocation");
        let swip = allocated.swip();
        allocated.write_with(|page| page[0] = 0x4d);
        drop(allocated);
        manager.manual_evict(&swip).expect("manual eviction");

        manager.prefetch(&[&swip]);
        wait_until(Duration::from_secs(1), || {
            manager.prefetch_in_flight.load(Ordering::Acquire) == 0
        });
        assert!(matches!(swip.load(), SwipState::Evicted(_)));
        assert_eq!(manager.frames.free_count(), 2);

        let demand = manager
            .fix_shared(&swip)
            .expect("one-shot failure must not poison demand");
        demand.read_with(|page| assert_eq!(page[0], 0x4d));
    }

    #[test]
    fn oversized_prefetch_batch_never_blocks_and_accepts_at_most_128() {
        const REQUESTS: usize = 140;

        let tier = MockTier::with_options(
            "slow-read",
            (PAGE_SIZE * (REQUESTS + 8)) as u64,
            0.0,
            LatencyProfile::new(50_000, 0, 0.0),
            None,
        )
        .expect("valid mock tier");
        let manager = BufferManager::new(BufConfig {
            dram_pool_bytes: REQUESTS * PAGE_SIZE,
            cooling_ratio: 0.0,
            eviction_mode: EvictionMode::Demand,
            economics: Economics::default(),
            tiers: vec![Box::new(tier)],
            ..BufConfig::default()
        })
        .expect("valid manager");
        let mut handles = Vec::with_capacity(REQUESTS);
        for value in 0..REQUESTS {
            let mut guard = manager.allocate().expect("batch page allocation");
            guard.write_with(|page| page[0] = value as u8);
            handles.push(guard.swip());
            drop(guard);
        }
        for swip in &handles {
            manager.manual_evict(swip).expect("batch page eviction");
        }
        let requests = handles.iter().collect::<Vec<_>>();

        let before = manager.stats();
        let started = Instant::now();
        manager.prefetch(&requests);
        assert!(started.elapsed() < Duration::from_millis(250));
        let after = manager.stats();
        assert_eq!(
            after.prefetch_submitted - before.prefetch_submitted,
            MAX_PREFETCH_IN_FLIGHT as u64
        );
        assert_eq!(
            after.prefetch_skipped - before.prefetch_skipped,
            (REQUESTS - MAX_PREFETCH_IN_FLIGHT) as u64
        );
        assert!(manager.prefetch_in_flight.load(Ordering::Acquire) <= MAX_PREFETCH_IN_FLIGHT);
        Arc::clone(&manager)
            .shutdown()
            .expect("prefetch workers must stop promptly");
    }
}
