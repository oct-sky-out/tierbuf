//! Model-based invariants for eviction, faulting, and guarded page access.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use proptest::collection;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestCaseError, TestCaseResult};

use tierbuf::pool::{
    BufConfig, BufferManager, Economics, EvictionMode, ExclusiveGuard, SharedGuard,
};
use tierbuf::swip::{Swip, SwipState};
use tierbuf::tier::TierBackend;
use tierbuf::tier::mock::MockTier;
use tierbuf::{PAGE_SIZE, TierBufError};

const FRAME_COUNT: usize = 3;
const TIER_PAGE_CAPACITY: usize = 128;
const MODEL_PAGE_CAP: usize = 32;
const OP_COUNT: usize = 200;
const MAX_RETRIES: usize = 16;
const PRESSURE_SWEEP: usize = FRAME_COUNT + 1;

#[derive(Clone, Debug)]
enum Op {
    Allocate { seed: u64 },
    SharedRead { index: u16 },
    ExclusiveWrite { index: u16, seed: u64 },
    OptimisticSnapshot { index: u16 },
    EpochTick,
    PressureSweep { start: u16, seed: u64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => any::<u64>().prop_map(|seed| Op::Allocate { seed }),
        4 => any::<u16>().prop_map(|index| Op::SharedRead { index }),
        3 => (any::<u16>(), any::<u64>())
            .prop_map(|(index, seed)| Op::ExclusiveWrite { index, seed }),
        2 => any::<u16>().prop_map(|index| Op::OptimisticSnapshot { index }),
        1 => Just(Op::EpochTick),
        1 => (any::<u16>(), any::<u64>())
            .prop_map(|(start, seed)| Op::PressureSweep { start, seed }),
    ]
}

fn operation_sequence() -> impl Strategy<Value = Vec<Op>> {
    collection::vec(op_strategy(), OP_COUNT)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        max_shrink_iters: 4_096,
        fork: false,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn buffer_manager_matches_reference_model(operations in operation_sequence()) {
        prop_assert_eq!(operations.len(), OP_COUNT);
        run_model(&operations)?;
    }
}

#[test]
fn dirty_eviction_and_fault_regression() {
    let manager = test_manager();
    let mut model = Vec::new();

    for seed in 0..8 {
        allocate_page(&manager, &mut model, seed).expect("pressure allocation must progress");
    }

    let evicted_index = model
        .iter()
        .position(|(swip, _)| matches!(swip.load(), SwipState::Evicted(_)))
        .expect("eight dirty pages in three frames must force an eviction");
    let evicted_swip = model[evicted_index].0.clone();
    let expected = model[evicted_index].1;
    let stats_before = manager.stats();
    assert!(stats_before.evictions > 0);
    assert!(stats_before.tiers[0].writes > 0);

    let guard = retry_shared(&manager, &evicted_swip).expect("evicted page must fault back in");
    let actual = guard.read_with(first_sixteen_page);
    drop(guard);

    assert_eq!(actual, expected);
    let stats_after = manager.stats();
    assert!(stats_after.evictions >= stats_before.evictions);
    assert_eq!(stats_after.faults, stats_before.faults + 1);
    assert_eq!(stats_after.tiers[0].reads, stats_before.tiers[0].reads + 1);
    Arc::clone(&manager)
        .shutdown()
        .expect("regression manager must shut down cleanly");
}

fn run_model(operations: &[Op]) -> TestCaseResult {
    let manager = test_manager();
    let mut model: Vec<(Swip, [u8; 16])> = Vec::with_capacity(MODEL_PAGE_CAP);

    for operation in operations {
        match *operation {
            Op::Allocate { seed } => {
                if model.len() < MODEL_PAGE_CAP {
                    allocate_page(&manager, &mut model, seed).map_err(test_failure)?;
                }
            }
            Op::SharedRead { index } => {
                if !model.is_empty() {
                    check_shared(&manager, &model, usize::from(index) % model.len())
                        .map_err(test_failure)?;
                }
            }
            Op::ExclusiveWrite { index, seed } => {
                if !model.is_empty() {
                    let model_index = usize::from(index) % model.len();
                    write_page(&manager, &mut model, model_index, pattern(seed))
                        .map_err(test_failure)?;
                }
            }
            Op::OptimisticSnapshot { index } => {
                if !model.is_empty() {
                    check_optimistic(&manager, &model, usize::from(index) % model.len())
                        .map_err(test_failure)?;
                }
            }
            Op::EpochTick => {
                thread::sleep(Duration::from_micros(100));
            }
            Op::PressureSweep { start, seed } => {
                if model.len() < MODEL_PAGE_CAP {
                    allocate_page(&manager, &mut model, seed).map_err(test_failure)?;
                }
                read_sweep(&manager, &model, usize::from(start)).map_err(test_failure)?;
            }
        }
    }

    for index in 0..model.len() {
        check_shared(&manager, &model, index).map_err(test_failure)?;
    }
    Arc::clone(&manager)
        .shutdown()
        .map_err(|error| test_failure(format!("shutdown failed: {error}")))?;
    Ok(())
}

fn test_manager() -> Arc<BufferManager> {
    let tier = MockTier::new((TIER_PAGE_CAPACITY * PAGE_SIZE) as u64)
        .expect("model-test MockTier configuration is valid");
    BufferManager::new(BufConfig {
        dram_pool_bytes: FRAME_COUNT * PAGE_SIZE,
        cooling_ratio: 0.34,
        eviction_mode: EvictionMode::Demand,
        economics: Economics {
            dram_price_gb_month: 4.5,
            epoch: Duration::from_micros(50),
        },
        tiers: vec![Box::new(tier) as Box<dyn TierBackend>],
        ..BufConfig::default()
    })
    .expect("model-test BufferManager configuration is valid")
}

fn allocate_page(
    manager: &BufferManager,
    model: &mut Vec<(Swip, [u8; 16])>,
    seed: u64,
) -> Result<(), String> {
    let expected = pattern(seed);
    let mut guard =
        retry_allocate(manager).map_err(|error| format!("allocation made no progress: {error}"))?;
    let swip = guard.swip();
    guard.write_with(|page| page[..16].copy_from_slice(&expected));
    drop(guard);
    model.push((swip, expected));
    Ok(())
}

fn check_shared(
    manager: &BufferManager,
    model: &[(Swip, [u8; 16])],
    index: usize,
) -> Result<(), String> {
    let (swip, expected) = &model[index];
    let guard = retry_shared(manager, swip)
        .map_err(|error| format!("shared read of model page {index} failed: {error}"))?;
    let actual = guard.read_with(first_sixteen_page);
    drop(guard);
    if actual == *expected {
        Ok(())
    } else {
        Err(format!(
            "shared read mismatch at model page {index}: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn write_page(
    manager: &BufferManager,
    model: &mut [(Swip, [u8; 16])],
    index: usize,
    replacement: [u8; 16],
) -> Result<(), String> {
    let swip = model[index].0.clone();
    let mut guard = retry_exclusive(manager, &swip)
        .map_err(|error| format!("exclusive write of model page {index} failed: {error}"))?;
    guard.write_with(|page| page[..16].copy_from_slice(&replacement));
    drop(guard);
    model[index].1 = replacement;
    Ok(())
}

fn check_optimistic(
    manager: &BufferManager,
    model: &[(Swip, [u8; 16])],
    index: usize,
) -> Result<(), String> {
    let (swip, expected) = &model[index];
    let mut last_error = None;
    for _ in 0..MAX_RETRIES {
        let guard = match manager.fix_optimistic(swip) {
            Ok(guard) => guard,
            Err(error) if is_transient(&error) => {
                last_error = Some(error);
                thread::yield_now();
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "optimistic fix of model page {index} failed: {error}"
                ));
            }
        };

        match guard.read_with(first_sixteen) {
            Ok(actual) if actual == *expected => return Ok(()),
            Ok(actual) => {
                return Err(format!(
                    "optimistic read mismatch at model page {index}: expected {expected:?}, \
                     got {actual:?}"
                ));
            }
            Err(error) if is_transient(&error) => {
                last_error = Some(error);
                thread::yield_now();
            }
            Err(error) => {
                return Err(format!(
                    "optimistic validation of model page {index} failed: {error}"
                ));
            }
        }
    }

    Err(format!(
        "optimistic read of model page {index} made no progress after {MAX_RETRIES} retries: {}",
        last_error.map_or_else(
            || "unknown contention".to_owned(),
            |error| error.to_string()
        )
    ))
}

fn read_sweep(
    manager: &BufferManager,
    model: &[(Swip, [u8; 16])],
    start: usize,
) -> Result<(), String> {
    if model.is_empty() {
        return Ok(());
    }

    let sweep_len = model.len().min(PRESSURE_SWEEP);
    for offset in 0..sweep_len {
        check_shared(manager, model, (start + offset) % model.len())?;
    }
    Ok(())
}

fn retry_allocate(manager: &BufferManager) -> Result<ExclusiveGuard<'_>, TierBufError> {
    let mut last_error = None;
    for _ in 0..MAX_RETRIES {
        match manager.allocate() {
            Ok(guard) => return Ok(guard),
            Err(error) if is_transient(&error) => {
                last_error = Some(error);
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or(TierBufError::PoolExhausted))
}

fn retry_shared<'a>(
    manager: &'a BufferManager,
    swip: &Swip,
) -> Result<SharedGuard<'a>, TierBufError> {
    let mut last_error = None;
    for _ in 0..MAX_RETRIES {
        match manager.fix_shared(swip) {
            Ok(guard) => return Ok(guard),
            Err(error) if is_transient(&error) => {
                last_error = Some(error);
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or(TierBufError::Contended))
}

fn retry_exclusive<'a>(
    manager: &'a BufferManager,
    swip: &Swip,
) -> Result<ExclusiveGuard<'a>, TierBufError> {
    let mut last_error = None;
    for _ in 0..MAX_RETRIES {
        match manager.fix_exclusive(swip) {
            Ok(guard) => return Ok(guard),
            Err(error) if is_transient(&error) => {
                last_error = Some(error);
                thread::yield_now();
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or(TierBufError::Contended))
}

fn is_transient(error: &TierBufError) -> bool {
    matches!(
        error,
        TierBufError::PoolExhausted | TierBufError::Contended | TierBufError::Retry
    )
}

fn first_sixteen(page: &[u8]) -> [u8; 16] {
    page[..16]
        .try_into()
        .expect("every tierbuf page contains sixteen bytes")
}

fn first_sixteen_page(page: &[u8; PAGE_SIZE]) -> [u8; 16] {
    first_sixteen(page)
}

fn pattern(seed: u64) -> [u8; 16] {
    std::array::from_fn(|index| {
        let rotated = seed.rotate_left((index * 7) as u32);
        (rotated as u8)
            .wrapping_add((index as u8).wrapping_mul(29))
            .rotate_left((index % 8) as u32)
    })
}

fn test_failure(message: impl Into<String>) -> TestCaseError {
    TestCaseError::fail(message.into())
}
