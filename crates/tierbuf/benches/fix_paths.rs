use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use tierbuf::PAGE_SIZE;
use tierbuf::pool::{BufConfig, BufferManager, Economics};
use tierbuf::tier::TierBackend;
use tierbuf::tier::mock::MockTier;

fn hot_fix_paths(criterion: &mut Criterion) {
    let tier = MockTier::new((PAGE_SIZE * 8) as u64).expect("benchmark mock tier");
    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes: PAGE_SIZE * 4,
        cooling_ratio: 0.1,
        economics: Economics::default(),
        tiers: vec![Box::new(tier) as Box<dyn TierBackend>],
    })
    .expect("benchmark buffer manager");
    let mut allocated = manager.allocate().expect("benchmark page");
    allocated.write_with(|page| page[0] = 0x5a);
    let swip = allocated.swip();
    drop(allocated);

    let mut group = criterion.benchmark_group("hot_fix");
    group.bench_function("shared", |bencher| {
        bencher.iter(|| {
            let guard = manager.fix_shared(black_box(&swip)).expect("shared fix");
            black_box(guard.read_with(|page| page[0]))
        });
    });
    group.bench_function("safe_optimistic_snapshot", |bencher| {
        bencher.iter(|| {
            let guard = manager
                .fix_optimistic(black_box(&swip))
                .expect("optimistic snapshot");
            black_box(
                guard
                    .read_with(|page| page[0])
                    .expect("stable optimistic validation"),
            )
        });
    });
    group.finish();
}

criterion_group!(benches, hot_fix_paths);
criterion_main!(benches);
