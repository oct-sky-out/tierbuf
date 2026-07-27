//! Long-running release stress for eviction, faulting, and prefetch races.

use std::env;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use tierbuf::pool::{BufConfig, BufferManager, Economics};
use tierbuf::swip::Swip;
use tierbuf::tier::TierBackend;
use tierbuf::tier::mock::MockTier;
use tierbuf::{PAGE_SIZE, TierBufError};

const THREADS: usize = 16;
const DEFAULT_SECONDS: u64 = 300;
const DEFAULT_FRAMES: usize = 64;

#[test]
#[ignore = "five-minute release stress; run through scripts/stress.sh"]
fn five_minute_randomized_pressure() {
    let duration = Duration::from_secs(env_value("TIERBUF_STRESS_SECONDS", DEFAULT_SECONDS));
    let frame_count = env_value("TIERBUF_STRESS_FRAMES", DEFAULT_FRAMES);
    assert!(frame_count > 0);
    let page_count = frame_count.saturating_mul(2).max(2);
    let tier = MockTier::new((page_count * PAGE_SIZE * 4) as u64).expect("stress tier");
    let manager = BufferManager::new(BufConfig {
        dram_pool_bytes: frame_count * PAGE_SIZE,
        cooling_ratio: 0.1,
        economics: Economics {
            dram_price_gb_month: 4.5,
            epoch: Duration::from_millis(25),
        },
        tiers: vec![Box::new(tier) as Box<dyn TierBackend>],
    })
    .expect("stress manager");

    let mut swips = Vec::with_capacity(page_count);
    for index in 0..page_count {
        let mut guard = retry_until(Instant::now() + Duration::from_secs(5), || {
            manager.allocate()
        })
        .expect("stress initialization must make progress");
        guard.write_with(|page| {
            page[0] = marker(index);
            page[1] = 0;
        });
        swips.push(guard.swip());
    }
    let swips: Arc<[Swip]> = swips.into();
    let start = Arc::new(Barrier::new(THREADS + 1));
    let deadline = Instant::now() + duration;
    let workers = (0..THREADS)
        .map(|worker| {
            let manager = Arc::clone(&manager);
            let swips = Arc::clone(&swips);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                let mut random = XorShift64::new(0x9e37_79b9_7f4a_7c15 ^ (worker as u64 + 1));
                start.wait();
                while Instant::now() < deadline {
                    let index = random.next() as usize % swips.len();
                    if random.next().is_multiple_of(5) {
                        let mut guard =
                            retry_until(Instant::now() + Duration::from_secs(5), || {
                                manager.fix_exclusive(&swips[index])
                            })
                            .expect("exclusive stress fix");
                        guard.write_with(|page| {
                            assert_eq!(page[0], marker(index));
                            page[1] = page[1].wrapping_add(1);
                        });
                    } else {
                        let guard = retry_until(Instant::now() + Duration::from_secs(5), || {
                            manager.fix_shared(&swips[index])
                        })
                        .expect("shared stress fix");
                        guard.read_with(|page| assert_eq!(page[0], marker(index)));
                    }

                    if random.next().is_multiple_of(32) {
                        let next = (index + 1) % swips.len();
                        manager.prefetch(&[&swips[next]]);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    for worker in workers {
        worker.join().expect("stress worker must not panic");
    }
    for (index, swip) in swips.iter().enumerate() {
        let guard = retry_until(Instant::now() + Duration::from_secs(5), || {
            manager.fix_shared(swip)
        })
        .expect("final stress verification");
        guard.read_with(|page| assert_eq!(page[0], marker(index)));
    }
    Arc::clone(&manager)
        .shutdown()
        .expect("stress manager must shut down");
}

fn retry_until<T>(
    deadline: Instant,
    mut operation: impl FnMut() -> tierbuf::Result<T>,
) -> tierbuf::Result<T> {
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(TierBufError::PoolExhausted | TierBufError::Contended | TierBufError::Retry)
                if Instant::now() < deadline =>
            {
                thread::yield_now()
            }
            Err(error) => return Err(error),
        }
    }
}

fn env_value<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn marker(index: usize) -> u8 {
    ((index.wrapping_mul(131).wrapping_add(17)) % 251 + 1) as u8
}

#[derive(Clone, Copy)]
struct XorShift64(u64);

impl XorShift64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}
