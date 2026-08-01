use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub(crate) fn try_update_u64<F>(
    atomic: &AtomicU64,
    set_order: Ordering,
    fetch_order: Ordering,
    update: F,
) -> Result<u64, u64>
where
    F: FnMut(u64) -> Option<u64>,
{
    atomic.try_update(set_order, fetch_order, update)
}

pub(crate) fn try_update_usize<F>(
    atomic: &AtomicUsize,
    set_order: Ordering,
    fetch_order: Ordering,
    update: F,
) -> Result<usize, usize>
where
    F: FnMut(usize) -> Option<usize>,
{
    atomic.try_update(set_order, fetch_order, update)
}

#[cfg(test)]
mod tests {
    use super::{try_update_u64, try_update_usize};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[test]
    fn update_returns_previous_value_and_stores_replacement() {
        let atomic = AtomicU64::new(7);

        assert_eq!(
            try_update_u64(&atomic, Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current + 5)
            }),
            Ok(7)
        );
        assert_eq!(atomic.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn rejected_update_returns_current_value_without_mutating() {
        let atomic = AtomicUsize::new(3);

        assert_eq!(
            try_update_usize(&atomic, Ordering::AcqRel, Ordering::Acquire, |_| None),
            Err(3)
        );
        assert_eq!(atomic.load(Ordering::Relaxed), 3);
    }
}
