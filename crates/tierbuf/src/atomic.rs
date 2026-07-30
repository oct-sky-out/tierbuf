use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

macro_rules! define_try_update {
    ($name:ident, $atomic:ty, $value:ty) => {
        pub(crate) fn $name<F>(
            atomic: &$atomic,
            set_order: Ordering,
            fetch_order: Ordering,
            mut update: F,
        ) -> Result<$value, $value>
        where
            F: FnMut($value) -> Option<$value>,
        {
            let mut current = atomic.load(fetch_order);
            loop {
                let Some(next) = update(current) else {
                    return Err(current);
                };
                match atomic.compare_exchange_weak(current, next, set_order, fetch_order) {
                    Ok(previous) => return Ok(previous),
                    Err(observed) => current = observed,
                }
            }
        }
    };
}

// `Atomic*::fetch_update` is deprecated on current nightly Rust in favor of
// `try_update`, but the replacement is not available on tierbuf's Rust 1.88
// MSRV. These helpers preserve the operation without suppressing warnings or
// requiring a newer compiler.
define_try_update!(try_update_u64, AtomicU64, u64);
define_try_update!(try_update_usize, AtomicUsize, usize);

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
