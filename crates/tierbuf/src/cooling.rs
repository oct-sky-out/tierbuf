//! Generation-checked cooling queue primitives.
//!
//! The queue deliberately stores more than a frame index. A frame can leave
//! DRAM and be reused while an old FIFO entry is still pending, so eviction
//! must also match the logical page, metadata generation, and exact resident
//! address before acting on a ticket.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::swip::{PageId, ResidentAddr};

/// One exact cooling candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoolingTicket {
    pub(crate) frame_index: usize,
    pub(crate) pid: PageId,
    pub(crate) generation: u32,
    pub(crate) resident: ResidentAddr,
}

/// A synchronized FIFO of cooling candidates.
#[derive(Debug, Default)]
pub(crate) struct CoolingQueue {
    entries: Mutex<VecDeque<CoolingTicket>>,
}

impl CoolingQueue {
    #[cfg(test)]
    pub(crate) fn push(&self, ticket: CoolingTicket) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(ticket);
    }

    pub(crate) fn push_unique(&self, ticket: CoolingTicket) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !entries.contains(&ticket) {
            entries.push_back(ticket);
        }
    }

    pub(crate) fn pop(&self) -> Option<CoolingTicket> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{CoolingQueue, CoolingTicket};
    use crate::swip::{PageId, ResidentAddr};

    fn ticket(index: usize) -> CoolingTicket {
        CoolingTicket {
            frame_index: index,
            pid: PageId::new(index as u64).expect("small test pid"),
            generation: index as u32,
            // Tests never install or dereference these synthetic opaque
            // identities; they only verify FIFO preservation.
            resident: ResidentAddr::from_test_address((index as u64 + 1) << 2),
        }
    }

    #[test]
    fn preserves_fifo_order_and_full_identity() {
        let queue = CoolingQueue::default();
        queue.push(ticket(1));
        queue.push(ticket(2));

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop(), Some(ticket(1)));
        assert_eq!(queue.pop(), Some(ticket(2)));
        assert!(queue.is_empty());
    }

    #[test]
    fn concurrent_producers_do_not_lose_entries() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 1_000;

        let queue = Arc::new(CoolingQueue::default());
        let workers: Vec<_> = (0..THREADS)
            .map(|worker| {
                let queue = Arc::clone(&queue);
                thread::spawn(move || {
                    for offset in 0..PER_THREAD {
                        queue.push(ticket(worker * PER_THREAD + offset));
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("producer should finish");
        }
        assert_eq!(queue.len(), THREADS * PER_THREAD);

        let mut seen = vec![false; THREADS * PER_THREAD];
        while let Some(entry) = queue.pop() {
            assert!(!seen[entry.frame_index]);
            seen[entry.frame_index] = true;
        }
        assert!(seen.into_iter().all(|present| present));
    }
}
