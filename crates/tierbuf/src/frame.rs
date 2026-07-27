//! Resident-frame metadata, pinning, and free-frame management.
//!
//! Page bytes live in one `AlignedPool`, while metadata lives in a separate
//! boxed slice whose address never changes. A
//! [`FrameTable`](crate::frame::FrameTable) joins the two by index and uses a
//! bounded lock-free queue for free indices.

use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;

use crate::latch::{ExclusiveRaw, HybridLatch, SharedRaw};
use crate::swip::{PageId, ResidentAddr, Swip};
use crate::sys;
use crate::sys::mmap::AlignedPool;
use crate::{PAGE_SIZE, Result};

const EVICTING: u32 = 1 << 31;
const PIN_COUNT_MASK: u32 = EVICTING - 1;
const EPOCH_SHIFT: u32 = 32;

/// Metadata associated with one resident page-data frame.
///
/// The pin count and eviction reservation share one atomic control word. This
/// makes `try_pin` race-free with eviction: a fixer increments only while the
/// reservation bit is clear, and an evictor can reserve only the exact
/// unpinned value zero.
#[derive(Debug)]
pub struct Frame {
    latch: HybridLatch,
    pid: AtomicU64,
    owner_swip: RwLock<Option<Swip>>,
    dirty: AtomicBool,
    heat_epoch: AtomicU64,
    generation: AtomicU32,
    pin_control: AtomicU32,
}

impl Frame {
    /// Creates unused frame metadata.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            latch: HybridLatch::new(),
            pid: AtomicU64::new(PageId::INVALID.get()),
            owner_swip: RwLock::new(None),
            dirty: AtomicBool::new(false),
            heat_epoch: AtomicU64::new(0),
            generation: AtomicU32::new(0),
            pin_control: AtomicU32::new(0),
        }
    }

    /// Returns this frame's hybrid latch.
    #[must_use]
    pub const fn latch(&self) -> &HybridLatch {
        &self.latch
    }

    /// Returns the currently installed page identifier.
    ///
    /// [`PageId::INVALID`] means that this metadata is on the free list.
    #[must_use]
    pub fn pid(&self) -> PageId {
        let raw = self.pid.load(Ordering::Acquire);
        if raw == PageId::INVALID.get() {
            PageId::INVALID
        } else {
            PageId::new(raw).expect("frame contains an invalid page identifier")
        }
    }

    /// Returns a stable clone of the installed page's owner Swip.
    #[must_use]
    pub fn owner_swip(&self) -> Option<Swip> {
        self.owner_swip
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns whether the resident page contains modifications.
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Sets the dirty flag to `dirty`.
    pub fn set_dirty(&self, dirty: bool) {
        self.dirty.store(dirty, Ordering::Release);
    }

    /// Returns the packed heat placeholder and its epoch as `(heat, epoch)`.
    #[must_use]
    pub fn heat_and_epoch(&self) -> (u32, u32) {
        unpack_heat_epoch(self.heat_epoch.load(Ordering::Acquire))
    }

    /// Replaces the packed heat placeholder and epoch.
    pub fn set_heat_and_epoch(&self, heat: u32, epoch: u32) {
        self.heat_epoch
            .store(pack_heat_epoch(heat, epoch), Ordering::Release);
    }

    /// Atomically replaces `(heat, epoch)` when it still equals `current`.
    ///
    /// The pair occupies one atomic word, so policy updates cannot combine
    /// heat from one access with the epoch observed by another.
    pub(crate) fn compare_exchange_heat_and_epoch(
        &self,
        current: (u32, u32),
        replacement: (u32, u32),
    ) -> std::result::Result<(u32, u32), (u32, u32)> {
        self.heat_epoch
            .compare_exchange_weak(
                pack_heat_epoch(current.0, current.1),
                pack_heat_epoch(replacement.0, replacement.1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(unpack_heat_epoch)
            .map_err(unpack_heat_epoch)
    }

    /// Returns the current occupant generation.
    ///
    /// The generation advances whenever a page is installed, allowing cooling
    /// queue entries to reject stale references after frame reuse.
    #[must_use]
    pub fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    /// Attempts to add one pin unless eviction has reserved the frame.
    ///
    /// Returns `false` when the `EVICTING` reservation is present or the
    /// 31-bit pin count is saturated.
    pub fn try_pin(&self) -> bool {
        let mut current = self.pin_control.load(Ordering::Acquire);
        loop {
            if current & EVICTING != 0 || current == PIN_COUNT_MASK {
                return false;
            }

            match self.pin_control.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    /// Removes one pin.
    ///
    /// # Panics
    ///
    /// Panics when the frame is not pinned or is reserved for eviction.
    pub fn unpin(&self) {
        let mut current = self.pin_control.load(Ordering::Acquire);
        loop {
            assert_eq!(
                current & EVICTING,
                0,
                "cannot unpin a frame reserved for eviction"
            );
            assert!(current > 0, "cannot unpin a frame with zero pins");

            match self.pin_control.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Returns the current number of pins.
    #[must_use]
    pub fn pin_count(&self) -> u32 {
        self.pin_control.load(Ordering::Acquire) & PIN_COUNT_MASK
    }

    /// Returns whether an evictor currently owns the frame reservation.
    #[must_use]
    pub fn is_evicting(&self) -> bool {
        self.pin_control.load(Ordering::Acquire) == EVICTING
    }

    /// Attempts the exact transition from zero pins to `EVICTING`.
    ///
    /// Once this succeeds, new pins fail until [`Self::release_eviction`] is
    /// called. The evictor must still acquire the exclusive latch and
    /// revalidate page ID, generation, dirty state, and Swip state.
    pub fn try_claim_eviction(&self) -> bool {
        self.pin_control
            .compare_exchange(0, EVICTING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Releases an exact `EVICTING` reservation back to zero pins.
    ///
    /// # Panics
    ///
    /// Panics unless this frame is in the exact reserved state.
    pub fn release_eviction(&self) {
        self.pin_control
            .compare_exchange(EVICTING, 0, Ordering::Release, Ordering::Relaxed)
            .expect("only an exact EVICTING reservation can be released");
    }

    /// Converts an exclusive free-frame reservation into the first pin.
    ///
    /// This single atomic transition closes the interval in which a stale
    /// cooling worker could otherwise reserve a frame after it had been
    /// removed from the free queue but before its new occupant was pinned.
    pub(crate) fn publish_reserved_pin(&self) {
        self.pin_control
            .compare_exchange(EVICTING, 1, Ordering::AcqRel, Ordering::Acquire)
            .expect("only a reserved free frame can publish its first pin");
    }

    /// Installs metadata for a newly selected, exclusively reserved occupant.
    #[allow(dead_code)] // Consumed by the staged buffer-manager integration.
    pub(crate) fn install(&self, pid: PageId, owner_swip: Swip) -> u32 {
        assert!(pid.is_valid(), "cannot install PageId::INVALID");
        assert_eq!(
            self.pin_control.load(Ordering::Acquire),
            EVICTING,
            "new occupant requires an exclusively reserved free frame"
        );

        let _exclusive = self.latch.lock_exclusive();
        *self
            .owner_swip
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(owner_swip);
        self.dirty.store(false, Ordering::Relaxed);
        self.heat_epoch.store(0, Ordering::Relaxed);
        let generation = self
            .generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        self.pid.store(pid.get(), Ordering::Release);
        generation
    }

    /// Clears metadata after an evictor has removed the resident Swip.
    #[allow(dead_code)] // Consumed by the staged eviction integration.
    pub(crate) fn clear_after_eviction(&self) {
        assert!(
            self.is_evicting(),
            "metadata can be cleared only under an eviction reservation"
        );

        let _exclusive = self.latch.lock_exclusive();
        self.pid.store(PageId::INVALID.get(), Ordering::Release);
        *self
            .owner_swip
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.dirty.store(false, Ordering::Release);
        self.heat_epoch.store(0, Ordering::Release);
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed frame metadata, page storage, and a bounded free-index queue.
///
/// Metadata resides in a `Box<[Frame]>`, so every [`ResidentAddr`] produced by
/// this table is stable for the table's lifetime. A resident Swip must be
/// unswizzled before its table is dropped.
pub struct FrameTable {
    data: AlignedPool,
    frames: Box<[Frame]>,
    free: ArrayQueue<usize>,
    free_membership: Box<[AtomicBool]>,
}

impl FrameTable {
    /// Creates a frame table backed by an aligned pool of `bytes`.
    ///
    /// # Errors
    ///
    /// Returns the same invalid-size and mapping errors as
    /// [`AlignedPool::new`].
    pub fn new(bytes: usize) -> Result<Self> {
        let data = AlignedPool::new(bytes)?;
        let frame_count = data.frame_count();
        let frames = std::iter::repeat_with(Frame::new)
            .take(frame_count)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free = ArrayQueue::new(frame_count);
        let free_membership = std::iter::repeat_with(|| AtomicBool::new(true))
            .take(frame_count)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        for index in 0..frame_count {
            free.push(index)
                .expect("new free queue must hold every frame exactly once");
        }

        Ok(Self {
            data,
            frames,
            free,
            free_membership,
        })
    }

    /// Returns the number of frames in the table.
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Returns the number of indices currently available on the free list.
    ///
    /// This is a moment-in-time observation and may change immediately under
    /// concurrent use.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free.len()
    }

    /// Pops one free frame index, or returns `None` when the pool is exhausted.
    #[must_use]
    pub fn pop_free(&self) -> Option<usize> {
        let index = self.free.pop()?;
        assert!(
            self.free_membership[index]
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "free queue contained a duplicate frame index"
        );
        Some(index)
    }

    /// Returns one frame index to the free list.
    ///
    /// The caller must return each successfully popped index exactly once.
    ///
    /// # Errors
    ///
    /// Returns the supplied index when it is out of range or when the bounded
    /// queue is already full.
    pub fn push_free(&self, index: usize) -> std::result::Result<(), usize> {
        if index >= self.frame_count() {
            return Err(index);
        }
        if self.free_membership[index]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(index);
        }
        if self.free.push(index).is_err() {
            self.free_membership[index].store(false, Ordering::Release);
            return Err(index);
        }
        Ok(())
    }

    /// Returns metadata for frame `index`.
    ///
    /// # Panics
    ///
    /// Panics when `index` is outside this table.
    #[must_use]
    pub fn frame(&self, index: usize) -> &Frame {
        &self.frames[index]
    }

    /// Reads a frame while holding its shared latch.
    ///
    /// The closure-scoped API prevents a page slice from escaping after the
    /// latch is released.
    pub fn read_with<R>(&self, index: usize, read: impl FnOnce(&[u8; PAGE_SIZE]) -> R) -> R {
        let frame = self.frame(index);
        let shared = frame.latch.lock_shared();
        self.data.read_frame_with(index, &shared, read)
    }

    /// Mutates a frame while holding its exclusive latch.
    ///
    /// The closure-scoped API prevents a mutable page slice from escaping
    /// after the latch is released.
    pub fn write_with<R>(&self, index: usize, write: impl FnOnce(&mut [u8; PAGE_SIZE]) -> R) -> R {
        let frame = self.frame(index);
        let exclusive = frame.latch.lock_exclusive();
        self.data.write_frame_with(index, &exclusive, write)
    }

    /// Returns the stable metadata identity used in resident Swip states.
    #[allow(dead_code)] // Consumed by the staged buffer-manager integration.
    pub(crate) fn resident_addr(&self, index: usize) -> ResidentAddr {
        sys::stable_slice_resident_addr(&self.frames, index)
    }

    /// Resolves a resident identity to its frame index in constant time.
    ///
    /// Identities outside this table, between elements, or beyond its stable
    /// metadata allocation are rejected.
    pub(crate) fn index_of_resident(&self, resident: ResidentAddr) -> Option<usize> {
        let address = resident.exposed_address();
        let base = self.frames.as_ptr().addr();
        let element_size = std::mem::size_of::<Frame>();
        let allocation_size = element_size.checked_mul(self.frames.len())?;
        let end = base.checked_add(allocation_size)?;

        if address < base || address >= end {
            return None;
        }
        let displacement = address - base;
        if !displacement.is_multiple_of(element_size) {
            return None;
        }

        let index = displacement / element_size;
        (self.resident_addr(index) == resident).then_some(index)
    }

    /// Reads frame data using a shared latch guard already held by the caller.
    pub(crate) fn read_latched_with<R>(
        &self,
        index: usize,
        guard: &SharedRaw<'_>,
        read: impl FnOnce(&[u8; PAGE_SIZE]) -> R,
    ) -> R {
        self.data.read_frame_with(index, guard, read)
    }

    /// Accesses frame data mutably using an exclusive latch guard already held
    /// by the caller.
    pub(crate) fn write_latched_with<R>(
        &self,
        index: usize,
        guard: &ExclusiveRaw<'_>,
        write: impl FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    ) -> R {
        self.data.write_frame_with(index, guard, write)
    }
}

const fn pack_heat_epoch(heat: u32, epoch: u32) -> u64 {
    ((epoch as u64) << EPOCH_SHIFT) | heat as u64
}

const fn unpack_heat_epoch(packed: u64) -> (u32, u32) {
    (packed as u32, (packed >> EPOCH_SHIFT) as u32)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{EVICTING, Frame, FrameTable};
    use crate::PAGE_SIZE;
    use crate::swip::{PageId, Swip};

    fn pid(value: u64) -> PageId {
        PageId::new(value).expect("test page id must be valid")
    }

    #[test]
    fn pin_and_eviction_claim_are_one_atomic_protocol() {
        let frame = Frame::new();

        assert!(frame.try_pin());
        assert!(frame.try_pin());
        assert_eq!(frame.pin_count(), 2);
        assert!(!frame.try_claim_eviction());

        frame.unpin();
        frame.unpin();
        assert_eq!(frame.pin_count(), 0);
        assert!(frame.try_claim_eviction());
        assert!(frame.is_evicting());
        assert_eq!(frame.pin_control.load(Ordering::Relaxed), EVICTING);
        assert!(!frame.try_pin());

        frame.release_eviction();
        assert!(!frame.is_evicting());
        assert!(frame.try_pin());
        frame.unpin();
    }

    #[test]
    fn metadata_retains_owner_clone_and_advances_generation() {
        let frame = Frame::new();
        let owner = Swip::from_pid(pid(7));

        assert_eq!(frame.pid(), PageId::INVALID);
        assert!(frame.try_claim_eviction());
        let first_generation = frame.install(pid(7), owner.clone());
        frame.publish_reserved_pin();
        frame.unpin();
        assert_eq!(first_generation, 1);
        assert_eq!(frame.pid(), pid(7));
        assert!(
            frame
                .owner_swip()
                .expect("owner must be installed")
                .shares_state_with(&owner)
        );

        assert!(frame.try_claim_eviction());
        frame.clear_after_eviction();
        frame.release_eviction();
        assert_eq!(frame.pid(), PageId::INVALID);
        assert!(frame.owner_swip().is_none());

        assert!(frame.try_claim_eviction());
        let second_generation = frame.install(pid(8), Swip::from_pid(pid(8)));
        frame.publish_reserved_pin();
        frame.unpin();
        assert_eq!(second_generation, first_generation.wrapping_add(1));
    }

    #[test]
    fn stable_resident_address_and_latched_page_access_round_trip() {
        let table = FrameTable::new(2 * PAGE_SIZE).expect("two-frame table should map");

        assert_eq!(table.resident_addr(0), table.resident_addr(0));
        assert_ne!(table.resident_addr(0), table.resident_addr(1));

        table.write_with(1, |page| {
            page[0] = 0x5a;
            page[PAGE_SIZE - 1] = 0xa5;
        });
        table.read_with(1, |page| {
            assert_eq!(page[0], 0x5a);
            assert_eq!(page[PAGE_SIZE - 1], 0xa5);
        });
    }

    #[test]
    fn free_list_exhausts_and_recovers_every_frame() {
        const FRAMES: usize = 256;
        let table =
            FrameTable::new(FRAMES * PAGE_SIZE).expect("256-frame table should map successfully");

        let first_pass: HashSet<_> = (0..FRAMES)
            .map(|_| table.pop_free().expect("all frames should be available"))
            .collect();
        assert_eq!(first_pass.len(), FRAMES);
        assert_eq!(table.pop_free(), None);
        assert_eq!(table.free_count(), 0);

        for &index in &first_pass {
            table
                .push_free(index)
                .expect("each index should return exactly once");
        }
        assert_eq!(table.free_count(), FRAMES);

        let second_pass: HashSet<_> = (0..FRAMES)
            .map(|_| {
                table
                    .pop_free()
                    .expect("returned frames should be reusable")
            })
            .collect();
        assert_eq!(second_pass, first_pass);
        assert_eq!(table.pop_free(), None);
    }

    #[test]
    fn eight_thread_free_list_stress_preserves_all_indices() {
        const FRAMES: usize = 64;
        const THREADS: usize = 8;
        const ITERATIONS: usize = 100_000;

        let table = Arc::new(FrameTable::new(FRAMES * PAGE_SIZE).expect("stress table should map"));
        let in_use: Arc<[AtomicBool]> = (0..FRAMES)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into();
        let start = Arc::new(Barrier::new(THREADS));

        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                let table = Arc::clone(&table);
                let in_use = Arc::clone(&in_use);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        let index = loop {
                            if let Some(index) = table.pop_free() {
                                break index;
                            }
                            thread::yield_now();
                        };

                        assert!(
                            !in_use[index].swap(true, Ordering::AcqRel),
                            "free list handed out frame {index} twice"
                        );
                        let frame = table.frame(index);
                        assert!(frame.try_pin());
                        frame.unpin();
                        assert!(
                            in_use[index].swap(false, Ordering::AcqRel),
                            "frame {index} was not marked in use"
                        );
                        table
                            .push_free(index)
                            .expect("a checked-out frame must fit back in the queue");
                    }
                })
            })
            .collect();

        for worker in workers {
            worker
                .join()
                .expect("free-list stress worker should finish");
        }

        let remaining: HashSet<_> = (0..FRAMES)
            .map(|_| {
                table
                    .pop_free()
                    .expect("all frame indices must survive stress")
            })
            .collect();
        assert_eq!(remaining.len(), FRAMES);
        assert_eq!(table.pop_free(), None);
        assert!(in_use.iter().all(|flag| !flag.load(Ordering::Acquire)));
    }
}
