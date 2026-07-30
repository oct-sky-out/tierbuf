//! Page-aligned anonymous memory mappings.

#[cfg(miri)]
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::io;
#[cfg(not(miri))]
use std::ptr;
#[cfg(miri)]
use std::ptr::NonNull;

use crate::latch::{ExclusiveRaw, SharedRaw};
use crate::{PAGE_SIZE, Result, TierBufError};

/// An anonymous memory mapping divided into fixed, 64-KiB-aligned frames.
///
/// The pool owns one private anonymous mapping. It over-allocates by at most one
/// frame minus one byte, then chooses a 64-KiB boundary inside that mapping as
/// the first frame. Dropping the pool releases the complete original mapping.
#[derive(Debug)]
pub struct AlignedPool {
    #[cfg(not(miri))]
    mapping: *mut libc::c_void,
    #[cfg(not(miri))]
    mapping_len: usize,
    #[cfg(miri)]
    allocation: NonNull<u8>,
    #[cfg(miri)]
    allocation_layout: Layout,
    aligned_start: *mut u8,
    frame_count: usize,
}

// SAFETY: Moving an AlignedPool transfers sole ownership of the mapping
// without changing its address. Safe APIs do not dereference its raw pointers.
unsafe impl Send for AlignedPool {}

// SAFETY: Shared access only exposes inert addresses and frame counts publicly.
// Crate-internal reference creation requires the corresponding latch token, so
// synchronized FrameTable operations may share a pool between threads.
unsafe impl Sync for AlignedPool {}

impl AlignedPool {
    /// Creates a pool containing `bytes / PAGE_SIZE` frames.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with an invalid-input source when `bytes`
    /// is zero, is not a multiple of [`PAGE_SIZE`], or overflows while adding
    /// alignment padding. It also returns [`TierBufError::Io`] when `mmap`
    /// fails.
    pub fn new(bytes: usize) -> Result<Self> {
        if bytes == 0 {
            return Err(invalid_size("pool size must be greater than zero"));
        }
        if !bytes.is_multiple_of(PAGE_SIZE) {
            return Err(invalid_size(
                "pool size must be an exact multiple of PAGE_SIZE",
            ));
        }

        #[cfg(miri)]
        {
            let allocation_layout = Layout::from_size_align(bytes, PAGE_SIZE)
                .map_err(|_| invalid_size("pool allocation layout is invalid"))?;
            // SAFETY: `allocation_layout` has non-zero size and a supported
            // power-of-two alignment. Ownership is retained until Drop.
            let allocation = unsafe { alloc_zeroed(allocation_layout) };
            let allocation = NonNull::new(allocation)
                .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
            debug_assert_eq!(allocation.as_ptr().addr() % PAGE_SIZE, 0);
            return Ok(Self {
                allocation,
                allocation_layout,
                aligned_start: allocation.as_ptr(),
                frame_count: bytes / PAGE_SIZE,
            });
        }

        #[cfg(not(miri))]
        {
            let mapping_len = bytes
                .checked_add(PAGE_SIZE - 1)
                .ok_or_else(|| invalid_size("pool size overflows with alignment padding"))?;

            // SAFETY: The arguments request a new private anonymous mapping.
            // The returned pointer is checked against MAP_FAILED before use.
            let mapping = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    mapping_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            if ptr::eq(mapping, libc::MAP_FAILED) {
                return Err(io::Error::last_os_error().into());
            }

            let base_address = mapping.cast::<u8>().addr();
            let alignment_offset = (PAGE_SIZE - (base_address % PAGE_SIZE)) % PAGE_SIZE;
            let aligned_start = mapping.cast::<u8>().wrapping_add(alignment_offset);

            debug_assert!(alignment_offset + bytes <= mapping_len);
            debug_assert_eq!(aligned_start.addr() % PAGE_SIZE, 0);

            Ok(Self {
                mapping,
                mapping_len,
                aligned_start,
                frame_count: bytes / PAGE_SIZE,
            })
        }
    }

    /// Returns a pointer to the first byte of frame `index`.
    ///
    /// The returned pointer is aligned to [`PAGE_SIZE`] and remains valid until
    /// this pool is dropped. Reading or writing through the pointer is unsafe;
    /// callers must prevent out-of-bounds access, use-after-free, and aliased
    /// mutable access.
    ///
    /// # Panics
    ///
    /// Panics when `index` is greater than or equal to [`Self::frame_count`].
    #[must_use]
    pub fn frame_ptr(&self, index: usize) -> *mut u8 {
        assert!(
            index < self.frame_count,
            "frame index {index} is out of bounds for a {}-frame pool",
            self.frame_count
        );

        self.aligned_start.wrapping_add(index * PAGE_SIZE)
    }

    /// Returns the number of fixed-size frames in the pool.
    #[must_use]
    pub const fn frame_count(&self) -> usize {
        self.frame_count
    }

    /// Runs `read` with a shared reference to one complete frame.
    ///
    /// `guard` is a witness that the owning FrameTable holds that frame's
    /// shared latch for the duration of the closure.
    pub(crate) fn read_frame_with<R>(
        &self,
        index: usize,
        _guard: &SharedRaw<'_>,
        read: impl FnOnce(&[u8; PAGE_SIZE]) -> R,
    ) -> R {
        let frame = self.frame_ptr(index).cast::<[u8; PAGE_SIZE]>();

        // SAFETY: `frame_ptr` bounds-checks `index` and returns a pointer to a
        // full PAGE_SIZE-byte frame. The shared latch witness prevents a
        // concurrent exclusive reference, and the closure cannot return a
        // reference whose lifetime depends on this temporary borrow.
        unsafe { read(&*frame) }
    }

    /// Runs `write` with an exclusive reference to one complete frame.
    ///
    /// `guard` is a witness that the owning FrameTable holds that frame's
    /// exclusive latch for the duration of the closure.
    pub(crate) fn write_frame_with<R>(
        &self,
        index: usize,
        _guard: &ExclusiveRaw<'_>,
        write: impl FnOnce(&mut [u8; PAGE_SIZE]) -> R,
    ) -> R {
        let frame = self.frame_ptr(index).cast::<[u8; PAGE_SIZE]>();

        // SAFETY: `frame_ptr` bounds-checks `index` and returns a pointer to a
        // full PAGE_SIZE-byte frame. The exclusive latch witness prevents all
        // concurrent shared or mutable references for the closure's duration.
        unsafe { write(&mut *frame) }
    }
}

impl Drop for AlignedPool {
    fn drop(&mut self) {
        #[cfg(miri)]
        {
            // SAFETY: `allocation` was returned for `allocation_layout` and
            // remains uniquely owned by this pool until this one Drop call.
            unsafe {
                dealloc(self.allocation.as_ptr(), self.allocation_layout);
            }
        }

        #[cfg(not(miri))]
        {
            // SAFETY: `mapping` and `mapping_len` are the unchanged address and
            // length returned to this owner by a successful `mmap`, and Drop runs
            // at most once for the mapping.
            let result = unsafe { libc::munmap(self.mapping, self.mapping_len) };
            debug_assert_eq!(result, 0, "munmap failed: {}", io::Error::last_os_error());
        }
    }
}

fn invalid_size(message: &'static str) -> TierBufError {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

#[cfg(test)]
mod tests {
    use super::AlignedPool;
    use crate::PAGE_SIZE;

    #[cfg(miri)]
    const TEST_FRAMES: usize = 4;
    #[cfg(not(miri))]
    const TEST_FRAMES: usize = 256;
    const TEST_POOL_BYTES: usize = TEST_FRAMES * PAGE_SIZE;

    #[test]
    fn every_frame_is_page_aligned() {
        let pool = AlignedPool::new(TEST_POOL_BYTES).expect("16 MiB pool should map");

        assert_eq!(pool.frame_count(), TEST_FRAMES);
        for index in 0..pool.frame_count() {
            assert_eq!(pool.frame_ptr(index).addr() % PAGE_SIZE, 0);
        }
    }

    #[test]
    fn first_and_last_byte_of_each_frame_round_trip() {
        let pool = AlignedPool::new(TEST_POOL_BYTES).expect("16 MiB pool should map");

        for index in 0..pool.frame_count() {
            let first = pool.frame_ptr(index);
            let last = first.wrapping_add(PAGE_SIZE - 1);
            let first_value = index as u8;
            let last_value = !first_value;

            // SAFETY: `first` and `last` are within the distinct frame selected
            // by `index`; the pool remains alive and no other access occurs
            // while these bytes are written and read.
            unsafe {
                first.write(first_value);
                last.write(last_value);
                assert_eq!(first.read(), first_value);
                assert_eq!(last.read(), last_value);
            }
        }
    }

    #[test]
    fn invalid_pool_sizes_are_rejected() {
        assert!(AlignedPool::new(0).is_err());
        assert!(AlignedPool::new(PAGE_SIZE - 1).is_err());
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn out_of_range_frame_panics() {
        let pool = AlignedPool::new(PAGE_SIZE).expect("one-frame pool should map");
        let _ = pool.frame_ptr(1);
    }
}
