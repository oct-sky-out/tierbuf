//! Reviewed unsafe submission helpers for Linux io_uring.

use std::io;
use std::os::fd::RawFd;

use io_uring::{IoUring, opcode, types};

use crate::PAGE_SIZE;

/// Heap-owned page storage whose alignment is suitable for direct I/O.
///
/// Moving the [`Box`] returned by [`Self::boxed_zeroed`] does not move the
/// allocation. An io_uring request may therefore retain its buffer address
/// until the matching completion is observed.
#[repr(C, align(65536))]
pub(crate) struct AlignedPage([u8; PAGE_SIZE]);

impl AlignedPage {
    /// Allocates one zero-filled, page-aligned buffer.
    pub(crate) fn boxed_zeroed() -> Box<Self> {
        Box::new(Self([0; PAGE_SIZE]))
    }

    /// Returns the complete page as an immutable byte slice.
    pub(crate) const fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Pushes one read request into `ring`'s submission queue.
///
/// The caller must submit the ring and wait for the matching completion before
/// it accesses or drops `buffer`.
pub(crate) fn push_read(
    ring: &mut IoUring,
    raw_fd: RawFd,
    offset: u64,
    buffer: &mut AlignedPage,
    user_data: u64,
) -> io::Result<()> {
    let length = u32::try_from(PAGE_SIZE)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "read buffer is too large"))?;
    let entry = opcode::Read::new(types::Fd(raw_fd), buffer.0.as_mut_ptr(), length)
        .offset(offset)
        .build()
        .user_data(user_data);

    // SAFETY: `entry` is copied into the submission queue before this function
    // returns. Its fd and offset are plain values. The only borrowed storage is
    // `buffer`; UringReader moves its owning AlignedPage into the in-flight map
    // and does not access or drop it until this unique `user_data` completes.
    unsafe {
        ring.submission()
            .push(&entry)
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ is full"))
    }
}
