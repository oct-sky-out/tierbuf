//! Low-level operating-system facilities.
//!
//! Unsafe code is confined to this module tree so that the rest of tierbuf can
//! build on small, reviewed system abstractions.

use std::ptr::NonNull;

use crate::swip::ResidentAddr;

pub(crate) mod file;
pub mod mmap;
#[cfg(all(target_os = "linux", feature = "uring"))]
pub(crate) mod uring;

/// Captures the address of an element in a boxed slice whose allocation is
/// retained by its owner.
///
/// The returned token is opaque to safe code. The owner must remove the token
/// from every Swip before dropping the boxed slice.
#[allow(dead_code)] // Consumed by the staged frame/pool integration.
pub(crate) fn stable_slice_resident_addr<T>(values: &[T], index: usize) -> ResidentAddr {
    let pointer = NonNull::from(&values[index]);

    // SAFETY: A Box keeps its slice allocation fixed until it is dropped.
    // FrameTable owns `values` for every interval in which it permits this
    // token to be installed in a Swip.
    unsafe { ResidentAddr::from_non_null(pointer) }
}
