#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
//! Tier-aware buffer management primitives.
//!
//! `tierbuf` treats DRAM and lower storage tiers as one managed page space.

#[cfg(not(target_pointer_width = "64"))]
compile_error!("tierbuf's tagged resident addresses require a 64-bit target");

use std::io;

use thiserror::Error;

/// The fixed page and frame size used by tierbuf, in bytes.
pub const PAGE_SIZE: usize = 64 * 1024;

/// Generation-checked cooling FIFO primitives.
pub mod cooling;
/// DRAM frame metadata, pinning, and free-frame storage.
pub mod frame;
/// Versioned shared, exclusive, and optimistic latch primitives.
pub mod latch;
/// Runtime statistics and storage-cost accounting.
pub mod metrics;
/// Heat tracking and economic placement policies.
pub mod policy;
/// Buffer-manager assembly, background tiering, prefetch, and page guards.
pub mod pool;
/// Stable, atomically tagged logical page handles.
pub mod swip;
/// Operating-system primitives whose implementations contain isolated unsafe code.
pub mod sys;
/// Storage-tier interfaces and built-in backend implementations.
pub mod tier;
/// Portable io_uring capability wrapper.
pub mod uring;

/// Errors returned by tierbuf operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TierBufError {
    /// An operating-system or storage I/O operation failed.
    #[error("I/O operation failed: {0}")]
    Io(#[from] io::Error),

    /// No free DRAM frame is available.
    #[error("the DRAM pool is exhausted")]
    PoolExhausted,

    /// A lower storage tier has no free page slot.
    #[error("storage tier '{tier}' is exhausted")]
    TierExhausted {
        /// Human-readable backend name.
        tier: String,
    },

    /// A page identifier does not refer to a known page.
    #[error("invalid page identifier: {0}")]
    InvalidPid(u64),

    /// A second, non-canonical handle was supplied for an existing page.
    #[error("page {0} is already registered through a different handle")]
    DuplicateHandle(u64),

    /// A configuration value violates a required invariant.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// The operation could not proceed because of concurrent contention.
    #[error("the operation is contended")]
    Contended,

    /// An optimistic operation observed a concurrent change and should retry.
    #[error("the operation must be retried")]
    Retry,

    /// The buffer manager is shutting down and no longer accepts work.
    #[error("the buffer manager is shutting down")]
    ShuttingDown,
}

/// A result produced by a tierbuf operation.
pub type Result<T> = std::result::Result<T, TierBufError>;

#[cfg(test)]
mod tests {
    #[test]
    fn workspace_smoke_test() {}
}
