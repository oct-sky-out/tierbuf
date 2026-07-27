//! File allocation helpers with portable sparse-file fallback.

use std::fs::File;
use std::io;

/// Reserves physical file space where the host supports `posix_fallocate`.
///
/// Unsupported filesystems retain the size established by `File::set_len` and
/// continue with sparse allocation.
#[cfg(target_os = "linux")]
pub(crate) fn preallocate(file: &File, length: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let length = libc::off_t::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file capacity is too large"))?;
    // SAFETY: `file` owns a valid descriptor for this call and both offsets
    // are non-negative values representable by `off_t`.
    let result = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    match result {
        0 => Ok(()),
        libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL => Ok(()),
        error => Err(io::Error::from_raw_os_error(error)),
    }
}

/// Keeps the `set_len` sparse-file allocation on non-Linux targets.
#[cfg(not(target_os = "linux"))]
pub(crate) fn preallocate(_file: &File, _length: u64) -> io::Result<()> {
    Ok(())
}
