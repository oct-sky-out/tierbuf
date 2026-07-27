//! Optional io_uring read engine with a synchronous caller-facing API.
//!
//! Callers submit fixed-page reads to one dedicated ring thread and wait on a
//! per-request completion channel. The thread can keep many requests in
//! flight concurrently, while each request owns the aligned kernel buffer for
//! its entire submission-to-completion lifetime.

use std::io;
use std::os::fd::RawFd;

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::collections::{HashMap, VecDeque};

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::ops::{Deref, DerefMut};

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::sync::{Mutex, RwLock};

#[cfg(all(target_os = "linux", feature = "uring"))]
use std::thread::{self, JoinHandle};

#[cfg(all(target_os = "linux", feature = "uring"))]
use io_uring::IoUring;

use crate::tier::TierOffset;
use crate::{PAGE_SIZE, Result, TierBufError};

#[cfg(all(target_os = "linux", feature = "uring"))]
use crate::sys::uring::AlignedPage;

/// A concurrent io_uring reader for fixed-size tier pages.
///
/// Native construction is available only on Linux with the `uring` Cargo
/// feature. Other builds retain this API but return
/// [`io::ErrorKind::Unsupported`], allowing portable embedders to select a
/// synchronous fallback.
pub struct UringReader {
    #[cfg(all(target_os = "linux", feature = "uring"))]
    engine: NativeEngine,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
struct NativeEngine {
    sender: SyncSender<Command>,
    stopping: AtomicBool,
    submission_gate: RwLock<()>,
    shutdown_lock: Mutex<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
enum Command {
    Read(ReadRequest),
    Shutdown,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
struct ReadRequest {
    raw_fd: RawFd,
    location: TierOffset,
    page: Box<AlignedPage>,
    completion: SyncSender<io::Result<Box<AlignedPage>>>,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
#[derive(Default)]
struct InflightRequests(HashMap<u64, ReadRequest>);

#[cfg(all(target_os = "linux", feature = "uring"))]
#[derive(Clone, Debug)]
struct SharedIoError {
    kind: io::ErrorKind,
    message: String,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
impl Deref for InflightRequests {
    type Target = HashMap<u64, ReadRequest>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
impl DerefMut for InflightRequests {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
impl Drop for InflightRequests {
    fn drop(&mut self) {
        // A panic can bypass the ordinary CQE/fatal-error paths. Closing an
        // io_uring fd initiates cancellation but is not documented as a
        // synchronous user-buffer lifetime barrier, so conservatively leak
        // only buffers whose completion was never observed.
        for (_, request) in self.0.drain() {
            leak_kernel_visible_page(request);
        }
    }
}

impl UringReader {
    /// Creates a ring with `entries` submission and completion slots.
    ///
    /// # Errors
    ///
    /// Returns [`TierBufError::Io`] with [`io::ErrorKind::InvalidInput`] when
    /// `entries` is zero. On builds other than Linux plus feature `uring`, it
    /// returns [`io::ErrorKind::Unsupported`]. Native ring setup errors are
    /// propagated unchanged.
    pub fn new(entries: u32) -> Result<Self> {
        if entries == 0 {
            return Err(invalid_input("io_uring entries must be greater than zero"));
        }

        #[cfg(all(target_os = "linux", feature = "uring"))]
        {
            Ok(Self {
                engine: NativeEngine::new(entries)?,
            })
        }

        #[cfg(not(all(target_os = "linux", feature = "uring")))]
        {
            let _ = entries;
            Err(unsupported())
        }
    }

    /// Returns whether this reader owns a native Linux io_uring instance.
    #[must_use]
    pub const fn is_native(&self) -> bool {
        cfg!(all(target_os = "linux", feature = "uring"))
    }

    /// Reads exactly one [`PAGE_SIZE`] page at the byte offset `location`.
    ///
    /// The call enqueues one request for the dedicated ring thread and waits
    /// synchronously for that request's uniquely identified completion.
    /// Concurrent callers may have reads in flight together. `location` is an
    /// absolute file byte offset, not a slot number.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for a negative fd, a buffer
    /// whose length is not exactly [`PAGE_SIZE`], an unaligned byte offset, or
    /// offset overflow. Native submission, completion, and CQE errors are
    /// propagated. Unsupported builds return [`io::ErrorKind::Unsupported`]
    /// after validating the arguments.
    pub fn read(&self, raw_fd: RawFd, location: TierOffset, buffer: &mut [u8]) -> Result<()> {
        validate_read(raw_fd, location, buffer.len())?;

        #[cfg(all(target_os = "linux", feature = "uring"))]
        {
            self.engine
                .read(raw_fd, location, buffer)
                .map_err(Into::into)
        }

        #[cfg(not(all(target_os = "linux", feature = "uring")))]
        {
            let _ = (raw_fd, location, buffer);
            Err(unsupported())
        }
    }

    /// Stops accepting requests, drains accepted reads, and joins the ring
    /// thread.
    ///
    /// Calling this method more than once is harmless. Reads submitted after
    /// shutdown return [`io::ErrorKind::BrokenPipe`].
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the ring thread terminated before receiving
    /// the shutdown command or panicked while draining requests.
    pub fn shutdown(&self) -> Result<()> {
        #[cfg(all(target_os = "linux", feature = "uring"))]
        {
            self.engine.shutdown().map_err(Into::into)
        }

        #[cfg(not(all(target_os = "linux", feature = "uring")))]
        {
            Ok(())
        }
    }

    #[cfg(all(test, not(all(target_os = "linux", feature = "uring"))))]
    fn unsupported_for_test() -> Self {
        Self {}
    }
}

impl Drop for UringReader {
    fn drop(&mut self) {
        #[cfg(all(target_os = "linux", feature = "uring"))]
        {
            let _ = self.engine.shutdown();
        }
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
impl NativeEngine {
    fn new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(entries)?;
        let queue_capacity = usize::try_from(entries)
            .map_err(|_| invalid_input_io("io_uring entry count does not fit usize"))?;
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let worker = thread::Builder::new()
            .name("tierbuf-uring".into())
            .spawn(move || run_completion_thread(ring, receiver))?;

        Ok(Self {
            sender,
            stopping: AtomicBool::new(false),
            submission_gate: RwLock::new(()),
            shutdown_lock: Mutex::new(()),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn read(&self, raw_fd: RawFd, location: TierOffset, destination: &mut [u8]) -> io::Result<()> {
        let submission = self
            .submission_gate
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.stopping.load(Ordering::Acquire) {
            return Err(engine_stopped());
        }

        let (completion, receiver) = mpsc::sync_channel(1);
        let request = ReadRequest {
            raw_fd,
            location,
            page: AlignedPage::boxed_zeroed(),
            completion,
        };
        self.sender
            .send(Command::Read(request))
            .map_err(|_| engine_stopped())?;
        drop(submission);

        let page = receiver.recv().map_err(|_| engine_stopped())??;
        destination.copy_from_slice(page.as_slice());
        Ok(())
    }

    fn shutdown(&self) -> io::Result<()> {
        let _shutdown = self
            .shutdown_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let first_shutdown = !self.stopping.swap(true, Ordering::AcqRel);
        let _submission = self
            .submission_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let send_error = if first_shutdown {
            self.sender
                .send(Command::Shutdown)
                .err()
                .map(|_| engine_stopped())
        } else {
            None
        };

        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let join_error = worker.and_then(|worker| {
            if worker.thread().id() == thread::current().id() {
                None
            } else {
                worker
                    .join()
                    .err()
                    .map(|_| io::Error::other("io_uring completion thread panicked"))
            }
        });

        if let Some(error) = join_error.or(send_error) {
            Err(error)
        } else {
            Ok(())
        }
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn run_completion_thread(ring: IoUring, receiver: Receiver<Command>) {
    let mut pending = VecDeque::new();
    let mut inflight = InflightRequests::default();
    // This owner is deliberately declared after both request collections.
    // Rust drops locals in reverse declaration order, so an unexpected unwind
    // closes the ring before either collection can release a kernel-visible
    // buffer.
    let mut ring = Some(ring);
    let mut next_user_data = 1_u64;
    let mut shutting_down = false;
    let mut fatal_error = None;

    loop {
        if pending.is_empty() && inflight.is_empty() && !shutting_down {
            match receiver.recv() {
                Ok(Command::Read(request)) => pending.push_back(request),
                Ok(Command::Shutdown) | Err(_) => shutting_down = true,
            }
        }

        loop {
            match receiver.try_recv() {
                Ok(Command::Read(request)) if !shutting_down => pending.push_back(request),
                Ok(Command::Read(request)) => {
                    complete_request(request, Err(engine_stopped()));
                }
                Ok(Command::Shutdown) => shutting_down = true,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    shutting_down = true;
                    break;
                }
            }
        }

        submit_pending(
            ring.as_mut().expect("ring is live until request cleanup"),
            &mut pending,
            &mut inflight,
            &mut next_user_data,
        );

        if inflight.is_empty() {
            if shutting_down && pending.is_empty() {
                break;
            }
            continue;
        }

        let wait_result = loop {
            match ring
                .as_ref()
                .expect("ring is live until request cleanup")
                .submit_and_wait(1)
            {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => break result,
            }
        };
        if let Err(error) = wait_result {
            fatal_error = Some(SharedIoError::from_error(&error));
            break;
        }

        let completions = ring
            .as_mut()
            .expect("ring is live until request cleanup")
            .completion()
            .map(|entry| (entry.user_data(), entry.result()))
            .collect::<Vec<_>>();
        if completions.is_empty() {
            fatal_error = Some(SharedIoError {
                kind: io::ErrorKind::InvalidData,
                message: "io_uring reported readiness without a completion".into(),
            });
            break;
        }

        for (user_data, result) in completions {
            let Some(request) = inflight.remove(&user_data) else {
                fatal_error = Some(SharedIoError {
                    kind: io::ErrorKind::InvalidData,
                    message: format!("io_uring returned unknown completion user_data {user_data}"),
                });
                break;
            };
            complete_cqe(request, result);
        }
        if fatal_error.is_some() {
            break;
        }
    }

    // Closing the ring initiates cancellation for requests the kernel may have
    // accepted before a fatal `io_uring_enter` failure. Because close is not a
    // documented synchronous user-buffer barrier, `fail_all` leaks only those
    // unresolved pages after reporting the failure to every waiter.
    drop(ring.take());
    if let Some(error) = fatal_error {
        fail_all(pending, &mut inflight, &error);
    } else {
        debug_assert!(pending.is_empty());
        debug_assert!(inflight.is_empty());
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn submit_pending(
    ring: &mut IoUring,
    pending: &mut VecDeque<ReadRequest>,
    inflight: &mut InflightRequests,
    next_user_data: &mut u64,
) {
    while let Some(request) = pending.pop_front() {
        let user_data = allocate_user_data(next_user_data, inflight);
        let previous = inflight.insert(user_data, request);
        debug_assert!(previous.is_none());
        let push_result = {
            let request = inflight
                .get_mut(&user_data)
                .expect("new in-flight request must remain present");
            crate::sys::uring::push_read(
                ring,
                request.raw_fd,
                request.location.get(),
                request.page.as_mut(),
                user_data,
            )
        };
        match push_result {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let request = inflight
                    .remove(&user_data)
                    .expect("SQ-full request must remain present");
                pending.push_front(request);
                break;
            }
            Err(error) => {
                let request = inflight
                    .remove(&user_data)
                    .expect("failed request must remain present");
                complete_request(request, Err(error));
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn allocate_user_data(next_user_data: &mut u64, inflight: &HashMap<u64, ReadRequest>) -> u64 {
    loop {
        let candidate = *next_user_data;
        *next_user_data = (*next_user_data).wrapping_add(1);
        if *next_user_data == 0 {
            *next_user_data = 1;
        }
        if candidate != 0 && !inflight.contains_key(&candidate) {
            return candidate;
        }
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn complete_cqe(request: ReadRequest, result: i32) {
    if result < 0 {
        complete_request(
            request,
            Err(io::Error::from_raw_os_error(result.saturating_neg())),
        );
    } else if result as usize != PAGE_SIZE {
        complete_request(
            request,
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("io_uring read completed with {result} bytes, expected {PAGE_SIZE}"),
            )),
        );
    } else {
        let ReadRequest {
            page, completion, ..
        } = request;
        let _ = completion.send(Ok(page));
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn complete_request(request: ReadRequest, result: io::Result<Box<AlignedPage>>) {
    let _ = request.completion.send(result);
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn fail_all(
    pending: VecDeque<ReadRequest>,
    inflight: &mut InflightRequests,
    error: &SharedIoError,
) {
    for request in pending {
        complete_request(request, Err(error.to_error()));
    }
    for (_, request) in inflight.0.drain() {
        let ReadRequest {
            page, completion, ..
        } = request;
        let _leaked = Box::leak(page);
        let _ = completion.send(Err(error.to_error()));
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn leak_kernel_visible_page(request: ReadRequest) {
    let ReadRequest { page, .. } = request;
    let _leaked = Box::leak(page);
}

#[cfg(all(target_os = "linux", feature = "uring"))]
impl SharedIoError {
    fn from_error(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn to_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

fn validate_read(raw_fd: RawFd, location: TierOffset, buffer_len: usize) -> Result<()> {
    if raw_fd < 0 {
        return Err(invalid_input("io_uring raw fd must be non-negative"));
    }
    if buffer_len != PAGE_SIZE {
        return Err(invalid_input(
            "io_uring read buffer length must equal PAGE_SIZE",
        ));
    }
    let offset = location.get();
    if !offset.is_multiple_of(PAGE_SIZE as u64) {
        return Err(invalid_input(
            "io_uring TierOffset must be PAGE_SIZE-aligned",
        ));
    }
    offset
        .checked_add(PAGE_SIZE as u64)
        .ok_or_else(|| invalid_input("io_uring read offset overflows u64"))?;
    Ok(())
}

fn invalid_input(message: &'static str) -> TierBufError {
    invalid_input_io(message).into()
}

fn invalid_input_io(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(not(all(target_os = "linux", feature = "uring")))]
fn unsupported() -> TierBufError {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "io_uring requires Linux and the tierbuf 'uring' Cargo feature",
    )
    .into()
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn engine_stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "io_uring completion thread is stopped",
    )
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::tier::TierOffset;
    use crate::{PAGE_SIZE, TierBufError};

    use super::UringReader;

    fn io_kind(error: TierBufError) -> io::ErrorKind {
        match error {
            TierBufError::Io(source) => source.kind(),
            other => panic!("expected I/O error, got {other:?}"),
        }
    }

    #[test]
    fn zero_entries_are_rejected_before_platform_detection() {
        let error = UringReader::new(0)
            .err()
            .expect("zero entries must be invalid");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);
    }

    #[cfg(not(all(target_os = "linux", feature = "uring")))]
    #[test]
    fn unsupported_build_reports_clear_errors_and_validates_reads() {
        let error = UringReader::new(8)
            .err()
            .expect("unsupported build must reject construction");
        assert_eq!(io_kind(error), io::ErrorKind::Unsupported);

        let reader = UringReader::unsupported_for_test();
        assert!(!reader.is_native());
        reader
            .shutdown()
            .expect("portable stub shutdown must be harmless");

        let mut page = vec![0; PAGE_SIZE];
        let error = reader
            .read(0, TierOffset::new(0), &mut page)
            .expect_err("unsupported read must fail");
        assert_eq!(io_kind(error), io::ErrorKind::Unsupported);

        let error = reader
            .read(0, TierOffset::new(0), &mut page[..PAGE_SIZE - 1])
            .expect_err("short buffer must fail before platform detection");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);

        let error = reader
            .read(0, TierOffset::new(1), &mut page)
            .expect_err("unaligned offset must fail before platform detection");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);

        let error = reader
            .read(-1, TierOffset::new(0), &mut page)
            .expect_err("negative fd must fail before platform detection");
        assert_eq!(io_kind(error), io::ErrorKind::InvalidInput);
    }

    #[cfg(all(target_os = "linux", feature = "uring"))]
    #[test]
    fn native_ring_reads_one_page_at_a_byte_offset() {
        use std::fs::{self, OpenOptions};
        use std::io::{Seek, SeekFrom, Write};
        use std::os::fd::AsRawFd;
        use std::path::PathBuf;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Barrier};
        use std::thread;

        static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

        struct TempPath(PathBuf);
        impl Drop for TempPath {
            fn drop(&mut self) {
                let _ = fs::remove_file(&self.0);
            }
        }

        let sequence = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = TempPath(std::env::temp_dir().join(format!(
            "tierbuf-uring-{}-{sequence}.bin",
            std::process::id()
        )));
        let _ = fs::remove_file(&path.0);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path.0)
            .expect("open temporary backing file");
        file.set_len((PAGE_SIZE * 2) as u64)
            .expect("size temporary file");
        file.seek(SeekFrom::Start(PAGE_SIZE as u64))
            .expect("seek second page");
        file.write_all(&vec![0xa5; PAGE_SIZE])
            .expect("write second page");
        file.flush().expect("flush test page");

        let reader = Arc::new(UringReader::new(8).expect("create native ring"));
        assert!(reader.is_native());
        let mut actual = vec![0; PAGE_SIZE];
        reader
            .read(
                file.as_raw_fd(),
                TierOffset::new(PAGE_SIZE as u64),
                &mut actual,
            )
            .expect("read second page through io_uring");
        assert_eq!(actual, vec![0xa5; PAGE_SIZE]);

        let error = reader
            .read(
                file.as_raw_fd(),
                TierOffset::new((PAGE_SIZE * 2) as u64),
                &mut actual,
            )
            .expect_err("a short read must reach its caller");
        assert_eq!(io_kind(error), io::ErrorKind::UnexpectedEof);

        let closed_fd = {
            let duplicate = OpenOptions::new()
                .read(true)
                .open(&path.0)
                .expect("open a disposable descriptor");
            duplicate.as_raw_fd()
        };
        let error = reader
            .read(closed_fd, TierOffset::new(0), &mut actual)
            .expect_err("a CQE error must reach its caller");
        match error {
            TierBufError::Io(source) => assert_eq!(source.raw_os_error(), Some(libc::EBADF)),
            other => panic!("expected I/O error, got {other:?}"),
        }

        const READERS: usize = 8;
        let barrier = Arc::new(Barrier::new(READERS));
        let workers = (0..READERS)
            .map(|_| {
                let reader = Arc::clone(&reader);
                let barrier = Arc::clone(&barrier);
                let raw_fd = file.as_raw_fd();
                thread::spawn(move || {
                    barrier.wait();
                    let mut page = vec![0; PAGE_SIZE];
                    reader
                        .read(raw_fd, TierOffset::new(PAGE_SIZE as u64), &mut page)
                        .expect("concurrent io_uring read");
                    page
                })
            })
            .collect::<Vec<_>>();

        for worker in workers {
            assert_eq!(
                worker.join().expect("reader thread must not panic"),
                vec![0xa5; PAGE_SIZE]
            );
        }

        reader.shutdown().expect("first shutdown");
        reader.shutdown().expect("idempotent shutdown");
        let error = reader
            .read(
                file.as_raw_fd(),
                TierOffset::new(PAGE_SIZE as u64),
                &mut actual,
            )
            .expect_err("reads after shutdown must fail");
        assert_eq!(io_kind(error), io::ErrorKind::BrokenPipe);
    }
}
