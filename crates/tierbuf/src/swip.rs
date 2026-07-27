//! Stable, atomically tagged page references.
//!
//! A [`Swip`](crate::swip::Swip) is a cheap, cloneable handle to one heap-allocated atomic state
//! cell. Cloning a handle keeps that cell alive, so moving or dropping another
//! handle cannot invalidate an owner retained by the buffer manager.

use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const TAG_EVICTED: u64 = 0b01;
const TAG_COOLING: u64 = 0b10;
const TAG_MASK: u64 = 0b11;
const PAGE_ID_MASK: u64 = u64::MAX >> 2;

/// A logical page identifier.
///
/// The largest 62-bit value is reserved as [`PageId::INVALID`], leaving all
/// values from zero through [`PageId::MAX`] available for real pages.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageId(u64);

impl PageId {
    /// Sentinel used in frame metadata when no logical page is installed.
    pub const INVALID: Self = Self(PAGE_ID_MASK);

    /// Largest value accepted as a real logical page identifier.
    pub const MAX: u64 = PAGE_ID_MASK - 1;

    /// Creates a valid logical page identifier.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        if value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Returns the integer representation of this identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns whether this value identifies a real logical page.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 <= Self::MAX
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Opaque identity of a resident frame.
///
/// This value is intentionally not a public raw pointer: callers may compare
/// identities returned by [`Swip::load`], but cannot manufacture one or
/// dereference it through the safe public API. Only two low tag bits are
/// assumed, so a resident frame needs four-byte alignment rather than 64-KiB
/// alignment.
#[repr(transparent)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ResidentAddr(u64);

impl fmt::Debug for ResidentAddr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ResidentAddr")
            .field(&format_args!("{:#x}", self.0))
            .finish()
    }
}

impl ResidentAddr {
    /// Captures the exposed address of a resident allocation.
    ///
    /// # Safety
    ///
    /// `pointer` must identify the frame represented by this token, and its
    /// allocation must remain live and at the same address for every interval
    /// in which a Swip contains this token.
    #[allow(dead_code)] // Used by the frame/pool phases that consume this primitive.
    pub(crate) unsafe fn from_non_null<T>(pointer: NonNull<T>) -> Self {
        let address = pointer.as_ptr().expose_provenance();
        assert_ne!(address, 0, "resident frame address must be non-null");
        assert_eq!(
            address & TAG_MASK as usize,
            0,
            "resident frame address must have its low two bits clear"
        );

        Self(u64::try_from(address).expect("resident address must fit the 64-bit Swip word"))
    }

    /// Reconstructs a typed pointer from this exposed address.
    ///
    /// # Safety
    ///
    /// The original allocation must still be live, and `T` must be the type
    /// and address captured by [`Self::from_non_null`]. The caller is
    /// responsible for all aliasing and synchronization requirements.
    #[allow(dead_code)] // Used by the frame/pool phases that consume this primitive.
    pub(crate) unsafe fn as_non_null<T>(self) -> NonNull<T> {
        let address =
            usize::try_from(self.0).expect("resident address must fit the target pointer width");
        let pointer = std::ptr::with_exposed_provenance_mut(address);

        // SAFETY: `from_non_null` rejects null and the caller upholds the
        // allocation, type, and lifetime requirements documented above.
        unsafe { NonNull::new_unchecked(pointer) }
    }

    /// Returns the exposed address for bounds-checked frame-table resolution.
    pub(crate) fn exposed_address(self) -> usize {
        usize::try_from(self.0).expect("resident address must fit the target pointer width")
    }

    #[cfg(test)]
    pub(crate) const fn from_test_address(address: u64) -> Self {
        assert!(address != 0, "test resident address must be non-zero");
        assert!(
            address & TAG_MASK == 0,
            "test resident address must have tag bits clear"
        );
        Self(address)
    }

    fn from_encoded(encoded: u64) -> Self {
        assert_ne!(encoded, 0, "resident Swip must not contain a null address");
        assert_eq!(encoded & TAG_MASK, 0, "resident address contains tag bits");
        Self(encoded)
    }
}

/// Snapshot of a [`Swip`]'s tagged state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwipState {
    /// The page is resident and eligible for ordinary access.
    Hot(ResidentAddr),

    /// The page remains resident but is an eviction candidate.
    Cooling(ResidentAddr),

    /// The page is not resident; the value is its logical identifier.
    Evicted(PageId),
}

#[derive(Debug)]
struct SwipInner {
    pid: PageId,
    owner_id: AtomicU64,
    word: AtomicU64,
}

/// A stable, cloneable handle to a 64-bit atomically tagged page state.
///
/// All clones refer to the same heap allocation. Code retaining an owner must
/// retain a cloned `Swip`, not a pointer to the movable outer handle. Resident
/// transitions are crate-private because installing a frame address requires
/// buffer-pool lifetime guarantees that a safe public caller cannot provide.
#[repr(transparent)]
#[derive(Clone)]
pub struct Swip {
    inner: Arc<SwipInner>,
}

impl Swip {
    /// Creates an evicted Swip for `pid`.
    ///
    /// # Panics
    ///
    /// Panics if `pid` is [`PageId::INVALID`].
    #[must_use]
    pub fn from_pid(pid: PageId) -> Self {
        assert!(pid.is_valid(), "cannot encode PageId::INVALID in a Swip");
        Self {
            inner: Arc::new(SwipInner {
                pid,
                owner_id: AtomicU64::new(0),
                word: AtomicU64::new(encode_evicted(pid)),
            }),
        }
    }

    /// Returns the immutable logical page identifier carried by this handle.
    #[must_use]
    pub fn pid(&self) -> PageId {
        self.inner.pid
    }

    /// Loads the current tagged state with acquire ordering.
    #[must_use]
    pub fn load(&self) -> SwipState {
        decode(self.inner.word.load(Ordering::Acquire))
    }

    /// Returns whether two handles share the same atomic state cell.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Binds a newly created handle to one buffer-manager identity.
    ///
    /// Manager identity zero is reserved for an unbound handle.
    pub(crate) fn try_bind_owner(&self, owner_id: u64) -> bool {
        assert_ne!(owner_id, 0, "buffer-manager identity zero is reserved");
        self.inner
            .owner_id
            .compare_exchange(0, owner_id, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Returns whether this handle was canonically bound to `owner_id`.
    pub(crate) fn belongs_to(&self, owner_id: u64) -> bool {
        owner_id != 0 && self.inner.owner_id.load(Ordering::Acquire) == owner_id
    }

    /// Changes exactly `Evicted(expected_pid)` to `Hot(resident)`.
    ///
    /// The expected page identity is part of the CAS word, so a stale fault
    /// cannot install its frame after the Swip has changed identity.
    #[allow(dead_code)] // Used by the frame/pool phases that consume this primitive.
    pub(crate) fn try_swizzle(
        &self,
        expected_pid: PageId,
        resident: ResidentAddr,
    ) -> Result<(), SwipState> {
        self.compare_exchange(encode_evicted(expected_pid), encode_hot(resident))
    }

    /// Changes exactly `Hot(expected_resident)` to
    /// `Cooling(expected_resident)`.
    #[allow(dead_code)] // Used by the cooling phase that consumes this primitive.
    pub(crate) fn try_mark_cooling(
        &self,
        expected_resident: ResidentAddr,
    ) -> Result<(), SwipState> {
        self.compare_exchange(
            encode_hot(expected_resident),
            encode_cooling(expected_resident),
        )
    }

    /// Changes exactly `Cooling(expected_resident)` back to
    /// `Hot(expected_resident)`.
    #[allow(dead_code)] // Used by the pool phase that consumes this primitive.
    pub(crate) fn try_resurrect(&self, expected_resident: ResidentAddr) -> Result<(), SwipState> {
        self.compare_exchange(
            encode_cooling(expected_resident),
            encode_hot(expected_resident),
        )
    }

    /// Changes exactly `Cooling(expected_resident)` to `Evicted(pid)`.
    ///
    /// The caller must first verify that the resident frame still contains
    /// `pid`; the exact resident identity in the CAS prevents a stale cooling
    /// queue entry from evicting a frame that has since been reused.
    #[allow(dead_code)] // Used by the cooling phase that consumes this primitive.
    pub(crate) fn try_unswizzle(
        &self,
        expected_resident: ResidentAddr,
        pid: PageId,
    ) -> Result<(), SwipState> {
        self.compare_exchange(encode_cooling(expected_resident), encode_evicted(pid))
    }

    #[allow(dead_code)] // Shared by staged crate-private transition helpers.
    fn compare_exchange(&self, expected: u64, replacement: u64) -> Result<(), SwipState> {
        self.inner
            .word
            .compare_exchange(expected, replacement, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(decode)
    }
}

impl fmt::Debug for Swip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Swip")
            .field("state", &self.load())
            .finish_non_exhaustive()
    }
}

fn encode_evicted(pid: PageId) -> u64 {
    assert!(pid.is_valid(), "cannot encode PageId::INVALID in a Swip");
    (pid.get() << 2) | TAG_EVICTED
}

#[allow(dead_code)] // Used by staged crate-private transition helpers.
const fn encode_hot(resident: ResidentAddr) -> u64 {
    resident.0
}

#[allow(dead_code)] // Used by staged crate-private transition helpers.
const fn encode_cooling(resident: ResidentAddr) -> u64 {
    resident.0 | TAG_COOLING
}

fn decode(encoded: u64) -> SwipState {
    match encoded & TAG_MASK {
        0 => SwipState::Hot(ResidentAddr::from_encoded(encoded)),
        TAG_COOLING => SwipState::Cooling(ResidentAddr::from_encoded(encoded & !TAG_MASK)),
        TAG_EVICTED => {
            let pid = PageId(encoded >> 2);
            assert!(pid.is_valid(), "Swip contains reserved PageId::INVALID");
            SwipState::Evicted(pid)
        }
        _ => panic!("Swip contains the reserved 0b11 tag"),
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use proptest::prelude::*;

    use super::{PageId, ResidentAddr, Swip, SwipState};

    #[repr(align(8))]
    struct TestResident {
        _bytes: [u8; 8],
    }

    fn pid(value: u64) -> PageId {
        PageId::new(value).expect("test page id must be valid")
    }

    fn resident(value: &mut TestResident) -> ResidentAddr {
        // SAFETY: Tests keep each TestResident alive and fixed in its Box
        // until every Swip has transitioned away from the captured address.
        unsafe { ResidentAddr::from_non_null(NonNull::from(value)) }
    }

    #[test]
    fn evicted_page_id_round_trips_for_boundary_and_patterned_values() {
        let mut values = vec![
            0,
            1,
            2,
            3,
            0xff,
            0x100,
            0xffff,
            1 << 31,
            1 << 47,
            PageId::MAX,
        ];
        values.extend((0..4096).map(|value| (value * 0x9e37_79b9_u64) & PageId::MAX));

        for value in values {
            let page_id = pid(value);
            assert_eq!(Swip::from_pid(page_id).load(), SwipState::Evicted(page_id));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn arbitrary_valid_page_id_round_trips(value in 0_u64..=PageId::MAX) {
            let page_id = PageId::new(value).expect("strategy emits valid page ids");
            prop_assert_eq!(
                Swip::from_pid(page_id).load(),
                SwipState::Evicted(page_id)
            );
        }
    }

    #[test]
    fn clones_keep_one_heap_stable_state_cell_alive() {
        let original = Swip::from_pid(pid(7));
        let retained_owner = original.clone();
        assert!(original.shares_state_with(&retained_owner));

        let moved_handles = vec![original];
        assert!(moved_handles[0].shares_state_with(&retained_owner));
        drop(moved_handles);

        assert_eq!(retained_owner.load(), SwipState::Evicted(pid(7)));
    }

    #[test]
    fn all_allowed_transitions_preserve_resident_identity() {
        let mut frame = Box::new(TestResident { _bytes: [0; 8] });
        let frame_address = resident(&mut frame);
        let swip = Swip::from_pid(pid(11));

        swip.try_swizzle(pid(11), frame_address)
            .expect("Evicted must become Hot");
        assert_eq!(swip.load(), SwipState::Hot(frame_address));

        swip.try_mark_cooling(frame_address)
            .expect("Hot must become Cooling");
        assert_eq!(swip.load(), SwipState::Cooling(frame_address));

        swip.try_resurrect(frame_address)
            .expect("Cooling must become Hot");
        assert_eq!(swip.load(), SwipState::Hot(frame_address));

        swip.try_mark_cooling(frame_address)
            .expect("Hot must become Cooling again");
        swip.try_unswizzle(frame_address, pid(11))
            .expect("Cooling must become Evicted");
        assert_eq!(swip.load(), SwipState::Evicted(pid(11)));
    }

    #[test]
    fn exact_cas_rejects_stale_pid_and_resident_identities() {
        let mut first_frame = Box::new(TestResident { _bytes: [0; 8] });
        let mut second_frame = Box::new(TestResident { _bytes: [0; 8] });
        let first_address = resident(&mut first_frame);
        let second_address = resident(&mut second_frame);
        let swip = Swip::from_pid(pid(21));

        assert_eq!(
            swip.try_swizzle(pid(22), second_address),
            Err(SwipState::Evicted(pid(21)))
        );
        swip.try_swizzle(pid(21), first_address)
            .expect("matching pid must win");

        assert_eq!(
            swip.try_mark_cooling(second_address),
            Err(SwipState::Hot(first_address))
        );
        assert_eq!(
            swip.try_unswizzle(first_address, pid(21)),
            Err(SwipState::Hot(first_address))
        );

        swip.try_mark_cooling(first_address)
            .expect("matching resident must cool");
        assert_eq!(
            swip.try_resurrect(second_address),
            Err(SwipState::Cooling(first_address))
        );
        assert_eq!(
            swip.try_unswizzle(second_address, pid(21)),
            Err(SwipState::Cooling(first_address))
        );
        swip.try_unswizzle(first_address, pid(21))
            .expect("matching resident must evict");
    }

    #[test]
    fn direct_hot_to_evicted_transition_is_not_available() {
        let mut frame = Box::new(TestResident { _bytes: [0; 8] });
        let frame_address = resident(&mut frame);
        let swip = Swip::from_pid(pid(31));

        swip.try_swizzle(pid(31), frame_address)
            .expect("matching fault must install");

        assert_eq!(
            swip.try_unswizzle(frame_address, pid(31)),
            Err(SwipState::Hot(frame_address))
        );
    }

    #[test]
    fn concurrent_cooling_and_resurrection_never_damage_tags() {
        const THREADS: usize = 8;
        #[cfg(not(miri))]
        const ITERATIONS: usize = 10_000;
        #[cfg(miri)]
        const ITERATIONS: usize = 100;

        let mut frame = Box::new(TestResident { _bytes: [0; 8] });
        let frame_address = resident(&mut frame);
        let swip = Swip::from_pid(pid(41));

        swip.try_swizzle(pid(41), frame_address)
            .expect("matching fault must install");

        let start = Arc::new(Barrier::new(THREADS));
        thread::scope(|scope| {
            for _ in 0..THREADS {
                let worker_swip = swip.clone();
                let worker_start = Arc::clone(&start);
                scope.spawn(move || {
                    worker_start.wait();
                    for _ in 0..ITERATIONS {
                        match worker_swip.load() {
                            SwipState::Hot(observed) => {
                                assert_eq!(observed, frame_address);
                                let _ = worker_swip.try_mark_cooling(observed);
                            }
                            SwipState::Cooling(observed) => {
                                assert_eq!(observed, frame_address);
                                let _ = worker_swip.try_resurrect(observed);
                            }
                            SwipState::Evicted(_) => {
                                panic!("worker cannot evict the resident frame");
                            }
                        }
                    }
                });
            }
        });

        match swip.load() {
            SwipState::Hot(observed) | SwipState::Cooling(observed) => {
                assert_eq!(observed, frame_address);
            }
            SwipState::Evicted(_) => panic!("resident state was unexpectedly evicted"),
        }
    }

    #[test]
    #[should_panic(expected = "PageId::INVALID")]
    fn invalid_page_id_cannot_be_encoded() {
        let _ = Swip::from_pid(PageId::INVALID);
    }

    #[test]
    fn exposed_address_can_be_recovered_by_internal_code() {
        let mut frame = Box::new(TestResident { _bytes: [0; 8] });
        let expected = NonNull::from(frame.as_mut());
        let address = resident(&mut frame);

        // SAFETY: `frame` is still live, and the requested type matches the
        // type used to create `address`.
        let recovered = unsafe { address.as_non_null::<TestResident>() };
        assert_eq!(recovered, expected);
    }
}
