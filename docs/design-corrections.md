# v0.1 safety corrections

The implementation keeps the plan's mechanisms—fixed 64 KiB pages, a tagged
resident/non-resident fast path, hybrid latching, cooling, and economic
placement—but tightens several contracts that are not sound as originally
written.

## Stable page handles

The original `owner_swip: *const Swip` backpointer can outlive a caller-owned
`Swip` after that value is moved or dropped. `Swip` therefore owns an
`Arc`-backed control cell. Moving or cloning the public handle does not move
the tagged atomic word, and a resident frame retains a clone while it is
eligible for background eviction. Buffer-manager operations reject a second,
non-canonical handle for an already registered page ID.

Only the two tag bits that are actually encoded are assumed to be clear on a
metadata pointer. The 64 KiB alignment guarantee applies to page-data frames;
metadata lives separately and is naturally aligned. State-changing pointer
operations remain crate-private and compare the expected page/frame identity.

## Pin versus eviction

A frame has one atomic pin-control word. Its high bit is an `EVICTING`
reservation and the remaining bits are the pin count. A fixer may increment
the count only while the reservation is clear. An evictor may reserve a frame
only by changing exactly zero pins into `EVICTING`, then revalidates the page
ID, generation, and cooling state under the exclusive latch. This closes the
check-then-act race in a separate `pin_count == 0` test.

Cooling queue entries carry the frame index, page ID, and generation. Stale
entries from an earlier occupant or cooling cycle are ignored.

A subtler race exists after eviction: a fixer may have loaded the former Hot
address before the Swip is unswizzled, then try to pin after that frame reaches
the free queue. A cooling worker can similarly prevalidate an old ticket and
pause before its reservation CAS. A free-list pop therefore acquires the same
exclusive control bit, retains it through data loading and metadata install,
and atomically converts it into the new occupant's first pin. The free queue
also tracks membership independently so duplicate indices cannot be accepted.

## Fault coalescing and backing locations

The latch in a newly selected frame cannot coalesce two concurrent faults,
because contenders can select different frames. A per-page generation
coordinator therefore chooses one loader before any frame is selected.
Registered waiters consume that generation's same success or replayable error;
a later independent call may start a new generation and retry. This both
preserves the one-resident-frame invariant and prevents a one-shot backend
failure from producing different answers for simultaneous callers.

The page directory retains the latest backing location while the page is
resident. A clean eviction can reuse it; a dirty eviction installs a newly
written location before freeing the old slot. In v0.1 this is an ephemeral
tiered page store: the latest lower-tier location is authoritative, but crash
recovery and a permanently write-through cold copy remain out of scope.

## Optimistic byte access

Version validation does not make a concurrent non-atomic Rust byte read safe:
reading `&[u8]` while a writer mutates those bytes is a language-level data
race even if validation later fails. The safe `read_with` API therefore holds
a shared latch around an arbitrary byte-slice closure and uses the optimistic
version only for validation. Truly lock-free optimistic access is reserved for
payload formats whose fields are atomic, which can use `HybridLatch`'s version
API directly under their own documented contract.

This also invalidates the original plan's expectation that the safe arbitrary
byte API must beat a shared fix. The Criterion target records both paths, but
does not gate on that ordering: copying a race-free 64 KiB snapshot is
deliberately more expensive than borrowing a hot page under a shared latch.

## Portable direct and asynchronous I/O

Direct I/O and `io_uring` are Linux capabilities, not portable defaults.
`FileTier` validates exact page-sized buffers and supports an explicit
buffered fallback. The default `uring` feature compiles its native dependency
only on Linux; other platforms and `--no-default-features` retain the
synchronous path. Prefetch callers enqueue into a bounded worker pool and
return immediately. Each worker retains its frame reservation while a
dedicated ring owner/completion thread executes file-backed reads; each
request owns its 64 KiB-aligned kernel buffer, so no caller-borrowed pointer
crosses threads. The ring tracks multiple requests by unique completion IDs.
Shutdown drains accepted work and joins both the prefetch and ring workers.

Linux does not document ring-file close as a synchronous barrier for every
kernel-visible user buffer. If the ring itself fails fatally or its worker
unwinds before completions can be observed, tierbuf reports the error and
conservatively leaks only the unresolved aligned pages rather than risk a
kernel use-after-free. Normal CQE, ordinary I/O-error, and shutdown paths
reclaim every request buffer; the exceptional leak is bounded by ring
capacity.
