# tierbuf architecture and invariants

This document describes the implemented v0.1 kernel contracts, including
autonomous cooling, bounded background prefetch, optional Linux `io_uring`,
cost accounting, and degradation-curve orchestration. Safety-driven changes
from the original plan are explained separately in
[design-corrections.md](design-corrections.md).

## Module map

| Module | Responsibility |
| --- | --- |
| `pool` | `BufferManager`, canonical page ownership, `PageDirectory`, allocate/fix/fault paths, background workers, prefetch, and page guards |
| `cooling` | Generation-checked FIFO entries for autonomous second-chance eviction |
| `swip` | Stable `Arc`-backed page handles and atomic `Hot`/`Cooling`/`Evicted` transitions |
| `frame` | Frame metadata, pin/eviction control word, stable resident identities, aligned-data association, and free queue |
| `latch` | Versioned shared, exclusive, and optimistic latch primitives |
| `sys::mmap` | Anonymous mapping, 64 KiB frame alignment, and isolated raw page-reference construction |
| `tier` | `TierBackend`, `FileTier`, `MockTier`, offsets, latency profiles, and deterministic write budgets |
| `policy::heat` | Global epoch plus lazy per-frame 8.24 fixed-point heat decay |
| `policy::economic` | DRAM page cost, read opportunity cost, break-even calculation, and demotion/admission decisions |
| `metrics` | Per-tier counters and time-integrated cost reporting |
| `uring` | Dedicated optional Linux `io_uring` submission/completion engine with owned aligned request buffers |

Page metadata and page bytes are deliberately separate. `Frame` values live in
a stable `Box<[Frame]>`; bytes live in one aligned anonymous mapping. A frame
index is the join key between them.

## Swip state machine

```text
                       successful fault CAS
             ┌──────────────────────────────────┐
             │                                  ▼
      Evicted(PageId)                       Hot(ResidentAddr)
             ▲                                  │
             │                                  │ mark_cooling CAS
             │                                  ▼
             └────────────────────────── Cooling(ResidentAddr)
                write-back committed +          │
                exact unswizzle CAS              │ resurrect CAS
                                                └──────────► Hot
```

The allowed transitions are:

1. `Evicted(pid) → Hot(addr)` after one fault loader installs an exact frame.
2. `Hot(addr) → Cooling(addr)` when the frame becomes an eviction candidate.
3. `Cooling(addr) → Hot(addr)` when a fixer gives it a second chance.
4. `Cooling(addr) → Evicted(pid)` only after lower-tier write-back and
   directory publication.

There is no direct `Hot → Evicted` operation. Every compare-and-swap includes
the expected page or resident identity, so stale queue entries and stale fault
loaders cannot transition a reused frame.

`ResidentAddr` is an opaque metadata identity, not a public dereferenceable
pointer. The 64 KiB alignment promise applies to page-data frames; tagged
metadata identities require only the two low tag bits to be clear.

## Resident fix and pin ordering

A shared or exclusive resident fix follows this order:

1. Validate that the supplied `Swip` is the manager's canonical handle for its
   `PageId`.
2. Load the tagged state. A resident state yields its `ResidentAddr` without a
   page-directory lookup.
3. Resolve the address to a frame index and attempt `Frame::try_pin`.
4. `try_pin` uses a CAS on one `AtomicU32`. Its high bit is `EVICTING`; the
   remaining 31 bits are the pin count. It increments only while the high bit
   is clear.
5. Acquire the requested shared or exclusive `HybridLatch` mode.
6. Revalidate the Swip state, page ID, frame identity, and stable owner clone.
   A mismatch releases the latch and pin, then retries.
7. If a fixer encountered `Cooling`, resurrect it to `Hot`.
8. Return a guard. Its `Drop` removes the pin, then the guard-owned latch token
   releases. If an evictor claims the resulting zero-pin frame in that small
   interval, it must still wait for the exclusive latch before proceeding.

The byte slice exists only inside `read_with` or `write_with`. This makes latch
and pin lifetimes structural: safe callers cannot retain a reference after the
guarded closure returns.

## Fault ordering

An `Evicted(pid)` fix enters the slow path:

1. Resolve the canonical per-page control record and join its fault
   generation. Exactly one leader proceeds before choosing a frame; registered
   followers later receive that generation's same success or replayable error.
2. Recheck the Swip. If another loader already made it resident, return to the
   fast-path retry.
3. Look up `pid` in `PageDirectory` to obtain the authoritative lower-tier
   location.
4. Pop one frame index from the bounded lock-free free queue.
5. Read exactly one page from the selected `TierBackend` into the aligned frame
   while holding exclusive byte access.
6. Install page ID, stable owner-Swip clone, clean state, and a new frame
   generation.
7. CAS exactly `Evicted(pid) → Hot(resident_addr)`.
8. On a failed CAS, clear the unpublished frame and return it to the free
   queue. On success, the outer fix loop pins and latches the new resident
   frame normally.

The generation is carried by cooling work so a delayed entry cannot act on a
later occupant of the same frame index.

## Eviction ordering

The eviction mechanism follows the reverse publication order:

1. Reserve an unpinned frame with the exact CAS `0 → EVICTING`. A fixer cannot
   add a pin while this bit is set.
2. Change the exact resident Swip from `Hot` to `Cooling`, or confirm that the
   same address is already cooling.
3. Acquire the frame's exclusive latch.
4. Revalidate resident address, page ID, owner identity, and Swip state under
   the reservation and latch. A queued cooling entry must additionally match
   the recorded frame generation.
5. If write-back is required, write the complete page to the selected lower
   tier. Do not change the visible resident state on failure.
6. Publish the new `PageDirectory` location before removing the resident
   address. Mark the frame clean only after the write succeeds.
7. CAS exactly `Cooling(addr) → Evicted(pid)`.
8. Clear metadata, release `EVICTING`, and return the index to the free queue.
9. Only after publication succeeds may an obsolete lower-tier slot be freed.

Any failure before step 7 resurrects the exact cooling address where possible
and releases the reservation. The cooler samples low-heat frames whenever the
free count is below its watermark; the evictor consumes generation-checked
FIFO entries and applies this protocol.

## Free-frame checkout ordering

Removing an index from the free queue is not by itself an atomic claim on its
metadata. A fixer that loaded the old resident Swip just before eviction can
transiently pin the now-free frame before its mandatory revalidation fails.
Likewise, a cooling worker can prevalidate the old occupant, pause, and attempt
its reservation after the frame has been returned.

Every free checkout therefore waits until it can CAS the frame from zero pins
to `EVICTING`. The checkout retains that reservation while zeroing or reading
the page and installing metadata, then atomically converts `EVICTING → 1` for
the new occupant's first pin. A parallel membership bit per frame also makes a
duplicate free-queue insertion an immediate invariant failure.

## Tier authority

`BufConfig::tiers` is ordered from the fastest lower tier toward the
authoritative cold tier. `PageDirectory` maps each logical `PageId` to the
latest committed lower-tier location and deliberately retains that entry while
the page is resident.

Authority depends on dirty state:

- a newly allocated page is dirty and authoritative only in its pinned DRAM
  frame until its first successful write-back;
- for a clean resident page, the directory location contains the same logical
  contents and can be reused by a later eviction;
- for a dirty resident page, DRAM is the newest copy and the recorded
  lower-tier slot may be stale;
- write-back publishes the new location before unswizzling and frees an old
  slot only after the new location is visible.

This is cache semantics, not crash consistency. The directory is process-local,
there is no WAL, and restart recovery is outside v0.1.

## Policy and time

Heat is an unsigned 8.24 fixed-point value packed with its last epoch in one
`AtomicU64`. `on_epoch` advances only a global counter. The next access applies
`heat >>= elapsed_epochs` (capped at a full-word shift) and then adds `1.0`
with saturation in one CAS loop.

`EconomicPolicy` converts DRAM dollars/GiB-month to a page-second cost and
lower-tier read latency to an explicit configurable opportunity cost. Their
ratio is a break-even reaccess interval. Hot pages favor the fastest eligible
tier; sufficiently cold pages may choose a cheaper tier. A tier with less than
one page of write budget is skipped, and `AccessHint::Scan` bypasses DRAM
admission.

`BufferManager` advances the policy clock and write budgets on its epoch
worker, integrates residency cost, and uses the policy for scan admission and
write-back target selection.

## Prefetch and Linux I/O

`prefetch` accepts only canonical evicted handles, never waits for queue space,
reserves free frames at submission, and caps queued plus running work at 128
requests. Four background workers own accepted requests until the fault either
publishes a resident frame or returns every reservation on error. Before the
resident Swip becomes visible, a completed prefetch publishes a one-shot
`(resident address, frame generation)` marker. The next exact demand fix
consumes it; eviction removes it, so frame reuse cannot create a false hit.

On Linux with the default `uring` feature, file-backed reads use
`UringReader` when ring construction and a raw file descriptor are available.
One dedicated thread owns the ring, batches queued requests into the SQ, and
routes CQEs by unique request ID. Each request owns a 64 KiB-aligned buffer
until its CQE is observed; the waiting demand or prefetch worker then copies it
into the reserved frame. Unsupported kernels, non-file tiers, non-Linux
targets, and `--no-default-features` use the synchronous backend path.

## Core invariants

1. A pinned frame cannot acquire the `EVICTING` reservation.
2. One logical page maps to at most one resident frame.
3. Dirty contents are never discarded before a successful lower-tier write.
4. At least one of the resident Swip or committed directory location remains
   valid during movement.
5. A stale page ID, resident address, generation, or owner identity cannot
   transition a reused frame.
6. Safe page references cannot escape their latch and pin lifetime.
7. A free-list frame is exclusively reserved from checkout through its first
   published pin.
8. One fault generation performs at most one backing read, and every
   registered follower observes that generation's same outcome.
