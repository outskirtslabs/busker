# ADR-007 Bounded native streaming storage

## Status

Accepted

## Context

Application virtual threads produce response bytes while H2O consumes them asynchronously on platform workers. Backpressure must bound pending output without invalidating bytes still being written or read. Receipt settlement, pending-byte credit and physical memory release have different completion events.

## Decision

Each worker has 256 reusable blocks of B bytes, where B is the positive `:output-buffer-size` setting. A platform thread allocates the blocks in a shared FFM arena at startup. Total streaming payload storage is 256 * B bytes per worker, or 8 MiB at the default B=32768, separate from the 256 fixed-response slots.

A response permits one producer at a time, at most B pending bytes and at most 64 unsettled receipts. Pending bytes include private reservations and accepted output awaiting a native send. A nonempty bounded write initially uses one block even if it writes fewer than B bytes. Empty writes consume receipt capacity but retain no block.

Application producers write through a temporary confined ByteBuffer view of a shared block. The callback runs synchronously on its calling virtual thread, uses its final position as the byte count and must not flip the buffer. Other threads cannot access that view. Closing its confined arena invalidates retained Java aliases before publication without freeing the backing allocation.

The platform worker constructs send descriptors pointing to the same blocks; it does not copy payload bytes when packaging a send. Each response has at most one native send of at most B bytes. Installing the complete in-flight record returns pending-byte credit before native entry, so synchronous callbacks can find it. Non-final proceed releases blocks and settles eligible last-byte receipt markers. Final sends retain blocks until request-pool cleanup, even if close has already settled their receipts.

Private blocks remain valid until the producer exits. Disconnect, interruption and retirement deadlines do not permit reuse while it is writing. The pool records free and checked-out blocks and rejects stale or duplicate releases. Retirement closes admission and frees the shared arena only when no blocks remain checked out; otherwise the caller retains the pool and retries.

Capacity pressure flushes accepted output. Global pool pressure visits only responses with published unflushed blocks; it cannot reclaim private callbacks or native sends. Blocking calls park through Java synchronization, while nonblocking calls return full without invoking their producer.

## Required verification

Tests check confined-view expiry and cross-thread rejection, exact bytes through real transports and compression, private-block retention during disconnect, final cleanup without proceed, duplicate release detection, and retirement with a delayed producer. Capacity tests distinguish pending bytes, receipt slots, physical blocks and mailbox permits, including global pressure across several responses.

## Consequences

Streaming payload allocation is fixed per worker. Protocol and compression buffers, request metadata, send descriptors and application-retained input are separate memory costs. A producer that never returns or uncertain native cleanup can retain the arena indefinitely; reporting incomplete retirement is required instead of freeing live storage.

See [the architecture](../architecture.md) for response acceptance, controls and generation lifecycle.
