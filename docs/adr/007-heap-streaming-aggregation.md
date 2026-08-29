# ADR-007 Heap streaming aggregation

## Status

Accepted

## Context

Streaming response producers run on application virtual threads. They aggregate body bytes into pooled `ByteBuffer` instances before publishing byte-bounded chunks to the event-loop worker.

The producer previously requested direct buffers. A pool miss called `ByteBuffer.allocateDirect` on the virtual thread. The worker did not pass that direct storage to H2O. It copied each buffer into a heap byte array and then into request-lifetime native memory before sending.

Direct producer storage therefore added native-memory allocation risk without removing a copy.

## Decision

Streaming producers borrow heap aggregation buffers. The byte-bounded queue accepts read-only slices from heap or direct buffers. The event-loop worker keeps the existing copy into request-lifetime native memory and performs the H2O send.

Buffer capacity, byte permits, flush behavior, final chunks, disconnect cleanup, and one-send-in-flight behavior do not change.

## Required verification

Tests must show that:

- a producer pool miss requests a heap buffer;
- worker packaging accepts heap buffers and preserves exact bytes;
- queued and in-flight byte accounting remains bounded;
- stopped and disconnected streams return aggregation buffers;
- slow clients still suspend producers through Java synchronization; and
- large payloads remain exact under the configured memory limit.

## Consequences

Application virtual threads no longer allocate direct response buffers. Streaming still performs one worker-side copy into native request memory. A later optimization may replace that copy with a bounded native-capable pool created and managed by platform threads.
