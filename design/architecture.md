# Busker architecture

Busker is a Clojure HTTP server built on [libh2o] through the Java Foreign Function and Memory API.
This document can be thought of as a materialized view of the [ADRs](./adr/README.md).
Busker has been quite a journey as I've attempted to increase performance while sticking to my goals, so many of the ADRs have been superceded or subtly changed.
So I created this document to document the current state.

This document is for Busker contributors/maintainers as it only covers implementation details.
Familiarity with the [user-facing docs][docs] is a pre-req.

[libh2o]: https://github.com/h2o/h2o
[docs]: https://docs.outskirtslabs.com/ol.busker/next/

[!NOTE]
"Ring" (capitalized) refers to the Clojure HTTP convention, while "ring" refers to a ring buffer.

## Execution model

Each generation starts one native libh2o event loop which runs in one Clojure platform thread per worker.
The event-loop thread runs native polling, mailbox handling, response-ring draining, request registration, and resource retirement.
`ol.busker.worker-context` binds the current worker on that thread so response-head and fixed-final operations can reject calls from other threads without a dependency cycle.

App code / Ring request handlers code run on virtual threads.
It must not block in native calls.
Native callbacks copy or signal data, then dispatch application handling through the worker and callback-dispatch table.
Lifecycle work uses platform threads because it can wait for threads and native resources to finish.

## Requests

The shim calls Clojure request callbacks with native request context data.
`ol.busker.native` decodes it, `ol.busker.request` builds Ring-compatible request state, and a virtual-thread handler produces a Ring response.
Streaming request bodies use a `java.nio.channels.ReadableByteChannel` and native proceed callbacks.
Callback-dispatch maps module and request sequence identifiers to live requests until cleanup.

## Responses

Internally there are two paths for responses to take: streaming responses or fixed-final.
A Ring response is sent down the fixed-final path if it has a complete string or byte-array body that fits within some configured size limits. 
This is a perf optimization so small requests can be sent in one clj->h2o operation.

All other responses use the streaming path, which sends body data in chunks with backpressure to prevent the application from producing data faster than the client can read it.

### Fixed-final responses

The fixed-final path avoids the queue, writer, and chunk-handling overhead for small responses whose complete body is already available.
The term comes about from:

- fixed: the complete body and its byte length are known before sending
- final: It completes the response and no new body chunks will follow, that is, it is not an interim response such as `100 Continue`

The fixed-final path uses preallocated slots to reduce per-response allocation and pass complete responses from handler threads to the event-loop worker without waiting for native I/O.
The slots are `clj_response_slot_t` structs maintained in a ring-buffer in native memory.

When a Ring handler returns a response, Busker claims a slot, copies the headers and body into it.
It then publishes the slot and requests a worker wake.
The slot keeps those bytes valid until the worker drains the ring and sends the response.

### Streaming responses

The streaming path supports bodies that arrive over time or do not fit the fixed-final limits.
Busker sends the status and headers first, then sends the body in chunks.
It also uses this path when no fixed-final slot is available.

Application code writes body data on a virtual thread.
Busker combines small writes into chunks and places them in a queue for that response.
The queue limits buffered bytes, not the number of chunks, so large writes cannot bypass the memory limit.
When the queue is full, the writing virtual thread waits without blocking the event-loop worker.

The event-loop worker takes chunks from the queue and passes them to libh2o which writes them to the socket.
libh2o requires Busker to wait for a proceed callback before submitting the next chunk for that response.
Until that callback, libh2o may still need the previous chunk's memory.
This allows libh2o to control the pace of delivery and keeps the data valid while it is in use.
This links application writes to network progress and prevents a slow client from causing unlimited buffering.

## Lifecycle

A generation is a running set of workers, listeners, and native resources.
During reload, Busker starts a new generation to accept new connections while the previous generation finishes work on its existing connections.
This lets configuration changes take effect without immediately interrupting active requests.

The previous generation stops accepting new connections but must still let its handlers send responses.
Busker keeps its response queues and native memory available until that work finishes or the shutdown timeout is reached.
Reaching the timeout does not make memory safe to free: Busker must first stop any thread or native operation that could still use it.

If startup fails partway through, Busker releases the resources it has already created.
Cleanup continues even if one release operation fails, and the original startup error is retained along with any cleanup errors.


## Decision record reconciliation

The [ADRs](adr/README.md) record decisions at the time they were made.
This document combines those decisions with later changes to describe the current design.
Where an older ADR differs from this document, use this document for current behavior and the ADR for historical context.

[ADR 009](adr/009-preallocated-response-completion-ring.md) supersedes [ADR 008](adr/008-native-fixed-response-exchange.md): preallocated native slots replace its queued, deep-copied response transfer.
This change does not replace the worker-affine callback dispatch in [ADR 002](adr/002-worker-affine-ffm-callback-dispatch.md), the mailbox ordering in [ADR 003](adr/003-coalesce-worker-mailbox-wakeups.md), or worker-side response-head staging in [ADR 006](adr/006-worker-side-response-head-staging.md).
[ADR 004](adr/004-platform-notifier-for-native-wakes.md) changes how workers are woken: a platform notifier makes the native wake calls, while the mailbox wake coalescing from ADR 003 remains in use.
The later shared header serializer serves both fixed-final responses and response-head staging; that consolidation is not part of ADR 009's original decision.

