# Busker architecture

Busker is a Clojure-first HTTP server built on libh2o through the Java Foreign Function and Memory API. This document describes the running system. ADRs record decisions and history; they do not replace this projection.

## Execution model

Each generation starts one native libh2o event loop and one Clojure platform thread per worker. The event-loop thread runs native polling, mailbox handling, response-ring draining, request registration, and resource retirement. `ol.busker.worker-context` binds the current worker on that thread so response-head and fixed-final operations can reject calls from other threads without a dependency cycle.

Request application code runs on virtual threads. It must not block in native calls. Native callbacks copy or signal data, then dispatch application handling through the worker and callback-dispatch table. Lifecycle work uses platform threads because it can wait for threads and native resources to finish.

## Requests

The shim calls Clojure request callbacks with native request context data. `ol.busker.native` decodes it, `ol.busker.request` builds Ring-compatible request state, and a virtual-thread handler produces a Ring response. Streaming request bodies use a channel and native proceed callbacks. Callback-dispatch maps module and request sequence identifiers to live requests until cleanup.

## Responses

Streaming response heads use `ol.busker.response-head`. A head means status and headers before body delivery; it is not the HTTP `HEAD` method. The worker stages `clj_header_t` descriptors and UTF-8 payload in a confined arena. Native code copies this data during the call.

Complete eligible responses use the fixed-final path. `ol.busker.response` first prepares one direct plan. Its headers are encoded by `ol.busker.response-serialization`, which is shared with response-head and FIFO fixed-final delivery. A direct plan claims a preallocated response-ring slot, writes `clj_header_t` descriptors that point into that slot's retained payload, publishes the slot, and requests a worker wake. The worker drains the ring and sends the final response. The native validator checks descriptor address ranges as integer ranges before it reads them.

A final response after an informational response uses the FIFO fixed-final command so its ordering is preserved. Ineligible direct candidates and direct-ring overload use the generic start-response writer; closed response admission stops delivery. FIFO delivery stages the same descriptor representation in worker-local scratch storage. Direct slot metadata is 2120 bytes on supported 64-bit targets: 64 retained 32-byte descriptors add 1024 bytes per slot, or 256 KiB for a 256-slot worker, compared with the former packed representation.

Streaming response bodies use response-channel, response-queue, and byte-bounded-queue. The byte-bounded queue limits queued bytes rather than item count. Native writable callbacks resume draining. The wake notifier carries only wake signals; mailboxes and response queues carry work.

## Lifecycle

A runtime can publish a new generation and drain an earlier one. Draining stops listener admission, permits already admitted response work to complete, and retires receivers only after pending native response work is clear. Receiver destruction closes response admission, takes writer protection, and rechecks pending work. Startup allocation failure unwinds resources in reverse order.

Native receiver, callback, slot, and arena lifetimes are tied to the generation. Slot payload remains valid from claim through READY and drain. Generation stop waits for worker and lifecycle conditions before native disposal.

## Decision record reconciliation

- [ADR 001](adr/001-response-emitter-close-callbacks.md) defines response-emitter close callbacks.
- [ADR 002](adr/002-worker-affine-ffm-callback-dispatch.md) defines worker-affine FFM callback dispatch.
- [ADR 003](adr/003-coalesce-worker-mailbox-wakeups.md) defines coalesced worker mailbox wakes.
- [ADR 004](adr/004-platform-notifier-for-native-wakes.md) defines the platform wake notifier.
- [ADR 005](adr/005-platform-executor-for-lifecycle.md) defines the platform lifecycle executor.
- [ADR 006](adr/006-worker-side-response-head-staging.md) defines worker-side response-head staging.
- [ADR 007](adr/007-heap-streaming-aggregation.md) defines heap streaming aggregation.
- [ADR 008](adr/008-native-fixed-response-exchange.md) is superseded by [ADR 009](adr/009-preallocated-response-completion-ring.md). ADR 002 callback dispatch, ADR 003 mailbox ordering, and ADR 006 response-head worker staging remain current. ADR 004 moves native direct wakes to the platform notifier. The shared header serializer is the current H5 implementation and is not attributed to ADR 009.

## Keeping this document current

An ADR change must update the affected sections here. An architectural code change must update this document and the relevant ADRs in the same change set. Historical ADR text must not be read as the current system when this document states a later replacement.
