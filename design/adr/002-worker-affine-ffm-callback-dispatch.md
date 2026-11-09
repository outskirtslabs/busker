# ADR-002 Worker-affine FFM callback dispatch

## Status

Accepted

## Context

The pinned libh2o implementation invokes request-body, response-generator and request-pool cleanup functions synchronously on the request's event-loop worker. Request-pool cleanup is the final native request-lifetime event. Dispatch must reject stale identities and keep callback stubs callable throughout request processing, reload and retirement.

## Decision

Each event-loop worker has a private callback-dispatch module with exactly three stable FFM upcall stubs in its generation's explicit shared arena. A stub captures that worker module directly.

Each module has a positive, nonwrapping process-lifetime module ID. Each registered request has a positive, nonwrapping sequence value local to the module. The pair `{module-id, request-seq}` identifies the dispatch entry. Zero means uninstalled. Exhaustion fails module creation or request admission rather than reusing an identity.

The shim stores both values in `clj_req_ctx_t` and passes them by value through typed callbacks. The JVM callbacks do not deserialize the native request context. Before table access, dispatch verifies phase, event-loop thread, module ID, positive request sequence, live entry and event kind. A worker-local `HashMap` stores entries. Invalid, stale, late and wrong-thread calls increment diagnostic counters and return without touching request state.

Request admission constructs request and emitter state, registers the entry, installs the identity and stable request-body callback, then permits native body delivery. Admission failure retires any registered entry before returning. Streaming response state is initialized only when needed.

H2O request-pool cleanup retires a dispatch entry. Response stop/reset closes response admission and settles receipts but does not retire the request or release a potentially live native send. Cleanup clears native target callback pointers before JVM retirement. Exact-identity removal happens once; the removed entry closes request input and cleans up response state. Input cleanup failure cannot suppress output cleanup or close callbacks. Later callbacks miss the entry and return.

Queued native request operations carry the dispatch identity and revalidate it on the event-loop worker immediately before native access. A check before enqueue is insufficient because cleanup can precede mailbox delivery.

Fixed slots and streaming blocks retain payload bytes independently of application views. A producer-use reference protects fixed receiver storage during preparation; its short admission lock is released before application code runs. Native request cleanup releases retained fixed slots and final streaming sends. Non-final streaming proceed ends one send and can start the next directly.

Handlers, middleware and emitter close callbacks run on virtual threads. Native dispatch performs identity checks, worker state changes, required byte copying and flow-control work, never arbitrary application callbacks. Generation candidates and draining generations have separate modules, tables, identities and callback stubs.

Final release requires:

1. Stop new request admission and drain native connections while dispatch remains available to admitted requests.
2. Establish that requests, producers, mailbox permits and native payload references have finished.
3. Unregister receivers before disposing their H2O contexts.
4. Stop and join every event-loop worker.
5. Dispose remaining native structures that can retain callback pointers.
6. Close the generation's shared arena and release its callback stubs.

Partial-startup contexts without workers also require receiver removal before context disposal. If native use or safe cleanup is uncertain, dependent memory must remain allocated. Failed retirement is reported and retains the generation rather than invalidating callable stubs or payload addresses.

## Required verification

Tests against the pinned library check worker affinity for HTTP/1.1, HTTP/2 and HTTP/3; stop before cleanup for active generators; final cleanup without stop; no callbacks after disposal; stable pointers across requests; exact identity rejection; stale queued operations; partial-startup cleanup order; and retention on failed retirement.

The shim and FFM layouts must agree on field sizes and offsets on every supported target. Real transport tests check status, headers, body bytes, completion, disconnect and backpressure.

## Consequences

Stub count is three per worker and independent of request count. Scalar identities avoid request-ID conversion, native-address routing and a concurrent global request table. The design depends on pinned libh2o callback serialization and cleanup order. Native library updates must preserve those premises or require a design review.
