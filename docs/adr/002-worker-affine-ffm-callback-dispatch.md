# ADR-002 Worker-affine FFM callback dispatch

## Status

Accepted

## Context

Busker currently creates FFM upcall stubs for request-body chunks, response proceed, and response stop/reset once per request. Two CPU profiles attributed 49.30% and 50.41% of samples to stub creation and cleanup. A temporary shared-stub probe removed the targeted profile share and raised median local throughput by 295.30% across five matched pairs.

The probe established the cause but used process-wide callbacks, automatic-arena lifetime, request-ID routing, and ambient current-worker lookup. Those choices do not define safe behavior during cleanup, reload, shutdown, identifier reuse, or overlapping runtime generations.

The pinned libh2o implementation invokes request-body, response-generator, and request-pool cleanup functions synchronously on the request's event-loop worker. Busker relies on request-pool cleanup as the final native request-lifetime event. These premises require tests for HTTP/1.1, HTTP/2, and HTTP/3.

## Decision

Each event-loop worker uses one private callback-dispatch module. The module creates exactly three stable FFM upcall stubs in its runtime generation's explicit shared arena. The request hot path reuses those stubs.

Each module has a positive, nonwrapping process-lifetime module ID. Each registered request has a positive, nonwrapping sequence value local to that module. The pair `{module-id, request-seq}` is the dispatch identity. Zero means uninstalled, and exhausted values cause module creation or request admission to fail rather than reuse an identity.

The shim stores both values in `clj_req_ctx_t` and passes them by value through typed callback functions. The three target JVM callbacks do not deserialize the native request context. No untyped application-data field, global request registry, per-request native allocation, or native reference-counted table is introduced.

A callback stub captures its worker module directly. Before table access, the module verifies its phase, event-loop thread, module ID, positive request sequence, live entry, and event kind. Dispatch uses a plain worker-local `HashMap`. Invalid, stale, late, or wrong-thread calls are no-ops with diagnostic counters and never fall back to a request ID, native address, another worker, or global state.

Request admission constructs request and response stream state, registers the dispatch entry, installs the identity and stable request-body callback, and only then permits native body delivery. Admission failure retires any registered entry before returning.

H2O request-pool cleanup is the only event that retires a dispatch entry. Response stop/reset stops the response writer but does not retire the request. Cleanup clears the three native target callback pointers before JVM retirement. Exact-identity removal is the retirement point; the removed entry then closes request input and stops response state exactly once. A later callback misses the entry and returns.

Every queued native request operation carries the dispatch identity and revalidates the live entry on the event-loop worker immediately before its downcall. A check before enqueue is not sufficient because cleanup can run before mailbox delivery.

Application handlers, middleware, and emitter close callbacks remain on virtual threads. Callback dispatch performs only identity checks, worker-local state changes, native-byte copying required before return, and existing flow-control work. It introduces no lock, semaphore, future wait, executor handoff, or application callback on an event-loop thread.

Request-body queue capacity, response byte limits, one-send-in-flight behavior, native credit, and cleanup behavior remain unchanged.

A runtime generation creates separate modules and stubs for each worker. Reload candidates and draining generations do not share modules, request tables, identities, or callback stubs.

Final release follows this order:

1. Stop new request admission and begin native connection draining.
2. Continue callback dispatch for admitted requests.
3. Dispose request and protocol contexts so request-pool cleanup retires entries.
4. Verify each worker module has no live entries.
5. Stop and join every event-loop worker.
6. Dispose remaining native structures that can retain callback pointers.
7. Close the runtime generation's shared arena, releasing its callback stubs.

If shutdown cannot establish worker join and final native-pointer release, it reports failed retirement and retains the runtime-generation resources. It must not close an arena while native code may still invoke a stub.

The per-request callback construction path is removed in the same coherent change. No runtime switch or compatibility path remains.

## Required verification

Tests against the pinned native library must demonstrate:

- request-body, response proceed/stop, and cleanup execute on the request's event-loop worker for HTTP/1.1, HTTP/2, and HTTP/3;
- generator stop precedes pool cleanup when a generator remains active;
- final response completion can reach cleanup without generator stop;
- no target callback occurs after request-pool disposal;
- callback pointers remain stable across many requests and release after repeated start, stop, and reload cycles;
- wrong, stale, duplicate, exhausted, and late identities never reach request state;
- queued native operations cannot use a retired request context;
- slow request and response readers retain bounded buffering and native flow control; and
- exact status, headers, body bytes, completion, disconnect, and socket-error behavior remain unchanged.

Any supported protocol path that violates the worker-thread or final-cleanup premise blocks this design and requires a new decision. Matched profiles and repeated throughput runs must also remove the targeted per-request stub lifecycle and improve median throughput beyond the declared tolerance before the optimization ships.

## Consequences

A runtime generation creates three stubs per event-loop worker rather than three per request. This adds negligible startup work proportional to worker count and removes the verified request-path cost.

The callback function pointer directly selects one worker module. Scalar identity avoids request-ID conversion, native-address routing, Java native-context reads, and concurrent global lookup.

The shim and Coffi layouts change together to carry two 64-bit values. A mismatch is a release blocker and requires size and offset checks on every supported target.

The design depends explicitly on pinned libh2o callback serialization and request-pool disposal order. A future native-library change that violates those premises requires a new design review rather than replacing the worker-local table with speculative concurrency.

A failed shutdown may retain a retired runtime generation to avoid releasing callable executable memory. This is visible operational failure, not silent cleanup.
