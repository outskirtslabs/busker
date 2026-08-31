---
status: proposed
---

# Publish complete responses through preallocated native slots

The native fixed-response exchange improved throughput but added a platform dispatcher, Java queue, repeated header encoding, native allocation, copying, and an H2O queue mutex. A matched profile assigned approximately 1.83 CPU microseconds/request to the dispatcher role while confirming that transport-side completion reduced H2O event-loop work per request.

Busker will test a fixed-capacity completion ring allocated with each H2O response receiver. An application virtual thread may claim one slot, write a bounded complete response directly into it, and publish or abort it through short native operations. Those operations use lock-free atomics, perform no allocation or response I/O, have fixed retry limits, and cannot wait. A claim token rejects stale publish or abort calls after slot reuse. The established platform notifier wakes H2O, and the H2O event-loop thread validates request identity, applies ready responses, and releases slots.

This ring initially coexists with the dispatcher and does not change ordinary response routing. Informational and streaming responses retain their existing FIFO, `proceed`, byte-limit, cancellation, buffer-lifetime, and emitter-callback behavior. Production cutover requires deleting the dispatcher path for eligible complete responses and validating overload fallback, shutdown, reload, exact responses, and performance under the controlled campaign method.
