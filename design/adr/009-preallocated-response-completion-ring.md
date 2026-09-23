---
status: accepted
---

# Publish complete responses through preallocated native slots

Eligible complete final responses use a fixed-capacity response ring allocated with each H2O response receiver. The handler virtual thread claims a slot, packs bounded headers and a string or byte-array body into its preallocated native storage, and publishes the result. Claim, publish, and abort are short native operations with fixed retry limits; they do not wait. A packing or publication failure aborts the claim. Publishing wakes the event-loop worker, which drains ready slots, validates each scalar request identity, applies the response, and releases the slot.

The direct path applies only to eligible complete final responses. A final response that is ineligible or finds the ring overloaded uses the generic start-response and response-writer path through the request worker's FIFO mailbox. Informational responses use that mailbox, and a fixed final response sent after an informational response uses it too, preserving response order. Streaming responses use the existing start-response command, body writer, `sendvec` operations, and H2O `proceed` callbacks; their byte limits, cancellation, buffer lifetime, and emitter close callbacks remain unchanged.

This replaces the platform dispatcher and Java completion queue proposed in ADR-008 for eligible complete final responses. The direct path does not require per-response native allocation or the H2O response-message mutex. It does not replace the generic, informational, or streaming paths.
