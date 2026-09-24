# Architecture decision records

These records explain the design decisions made during Busker's development.
See [Busker architecture](../architecture.md) for the current design and how later decisions replace earlier ones.

- [001 — Response emitter close callback executor](001-response-emitter-close-callbacks.md)
  - Runs application close callbacks separately from event-loop threads.
- [002 — Worker-affine FFM callback dispatch](002-worker-affine-ffm-callback-dispatch.md)
  - Reuses native callback stubs per worker and routes callbacks to live requests.
- [003 — Coalesce worker mailbox wakeups](003-coalesce-worker-mailbox-wakeups.md)
  - Combines wake requests while preserving mailbox order.
- [004 — Platform notifier for native wakes](004-platform-notifier-for-native-wakes.md)
  - Moves native worker wake calls off application virtual threads.
- [005 — Platform executor for lifecycle operations](005-platform-executor-for-lifecycle.md)
  - Runs start, reload, and stop work on platform threads.
- [006 — Worker-side response head staging](006-worker-side-response-head-staging.md)
  - Prepares native status and header data on the event-loop worker.
- [007 — Heap streaming aggregation](007-heap-streaming-aggregation.md)
  - Combines application writes in heap buffers before the worker prepares native sends.
- [008 — Apply complete fixed responses through H2O's event loop](008-native-fixed-response-exchange.md)
  - Introduced a queued transfer of complete responses.
  - Superseded by ADR 009.
- [009 — Publish complete responses through preallocated native slots](009-preallocated-response-completion-ring.md)
  - Replaces ADR 008's transfer with reusable native response-ring slots.
