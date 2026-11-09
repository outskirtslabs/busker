# Architecture decision records

These records describe Busker's design decisions.
See [Busker architecture](../architecture.md) for the execution, response-storage and lifecycle contracts.

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
- [007 — Bounded native streaming storage](007-bounded-native-streaming-storage.md)
  - Retains reusable native blocks through producer access and asynchronous sends.
- [008 — Apply complete fixed responses through H2O's event loop](008-native-fixed-response-exchange.md)
- [009 — Publish complete responses through preallocated native slots](009-preallocated-response-completion-ring.md)
  - Sends fixed-response bodies directly from slots retained until native cleanup.
