---
status: superseded
---

# Apply complete fixed responses through H2O's event loop

**Superseded by [ADR-009: Publish complete responses through preallocated native slots](009-preallocated-response-completion-ring.md).**

Trillium's measured gains came from coarse response outcomes and transport-side request state, but its platform-thread handlers do not satisfy Busker's virtual-thread contract. Busker will instead keep Ring handlers on virtual threads, admit complete fixed responses to a bounded Java queue, stage them on a dedicated platform thread, and send deep-copied messages to an H2O multithread receiver. H2O validates the scalar request identity and applies each live response on its event-loop thread.

This exchange optimizes only complete fixed responses. Streaming continues through `Start`, `Data`, `End`, and `Abort` operations on the existing path, with H2O `proceed` controlling flow, hard byte limits, and request cleanup cancelling producers and releasing chunks. A request that emitted an informational response also keeps its final response on the existing FIFO worker mailbox. Both paths retain the existing emitter retirement process, which dispatches ordered `on-close` callbacks exactly once on virtual threads.

The dispatcher drains accepted commands before shutdown. H2O destroys the response receiver only after the dispatcher stops, pending native messages reach zero, connections drain, and request cleanup removes every scalar identity. Stale messages are discarded without reading retired request memory.
