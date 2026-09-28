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

Each worker tracks streams with pending data or a send in progress, instead of checking every active request for streaming work.

## Lifecycle

A generation is a running set of workers, listeners, and native resources.
During reload, Busker starts a new generation to accept new connections while the previous generation finishes work on its existing connections.
This lets configuration changes take effect without immediately interrupting active requests.

The previous generation stops accepting new connections but must still let its handlers send responses.
Busker keeps its response queues and native memory available until that work finishes or the shutdown timeout is reached.
Reaching the timeout does not make memory safe to free: Busker must first stop any thread or native operation that could still use it.

If startup fails partway through, Busker releases the resources it has already created.
Cleanup continues even if one release operation fails, and the original startup error is retained along with any cleanup errors.


## Local optimization tooling

[Tempo](../swamp/README.md) is a development-only Swamp workflow, separate from the
server runtime. It prepares isolated Git worktrees, runs `bb qa` and serial
`bb bench:local` measurements, and records source and native-library identity.
The local benchmark classpath puts shim resources ahead of native copies in
`target/classes`. Diagnostic recordings cannot supply benchmark scores.
The current fixed measurement method pins the entire benchmark coordinator and
server JVM, including GC, JIT, and native threads, to physical CPUs 10–11. Warmup
and measured h2load clients run on disjoint physical CPUs 12–13. The same
allocation applies to H1, TLS H2, candidate measurements, and profiling;
Pi, QA, the controller, and REPL are not pinned. Before a run, the coordinator
checks its inherited allowed CPU set through a child process and checks host
core and cache topology. The child check does not prove the coordinator's or
Java's live thread masks. During every sample the JVM records its thread masks
at readiness and at the start and end of measurement. For every warmup and
measured h2load invocation, the wrapper verifies the launched executable and
records the task masks visible under its live PID after exec.
These records are point-in-time observations, not proof of continuous CPU
residence. Missing or inconsistent sample evidence invalidates the measurement.
Method identity, topology, and resolved h2load path are saved with each result
and checked when assembling comparisons. Older unpinned results cannot serve
as new H1/H2 anchors or as parent evidence for the fixed method.

Each scored cell collects two complete batches of the configured three or six
valid samples before a precision decision. The arithmetic
mean is the score only when the two-sided Student-t interval has a relative
half-width no greater than the configured limit (by default 5% at 95%
confidence). Invalid execution and insufficient remaining time may stop
collection early, but a precise first batch does not.
Run config, benchmark records, and comparisons persist the precision settings,
fixed sample count, and `arithmetic-mean-student-t-v1` identity. Parent imports
reject missing or different estimator evidence; old median measurements are not
reclassified. Candidate improvement, H2 protection, and target decisions still
compare point estimates and do not prove a difference is statistically significant.
An imprecise baseline stops the campaign. An imprecise candidate cannot be
scored or accepted; Tempo retains the recorded samples, discards the candidate,
and starts another attempt from the verified best if limits permit. Other
invalid measurements still stop the campaign.
The separate unscored repeatability procedure applies the same configured
six-sample calculation to both protocols and records version, precision, and
sample count; its report applies to a later campaign only when those settings
and the recorded source, native library, and method match the intended run.

Swamp executes official commands and Git operations. Restricted Pi processes
receive exact saved inputs and file tools; implement and repair may also use a
dedicated experiment-worktree REPL through `brepl` for namespace reloads,
evaluation, and focused tests. Swamp stops that REPL before QA or measurement;
REPL feedback cannot replace official checks. The untracked project `AGENTS.md`
is saved once per run with its contents and SHA-256 as read-only inference
guidance, separate from the reviewed Git support snapshot.
For review, Swamp saves the raw Git diff from the best commit to the tested
candidate commit as a read-only file and archives its bytes with both revisions
and a SHA-256 digest. The reviewer can read that file without Git access.
An edited revision must pass tests, fresh measurements, and review before replacing
the best revision. H1 acceptance also checks TLS H2 against the initial baseline.
Run limits are checked between phases; unfinished edits and raw evidence are kept
separately. This tooling does not change Ring behavior, virtual-thread execution,
streaming flow control, or server lifecycle rules.
A supervised run loads its model from a private, commit-derived Swamp snapshot and
checks a saved file manifest before and after each effect. Scoped child commands
append directly to private logs, so stopping the separate controller cannot lose
flushed output held in its process pipes. Ambiguous interrupted steps require
inspection rather than automatic replay.
A fresh campaign may cite a stopped parent's exact-version Swamp export. Tempo
checks its hashed config, step results, raw measurement samples, passing tests,
review, retained revision, support snapshot, and remaining cumulative time,
attempts, and Pi calls. An interrupted parent additionally needs a leader-approved
receipt in its private run directory and unchanged partial Pi logs. That receipt
disables recovery of the old run; no unresolved effect is replayed. A new run
remeasures the retained revision before further edits, keeps the original H1/H2
baseline and the highest verified best-revision H1 score, and refuses missing or inconsistent proof.

## Decision record reconciliation

The [ADRs](adr/README.md) record decisions at the time they were made.
This document combines those decisions with later changes to describe the current design.
Where an older ADR differs from this document, use this document for current behavior and the ADR for historical context.

[ADR 009](adr/009-preallocated-response-completion-ring.md) supersedes [ADR 008](adr/008-native-fixed-response-exchange.md): preallocated native slots replace its queued, deep-copied response transfer.
This change does not replace the worker-affine callback dispatch in [ADR 002](adr/002-worker-affine-ffm-callback-dispatch.md), the mailbox ordering in [ADR 003](adr/003-coalesce-worker-mailbox-wakeups.md), or worker-side response-head staging in [ADR 006](adr/006-worker-side-response-head-staging.md).
[ADR 004](adr/004-platform-notifier-for-native-wakes.md) changes how workers are woken: a platform notifier makes the native wake calls, while the mailbox wake coalescing from ADR 003 remains in use.
The later shared header serializer serves both fixed-final responses and response-head staging; that consolidation is not part of ADR 009's original decision.

