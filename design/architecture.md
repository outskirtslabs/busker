# Busker architecture

Busker is a Clojure HTTP server built on [libh2o] through the Java Foreign Function and Memory API.
This document describes its execution, response-storage and lifecycle contracts. The [ADRs](./adr/README.md) describe individual design decisions.

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

When a worker finds no mailbox work and no new wake, it waits in three steps. If it has live requests, it first polls for up to `:worker-hot-poll-micros` (default 50, 0 disables the poll), spinning on its wake generation and mailbox count and running a zero-wait native turn every 5 microseconds so sockets keep being serviced. Work found by the poll is handled at once. When the budget runs out, or when the worker has no live requests, it arms sleep and rechecks for work, then sleeps in `h2o_evloop_run` until a native wake or I/O ends the sleep.

### Native write completion

Workers submit responses on the event-loop thread, including between calls to `h2o_evloop_run`, while preserving one active native send per response. This requires completion delivery to record socket-interest reconciliation before the next poll. Otherwise an earlier update can retain write interest for an immediately completed write whose callback is then cleared.

`shim/build.zig` applies the local repair tracked in [H2O issue 3828](https://github.com/h2o/h2o/issues/3828): `run_socket` calls `link_to_statechanged(sock)` before `on_write_complete`. Recording the update before the callback permits rearm or close without accessing the socket afterwards. The build checks the pinned header's hash and a unique replacement site, generates a private header and `socket.c` copy, and substitutes that translation unit in the actual `h2o-evloop` artifact for every target and native test. Dependency sources and pins remain unchanged. The patch can be removed once the pinned H2O includes the repair. The exact between-loop submission phase is not yet an upstream-confirmed API contract.

Normal shim builds run a Linux regression covering the stale-interest sequence, callback rearm, close and write errors. TCP/TLS stalled-reader tests check exact response bytes and backpressure. Cross-compilation does not establish kqueue runtime coverage; delayed zerocopy reclamation and asynchronous TLS paths require separate verification.

## Requests

The shim calls Clojure request callbacks with native request context data.
Busker copies request metadata into JVM memory before the native callback returns.
The handler thread decodes that copy when it reads the request, so decoding does not depend on the native request remaining valid.
Streaming request bodies use a `java.nio.channels.ReadableByteChannel` and native proceed callbacks.
Callback-dispatch maps module and request sequence identifiers to live requests until cleanup.
Each request retains the generation configuration unchanged and references its worker. Streaming writers read the block pool and command services from that worker, passing the worker explicitly to each service.

## Responses

Internally there are two paths for responses to take: streaming responses or fixed-final.
A Ring response returned by a handler is sent down the fixed-final path when nothing has been written to its emitter, its status is 200 to 999 other than 204 and 304, and its body is a byte array or a string whose response charset is UTF-8, of at most `:output-buffer-size` and 32768 bytes.
This is a perf optimization so small requests can be sent in one clj->h2o operation.

Other supported bodies use the streaming path, which sends body data in chunks with backpressure to prevent the application from producing data faster than the client can read it. Every response head, including streaming and informational heads, must fit 64 header pairs and 16384 staging bytes. Excess headers cause an exception before acceptance; streaming does not increase header capacity.

### Fixed-final responses

The fixed-final path avoids the queue, writer, and chunk-handling overhead for small responses whose complete body is already available.
The term comes about from:

- fixed: the complete body and its byte length are known before sending
- final: It completes the response and no new body chunks will follow, that is, it is not an interim response such as `100 Continue`

The fixed-final path uses preallocated slots to reduce per-response allocation and pass complete responses from handler threads to the event-loop worker without waiting for native I/O.
The slots are `clj_response_slot_t` structs maintained in a ring-buffer in native memory.

When a Ring handler returns a response, Busker claims a slot, copies the headers and body into it.
It publishes the slot state, then its index in a capacity-sized ready bitmap, and requests a worker wake.
The worker detaches each bitmap word before processing its set bits, so publication during a drain remains discoverable without scanning every slot.
The slot keeps those bytes valid until native request cleanup, including after a final send returns. H2O reads the body directly from the slot; only headers are copied into its request pool. Cleanup detaches the slot reference and checks its claim token before making the slot reusable. Slots being filled survive concurrent disconnects until their producers return.

An `emit!` producer with `:end? true` on an emitter without earlier output also uses a slot, through a confined ByteBuffer view, when its capacity is at most 32768 bytes, the request method is not HEAD and the status is not 204, 205 or 304. Ring returns on this path use no streaming controller or receipt. Slot exhaustion parks a blocking caller or rejects a nonblocking operation; it does not select a second copying transport.

### Streaming responses

The streaming path handles incremental bodies, supported bodies beyond the fixed-body limits, responses that already sent informational headers, and Ring returns the fixed path does not take, such as nil bodies, 204 and 304 statuses, ByteBuffers and strings in other charsets. Those Ring returns create a streaming writer and an internal receipt. A platform thread allocates 256 native blocks per worker in a shared FFM arena. Each block has `:output-buffer-size` bytes (B). This reserves 8 MiB per worker at the default B=32768, separately from fixed slots. The worker sends descriptors for these blocks without copying their payloads. Blocking strings and ByteBuffers larger than B are encoded or copied straight into successive blocks; byte arrays are copied in B-sized pieces.

Each response admits one producer at a time. A bounded write reserves bytes, a block, a receipt and a mailbox permit before invoking application code. The callback runs synchronously on its calling virtual thread through a confined view of the shared block. Closing that view revokes application access before publication without freeing the backing storage. Neither close nor native cleanup waits for a callback to return while holding a lock.

Acceptance registers the output record and receipt atomically against closure. Reserved mailbox permits do not occupy FIFO positions: a slow private producer cannot prevent the worker from consuming published commands. Accepted records stay reachable until publication or explicit discard. The worker copies descriptors, not payload bytes, when combining native blocks into a send.

Three separate limits have separate release events. Pending byte credit is B per response and returns when records enter a native send. Each response has at most 64 unsettled receipts; receipt credit returns on settlement. Physical blocks remain checked out through native use. Each response has at most one native send, containing at most B bytes. Non-final proceed releases its blocks; final sends retain theirs until request cleanup. Native stop settles receipts but cannot release a still-live send.

One logical incremental write can publish a begin record, several data records and an EOF marker while retaining one receipt. Close captures only the accepted prefix, not future bytes or EOF from a blocked input source. A source that returns after close discards private bytes. Failure after begin acceptance terminates the response and returns the accepted receipt rather than reporting that no output was accepted.

Flush and end requests occupy coalesced per-response state, not mailbox entries. Each captures an accepted sequence prefix. The worker waits until that prefix has arrived before applying the control, so a control cannot overtake an accepted but unpublished head. Empty body writes are ordered local receipt markers, not empty intermediate native sends. Pressure flushes previously accepted output; global pool pressure tracks responses with unflushed blocks rather than scanning all requests.

Workers process bounded mailbox turns and one ready batch per selected response before polling native I/O. When H2O calls proceed during the send, so that the response is ready again at once, the worker sends up to 256 batches of that response in the same turn. Proceed can drain directly without another mailbox message, with reentrant callbacks handled by a trampoline. Unflushed output does not make the worker poll continuously. Close settles pending receipts immediately, independently of native storage release, and dispatches ordered callbacks on virtual threads.

If sending a returned Ring response throws while its emitter is open, Busker creates the streaming writer if necessary and requests an error-final send. The worker supplies a minimal status-200 head if no final head has started; this is a transport failure, not a successful empty response. Application failure checks admission and requests abort in one queue-lock transition. If `close!` or an accepted `:end? true` wins first, the accepted bytes and normal final send remain intact, including while awaiting mailbox consumption or native proceed. This also applies to incremental body-source failures. Publication, worker and native-send failures still discard pending output after admission closes: the transport can no longer deliver that prefix safely. A native stop notification records that transport has stopped and suppresses further sends. Failure and stop settle pending receipts without changing earlier results or releasing storage still in native use.

## Lifecycle

A generation is a running set of workers, listeners, and native resources.
During reload, Busker starts a new generation to accept new connections while the previous generation finishes work on its existing connections.
This lets configuration changes take effect without immediately interrupting active requests.

The previous generation stops accepting new connections but must still let its handlers send responses.
Busker keeps its response queues and native memory available until that work finishes or the shutdown timeout is reached.
Reaching the timeout does not make memory safe to free. Receiver admission closes first; previously entered producers and native requests retain their generation until publication, discard or cleanup. A producer that ignores interruption can delay release indefinitely. Failed retirement reports and retains the affected arenas and callback stubs instead of invalidating live addresses.

Threads that publish a response or mailbox command call the native wake themselves, at most once per worker sleep. During context disposal, the event-loop worker directly retires its wake receiver: it quiesces the wake endpoint, waits for native wake calls in progress, closes the endpoint and unregisters the receiver before freeing it. A wake on a closed endpoint performs no native work.

If startup fails partway through, Busker attempts to stop the partial generation and rethrows the original startup error. Contexts without workers also have their wake and response receivers unregistered before context disposal. Cleanup cannot continue through dependent native resources when their safe release is uncertain. Such resources must remain allocated; an error or timeout is not evidence that native callers have finished.

### HTTP/3 packet forwarding

HTTP/3 contexts forward QUIC packets to the worker or generation selected by the connection ID.
The shim drops packets with a zero forwarding TTL and decrements TTL in each forwarded envelope.
This is an internal hop count, not the IP packet TTL; it prevents forwarding loops during routing and reloads.
On receipt, the shim caps TTL at the context's configured default and rejects envelopes without an IPv4 or IPv6 source address.
These checks are performed by Busker's callbacks, as required by the current libh2o callback contract.
Native socketpair regression tests cover these rules and run during normal shim builds.

## Related decisions

- [ADR 001](adr/001-response-emitter-close-callbacks.md): virtual-thread close callbacks.
- [ADR 002](adr/002-worker-affine-ffm-callback-dispatch.md): worker-affine callback identities and lifetime.
- [ADR 003](adr/003-coalesce-worker-mailbox-wakeups.md): mailbox permits, ordered controls and wake coalescing.
- [ADR 006](adr/006-worker-side-response-head-staging.md): worker-side streaming header staging.
- [ADR 007](adr/007-bounded-native-streaming-storage.md): bounded native streaming blocks.
- [ADR 009](adr/009-preallocated-response-completion-ring.md): retained fixed-response slots.
- [ADR 010](adr/010-direct-native-wake-from-publishing-threads.md): native wakes from publishing threads.

