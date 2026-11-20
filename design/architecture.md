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
No upcall lets a Throwable return to native code, since that ends the JVM. Each upcall catches and logs it. The request callback then answers 503, the body callback fails the request body so that the handler's next read throws an IOException and H2O answers 502 or resets the stream, the TLS lookup fails the handshake, and the other callbacks return. A failed accept gives back what it took and stops reading that listener until the next loop turn. A worker fails when its event loop throws, or when its accepts fail for one second with no accept completed in between (an accept refused at the connection limit does not count as completed).
Lifecycle work uses platform threads because it can wait for threads and native resources to finish.

A worker with no work sleeps in `h2o_evloop_run`, for at most 1 ms while requests wait in a dispatch queue. With live requests and a positive `:worker-hot-poll-micros` (default 0), it first polls for up to that budget and runs a zero-wait native turn every 5 microseconds.

### Request dispatch

With `:request-threads :per-request` (default), each request starts a new virtual thread through the generation's request executor. With `:reuse-within-connection`, `ol.busker.request-dispatch` runs the quick requests of one connection one after another on one virtual thread, the drainer. A drainer runs only the requests of one connection, keyed by H2O's connection id. The worker offers a task before it reads the drainer's active flag, and a drainer clears the flag before its last poll, so every queued task has a drainer that polls after it. A drainer whose last poll finds the queue empty while the worker moves queued requests to new threads waits for the move to end and polls again. After a drainer has run for 100 microseconds, new and queued requests of its connection get new threads, so a request waits in the queue for at most about 1 ms plus one loop turn, counted from when its drainer starts running. A drain state goes back to the free list only when no drainer runs or starts on it and its queue is empty. The table holds at most one state per open connection of the worker, plus one per closed connection whose handlers still run. If starting a drainer throws, the worker undoes the start, clears the active flag and fails the request as the default mode does. If an older drainer already took the request, the request runs on that drainer and does not fail. A task that throws goes to the thread's uncaught-exception handler, and the drainer continues. Before the executor shuts down, stop waits up to 1 s in total for the workers to give every queued request a new thread. When the wait runs out, the queued requests of a worker that has not done so run one after another on their drainer. After a stop begins, each new request goes to the executor directly. Before `shutdownNow`, stop makes the drainers interrupt themselves before each queued task. A request that arrives after the shutdown gets `CLJ_HANDLER_SHUTTING_DOWN`, which the shim answers with 503. Between tasks a drainer clears the interrupt flag and resets nothing else bound to the thread. Busker binds nothing to the thread per request.

### Accepting connections

Every worker reads a duplicate of each TCP and Unix socket listener. A worker reads its listeners only while connections are below the global `:max-connections` limit and its connection count is not above the smallest count of its generation. One accept callback checks the minimum at its start, then takes up to 32 queued connections, each while connections are below the limit and the generation is not stopping. Workers wake each other when the minimum rises or connections drop below the limit. Connections never move between workers. A failed worker does not count toward the minimum.

Each worker has a UDP socket with `SO_REUSEPORT` for each HTTP/3 listener. The worker that reads the first Initial of a new flow forwards it to the accepting worker of the active generation with the fewest HTTP/3 connections and pending flows, or keeps it when it is that worker. A table of 64 entries sends the flow's later Initial and 0-RTT datagrams to the same worker for 3 s. While all entries are live, workers keep new flows. Datagrams with a connection ID issued by a worker of any generation go to that worker. A worker that stops accepting is not chosen, and a worker whose thread failed is not chosen again, also after a later activation. If the first forward fails because the chosen worker's channel is full or gone, the deciding worker accepts the flow and keeps its later datagrams. A later datagram that meets a full channel is dropped and the client sends it again. An HTTP/3 connection stops counting toward the balance when it enters its closing or draining period, or, if it ends without that report after an idle or handshake timeout, when it is destroyed. It keeps its slot in the global connection limit until it is destroyed.

On Linux, when a connection whose datagrams arrive forwarded completes its handshake, its worker opens a UDP socket bound with `SO_REUSEPORT` to the connection's local address and connected to the client, so the kernel delivers that client's datagrams directly. A listener has at most 64 connected sockets over all workers and generations. While all are in use, a worker keeps every new flow it reads. A connected socket follows client migration and closes with its connection, with its worker's disposal, or when retirement keeps the context of a failed worker. A failed system call stops new attempts on that worker for one second. A connection gets no connected socket when its local address is a wildcard, multicast, broadcast or IPv6 link-local address, or when its peer is an IPv6 link-local or IPv4-mapped address.

### Native write completion

Workers submit responses on the event-loop thread, also between calls to `h2o_evloop_run`, with at most one active native send per response. This needs the H2O repair of [issue 3828](https://github.com/h2o/h2o/issues/3828), `link_to_statechanged(sock)` before `on_write_complete`, which the pinned h2o-zig package applies as a patch. `test_socket_write_completion.c` fails without it. After a turn that handed output to H2O between runs, the worker polls without waiting, because H2O's poll wait does not count completion callbacks already pending. The native tests cover this on Linux only. No test covers kqueue, delayed zerocopy reclamation or asynchronous TLS.

## Requests

The shim calls Clojure request callbacks with native request context data.
Busker copies request metadata into JVM memory before the native callback returns.
The handler thread decodes that copy when it reads the request, so decoding does not depend on the native request remaining valid.
Reading a key that the decoded Ring request never has, such as `:ol.busker/entrypoint` or a key added by middleware, does not decode it.
All listeners share one hostconf and one handler. Each entrypoint has a `clj_entrypoint_hosts_t` record whose hosts array the accept contexts of its listeners use, so the request callback finds the entrypoint from `req->conn->hosts` on every protocol. A request without a known entrypoint is not dispatched: the shim answers 500 (`CLJ_HANDLER_ERROR`), and Debug builds assert.
The metadata also carries H2O's connection id, which one process-wide counter gives out and never reuses. A connection address can be reused while a handler of the closed connection still runs.
Streaming request bodies use a `java.nio.channels.ReadableByteChannel` and native proceed callbacks.
Callback-dispatch maps module and request sequence identifiers to live requests until cleanup.
Request cleanup frees the request's fixed slot, removes the request from the receiver table and clears its native callbacks at once. The JVM gets the cleanups of an event-loop turn or ring drain in batches of at most 64, before the turn returns to Java and before any request, body, proceed or stop callback. A cleanup outside a turn or ring drain, or during one of those callbacks, reaches the JVM at once.
Each request keeps its generation's configuration and references its worker. Streaming writers take the block pool and command services from that worker.

## Responses

Internally there are two paths for responses to take: streaming responses or fixed-final.
A returned Ring response takes the fixed-final path when nothing was written to its emitter, its status is 200 to 999, its head fits 64 header pairs and 16384 staging bytes, and its body is nil, a byte array, a ByteBuffer, or a string whose encoding fits the fixed body limit, the smaller of `:output-buffer-size` and `:fixed-response-max-body-size`.
This is a perf optimization so small requests can be sent in one clj->h2o operation.

Other responses stream. A head that does not fit a slot streams and logs `:ol.busker.response/large-head` at most once a minute per generation.

### Fixed-final responses

The fixed-final path avoids the queue, writer, and chunk-handling overhead for small responses whose complete body is already available.
The term comes about from:

- fixed: the complete body and its byte length are known before sending
- final: It completes the response and no new body chunks will follow, that is, it is not an interim response such as `100 Continue`

The fixed-final path uses preallocated slots to reduce per-response allocation and pass complete responses from handler threads to the event-loop worker without waiting for native I/O.
Each worker has `:fixed-response-slots` `clj_response_slot_t` slots in a native ring. A generation builds its slots and limits from the config it started with, so a request always uses the limits of its slots, also after a reload.

When a Ring handler returns a response, Busker claims a slot and copies the headers and body into it.
It publishes the slot state and then its bit in a ready bitmap, and wakes the worker. The worker detaches each bitmap word before it processes the bits. H2O reads the body from the slot directly. The slot stays valid until native request cleanup, also after the final send returns. Cleanup checks the claim token before the slot is reused, and a slot being filled survives a disconnect until its producer returns. A claim reads the worker's count of free slots first and does not scan the slots when that count is zero. When all slots are in use, a blocking caller parks on a lock-free queue and a nonblocking call is rejected. Each freed slot wakes one parked caller. The event-loop thread takes no Java monitor to wake it, but request cleanup can still take the locks of the responses it closes. Closing a response wakes the caller that waits for that response. Closing receiver admission wakes every parked caller. No other transport takes the response.

An `emit!` producer with `:end? true` on an emitter without earlier output also uses a slot when its capacity is at most the fixed body limit, its head fits, the method is not HEAD and the status is not 204, 205 or 304.

### Content length

On both paths the content length is the handler's `content-length` header, or else the body's length. A HEAD or 304 response with a nil body carries none unless the handler sets one. A 204 response never carries one and a 205 response carries 0. HEAD, 204, 205 and 304 responses send no body bytes. A `content-length` that is not a non-negative integer, or that differs from a known complete body length, makes the response invalid. Busker never corrects it. Statuses 204, 205 and 304 and a HEAD response with an empty body are not checked. A string or sequence body in a charset that Java can name but not encode is also invalid. An `emit!` producer on a fixed slot whose output differs from the head's content length throws before publication.

### Streaming responses

Streaming responses use a writer and an internal receipt. Each worker has 256 native blocks of `:output-buffer-size` bytes (B) in a shared FFM arena, allocated on a platform thread. The worker sends block descriptors without copying payloads. Each response admits one producer at a time. A bounded write reserves bytes, a block, a receipt and a mailbox permit before it runs application code. Reserved permits take no mailbox FIFO position. Acceptance registers the output record and receipt atomically against closure, and accepted records stay reachable until publication or explicit discard. A producer callback writes through a confined view that closes before publication. Neither close nor native cleanup waits for a callback while holding a lock.

Each limit has a separate release event. Pending byte credit is B per response and returns when records enter a native send. A response has at most 64 unsettled receipts, and receipt credit returns on settlement. Blocks stay checked out through native use: non-final proceed releases them, and a final send keeps them until request cleanup. A write that waits for a block parks on a lock-free queue, and each released block wakes one waiter. A response that registers unflushed output also wakes one waiter, which then tries again. Closing a response wakes the write that waits for that response. Pool retirement and worker failure wake every waiting write. A response has at most one native send, of at most B bytes. Native stop settles receipts but does not release a live send.

When the final head declares a content length and the response sends a body, the worker counts the bytes it sends, and does not send a batch that passes it or a final send that falls short of it. In both cases the response fails.

Flush and end are coalesced per-response state, not mailbox entries, and apply only after their accepted prefix has arrived, so they cannot overtake an accepted head. Empty body writes that do not end the response are ordered local receipt markers, not empty native sends. Close captures only the accepted prefix, settles pending receipts at once and dispatches close callbacks on virtual threads. A source that returns after close discards its private bytes. Failure after begin acceptance terminates the response and returns the accepted receipt. Workers process bounded mailbox turns and one ready batch per response before polling. When H2O calls proceed during the send, the worker sends up to 256 batches of that response in the same turn. Unflushed output does not make the worker poll continuously.

The shim turns off H2O's `http1.req_io_timeout_while_generating` unless `:http1-req-io-timeout-while-generating?` is true. The user docs describe the effect on each protocol.

If sending a returned Ring response throws an invalid response while its emitter is open and before a final head is accepted, Busker logs it and sends 500. Any other failure while the emitter is open requests an error-final send, with a minimal status-200 head when no final head has started. H2O then closes an HTTP/1.1 connection and resets an HTTP/2 stream. H2O ends an HTTP/3 stream normally on an error send, so the shim resets it with `H3_INTERNAL_ERROR` right after. An application failure checks admission and requests abort in one queue-lock transition, so it does not change output that `close!` or an accepted `:end? true` already ended. Publication, worker and native-send failures discard pending output. Failure and native stop settle pending receipts without changing earlier results or releasing storage in native use.

## Lifecycle

A generation is a running set of workers, listeners, and native resources.
During reload, Busker starts a new generation to accept new connections while the previous generation finishes work on its existing connections.
This lets configuration changes take effect without immediately interrupting active requests.

A reload with an unchanged configuration returns `:unchanged`, unless a worker of the active generation has failed. A reload publishes the new generation and waits up to 60 s until each worker of the previous one has closed its listener sockets or failed. It returns `:activated` either way and retires the previous generation in the background. A retry with the same configuration after that timeout returns `:unchanged`. A server stop drains every generation, stops each certificate runtime and each in-memory ticket service once, and ends stopped even when a generation cannot be retired. It then throws the first error of a retirement or a service stop, with others suppressed, and a later stop throws the same error. A start or reload that fails before it publishes the new generation stops what it started and throws, with the errors of these stops suppressed. Server start, reload and server stop are a pure core, `ol.busker.lifecycle.runtime`, that returns each action as data. `ol.busker.runtime` performs the action and gives the outcome back to the core. The lifecycle thread and the drain threads write the runtime state only with `swap!` and pure functions of the core.

The previous generation stops accepting new connections but must still let its handlers send responses.
Its workers are told to stop accepting before its native handler starts to answer new requests with 503.
A worker that is in an accept at that moment can still take one more connection.
Busker keeps its response queues and native memory available until that work finishes or the shutdown timeout is reached.
A timeout does not make memory safe to free. Receiver admission closes first. Producers and native requests that entered before keep their generation until publication, discard or cleanup. Failed retirement reports the failure and keeps the affected arenas and callback stubs.

When a worker fails, `:on-native-failure!` logs it, stops the worker accepting, settles its live responses and ends its loop. The worker counts as stopped accepting at once. When the generation stops and the failed worker's thread has ended, the stopping thread takes, once each, the stop steps that the worker did not take. If the failed worker left connections, responses or blocks, or its stream pool cannot retire, retirement keeps the generation's loops, contexts, receivers, HTTP/3 worker contexts and memory. It closes the TCP and Unix socket listeners, releases the TCP and Unix socket claims, closes the UDP sockets of the kept HTTP/3 contexts and records the generation as kept, in that order. Each closed UDP listener socket is replaced by an unbound socket, because H2O closes it when the context is freed. The UDP claims stay. Kept connections keep their slots in the connection limit. The other workers dispose their contexts once their connections close, so the kept report comes when they end, not at the timeout. Retirement reports a timeout only when a worker thread is still alive at the deadline.

Threads that publish a response or mailbox command call the native wake themselves, at most once per worker sleep. At context disposal the worker quiesces its wake endpoint, waits for wake calls in progress, closes the endpoint and unregisters the receiver before it frees it. A wake on a closed endpoint does nothing.

A start acquires the native resources of a generation one node at a time, in the order of `acquire-order` in `ol.busker.lifecycle.generation`, where each node comes after the nodes it needs. Each acquire completes, or frees what it made and throws. An acquire that cannot free what it made keeps that node and what it needs, and the generation is recorded as kept. If one fails, Busker releases what the start acquired by the rules of a stop and throws the first error, with the errors of that release suppressed. Contexts without workers also have their wake and response receivers unregistered before context disposal. Cleanup cannot continue through dependent native resources when their safe release is uncertain. Such resources stay allocated and the generation is recorded as kept, because an error or timeout is not evidence that native callers have finished. A native release that throws is not tried again, and the report of the kept generation says that a release threw.

### HTTP/3 packet forwarding

The shim forwards QUIC packets to the worker or generation that the connection ID selects, and the first datagrams of a new flow to the worker chosen for it. Each worker reads forwarded packets from a socketpair. The shim honours forwarding envelopes only there, never on the public or a connected UDP socket.
The shim drops packets with a zero forwarding TTL and decrements TTL in each forwarded envelope.
This is an internal hop count, not the IP packet TTL; it prevents forwarding loops during routing and reloads.
On receipt, the shim caps TTL at the context's configured default and rejects envelopes without an IPv4 or IPv6 source address.
These checks are performed by Busker's callbacks, as required by the current libh2o callback contract.
Workers write to each other's socketpairs without a lock, and find workers in an attached-worker list that is replaced, never changed. A register, unregister or activation replaces the list and frees the old one, and a departed worker's record, only after every worker that used the old list has left it. Each worker marks the stretch in which it uses the list in a counter that only it writes, and the flow table keeps worker ids, not pointers. If the shorter list cannot be allocated, the departed worker stays in the current list with its write end marked closed, so it is neither chosen nor written to, and its record is freed with the next list. A listener has one list and one record per attached worker, however many reloads it survives.
A forwarded datagram stays in H2O's receive batch as a one-byte stub that fails to decode. When the callback drops a datagram, H2O processes the datagrams grouped before it in the batch with the dropped datagram's addresses. A failed decryption overwrites the packet in place, so the shim forwards an Initial with another worker's connection ID before H2O reads it, and the accept callback reports a failed decryption to H2O as no connection.

### HTTP/3 key exchange

The HTTP/3 TLS context offers x25519, secp256r1 and secp384r1, in that order. With secp256r1 alone, clients that send an x25519 share need a HelloRetryRequest, and picotls does not resume a session after one. TCP listeners use BoringSSL's default groups. The HTTP/3 context resumes a session only with a new key exchange (psk_dhe_ke). A client that offers psk_ke alone gets a full handshake.

### Early data

`:ol.busker/early-data?` comes from `h2o_conn_is_early_data`. HTTP/3 listeners accept up to 8192 bytes of 0-RTT data. TCP listeners accept no early data, because Busker does not call `SSL_CTX_set_early_data_enabled`. `h2o_conn_is_early_data` reads only picotls state, so before TCP early data is turned on, the flag must come from BoringSSL's `SSL_in_early_data` or `SSL_in_init`.

### QUIC address tokens

Each session ticket key yields an AES-128-GCM address-token key, as in h2o's server: the SHA-256 digest of its cipher and HMAC keys, expanded with the HKDF label `address-token`. A token starts with the first byte of its ticket key's name. The ticket key service keeps that byte unique among the kept keys. New tokens use the newest key, so token keys rotate, expire and survive reloads as ticket keys do. Each HTTP/3 worker has cipher contexts for every ticket key, used only on its loop, and rebuilds them when the ticket key generation changes.

A resumption token from the same IP address and at most ten minutes old validates the client's address. Other tokens leave it unvalidated, and a token that no kept key decrypts is ignored. A Retry-type token whose first byte names a kept key but that does not decrypt gets INVALID_TOKEN. Busker never sends Retry. With session tickets disabled, workers have no token keys, ignore presented tokens and send NEW_TOKEN frames with random bytes, because quicly offers no way to skip a requested token.

## Related decisions

- [ADR 001](adr/001-response-emitter-close-callbacks.md): virtual-thread close callbacks.
- [ADR 002](adr/002-worker-affine-ffm-callback-dispatch.md): worker-affine callback identities and lifetime.
- [ADR 003](adr/003-coalesce-worker-mailbox-wakeups.md): mailbox permits, ordered controls and wake coalescing.
- [ADR 006](adr/006-worker-side-response-head-staging.md): worker-side streaming header staging.
- [ADR 007](adr/007-bounded-native-streaming-storage.md): bounded native streaming blocks.
- [ADR 009](adr/009-preallocated-response-completion-ring.md): fixed-response slots kept until cleanup.
- [ADR 010](adr/010-direct-native-wake-from-publishing-threads.md): native wakes from publishing threads.
- [ADR 011](adr/011-share-virtual-threads-within-a-connection.md): opt-in sharing of virtual threads among the quick requests of one connection.
