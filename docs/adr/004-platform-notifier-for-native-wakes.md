# ADR-004 Platform notifier for native wakes

## Status

Accepted

## Context

Ring handlers, middleware, response producers, and request-body readers run on virtual threads. H2O request operations run on a dedicated event-loop platform thread. The worker mailbox transfers request-affine operations between those threads and revalidates each request immediately before native use.

The mailbox already limits command count, coalesces command wakes, preserves FIFO order, and closes the empty-drain race. Streaming responses separately limit bytes and send one native batch at a time.

The first producer in a mailbox signal batch still calls `clj_h2o_mt_wakeup` through FFM. Direct streaming signals call the same function. H2O's wake path acquires its multithread-queue mutex and writes to a nonblocking eventfd or pipe. A virtual thread remains mounted while a foreign function runs, so mutex or scheduler delay can retain its carrier.

The event-loop worker cannot wake itself while it is sleeping inside `h2o_evloop_run`. Polling with a short timeout would consume CPU or add response latency. H2O's multithread queue has no command or byte limit and does not replace Busker's Java backpressure.

## Decision

Each `SharedRuntime` creates one platform wake notifier shared by active and draining generations. Application virtual threads publish commands and wake state using Java operations only. They do not call the native H2O wake function.

Each worker stores:

- a monotonically increasing Java wake generation;
- an armed flag that is true only while the worker can sleep in `h2o_evloop_run`;
- one preallocated notifier endpoint with a pending-token flag and the worker's receiver reference.

A wake request increments the worker's Java generation. If the worker is armed and its endpoint has no pending token, the producer offers that endpoint to the shared notifier queue. The queue is logically limited by registered endpoints because each endpoint can publish at most one pending token.

Before a normal native wait, the worker arms itself and rechecks its mailbox and Java wake generation. New work found by the recheck prevents the wait and runs the next native pass with zero wait. This sequence prevents work published immediately before sleep from becoming stranded.

The notifier clears an endpoint's pending-token flag before calling `clj_h2o_mt_wakeup`. Work published during that foreign call can therefore enqueue another token. The notifier catches wake failures, retries the admitted endpoint once, reports a persistent failure, and does not claim successful delivery.

The H2O receiver remains a wake-only receiver. Commands, response bytes, and Java request references do not enter H2O's multithread message list.

Generation retirement submits a control command while the endpoint is open and wakeable. The event-loop worker handles that command by moving its endpoint from open to quiescing, waiting for any in-flight notifier call, closing the endpoint, unregistering and freeing its receiver, and clearing the receiver reference. Stale queued tokens observe the closed endpoint and skip native work. Runtime shutdown stops and joins the shared notifier only after all generations retire.

The following behavior remains unchanged:

- one FIFO worker mailbox;
- the mailbox signal-generation protocol;
- `FixedFinalCommand` preparation and execution;
- worker-local request identities and live-request lookup;
- per-request response byte limits;
- direct streaming proceed draining;
- request-body credit; and
- native request operations on the event-loop worker.

## Required verification

Tests must demonstrate:

- no application virtual thread invokes the native wake adapter;
- a worker does not sleep when work appears before or during its arm-and-recheck sequence;
- repeated wake requests produce at most one pending endpoint token;
- work arriving during a notifier downcall can publish another token;
- queued tokens stay logically limited to one per endpoint;
- notifier close drains admitted tokens and rejects later requests;
- endpoint quiescence and receiver destruction run in order on the H2O worker;
- fixed-final, streaming, informational, request-body, stop, reload, and multi-worker behavior remains exact; and
- full validation and controlled large-payload tests pass.

The canonical benchmark must classify the throughput hypothesis. A matching profile is required only if the candidate advances.

## Consequences

Application request paths no longer enter H2O through FFM merely to wake an event loop. Native mutex and eventfd work moves to one platform notifier thread per `SharedRuntime`.

An idle wake gains one Java queue handoff. Under sustained load, the armed check should avoid most notifier and native wake work because the event-loop worker is already active. The net throughput effect remains a measurement question.
Reload keeps one notifier while generations overlap. Endpoint state and in-flight counts prevent an old queued token from reaching a retired receiver.

This decision does not change public Ring behavior or introduce a native command transport. Lifecycle calls made directly by arbitrary virtual threads remain a separate question outside this attempt.
