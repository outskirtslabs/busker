# ADR-004 Platform notifier for native wakes

## Status

Superseded by [ADR-010](010-direct-native-wake-from-publishing-threads.md)

## Context

Ring handlers, middleware, response producers, and request-body readers run on virtual threads. H2O request operations run on a dedicated event-loop platform thread. The worker mailbox transfers request-affine operations between those threads and revalidates each request immediately before native use.

The mailbox limits command count, coalesces command wakes, preserves FIFO order, and closes the empty-drain race. Streaming responses separately limit bytes and send one native batch at a time.

H2O's `clj_h2o_mt_wakeup` path acquires its multithread-queue mutex and writes to a nonblocking eventfd or pipe. A virtual thread remains mounted while a foreign function runs, so mutex or scheduler delay can retain its carrier. Busker therefore performs native wakes on a platform notifier thread.

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

The event-loop worker retires its wake receiver directly during context disposal. It moves the endpoint from open to quiescing, waits for any in-flight notifier call, closes the endpoint, unregisters and frees its receiver, and clears the receiver reference. Stale queued tokens observe the closed endpoint and skip native work. Runtime shutdown stops and joins the shared notifier only after all generations retire.

The notifier works with:

- one FIFO worker mailbox;
- the mailbox signal-generation protocol;
- fixed-response slot publication and worker-side draining;
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

## Consequences

Application request paths use Java operations to request event-loop wakes. One platform notifier thread per `SharedRuntime` performs native mutex and eventfd work.

An idle wake gains one Java queue handoff. Under sustained load, the armed check should avoid most notifier and native wake work because the event-loop worker is already active. The net throughput effect remains a measurement question.
Reload keeps one notifier while generations overlap. Endpoint state and in-flight counts prevent an old queued token from reaching a retired receiver.

The notifier carries wake requests only. Java mailboxes and response slots carry application output; H2O's native message list carries neither commands nor payload bytes.
