# ADR-011 Opt-in reuse of a virtual thread within one connection

## Status

Accepted.

## Context

Each worker hands its requests to the generation's request executor, a virtual-thread-per-task executor. Starting a virtual thread from the worker submits a task to the scheduler's shared queue, and the scheduler signals a carrier thread when the queue looked empty and fewer carriers than its parallelism are active. On a machine with many idle carriers, most small requests therefore wake a sleeping carrier, which runs a few microseconds of handler work and sleeps again. On 16 CPUs with 8 workers and HTTP/2 GET requests, carriers slept 0.31 times per request at the default carrier count, and the request rate followed carrier sleeps: 4 carriers gave 0.0008 sleeps per request and 1.40 times the rate, with no resource saturated at the default. JDK 25 has no public API to start a batch of virtual threads with one signal, and a carrier count is not a setting Busker should choose for its users.

Running several requests on one virtual thread gives the scheduler fewer tasks. It also means that anything bound to the thread stays for a later request on it, for example `ThreadLocal` values, the thread's name, context class loader and uncaught-exception handler, its identity (anything keyed by the `Thread` object or its id), a lock a handler left held, and an interrupt or unpark that application code sends to the thread after its request ended; a leftover unpark permit makes the next request's first park return at once. The JDK has no public way to clear a thread's thread-local state. Busker binds nothing to the thread per request, and scoped values cannot carry over. The user guide promises one virtual thread per request. Helidon 4.5.5 and enso 1.0.0-beta138 run all HTTP/1.1 requests of a connection on that connection's virtual thread, clear no thread-local state, and start a new virtual thread for every HTTP/2 stream; neither shares a thread across connections. A version that shared a thread among all requests of a worker carried thread-local state across connections; with two connections on one worker, up to 225 threads per 8,000 requests served both.

## Decision

The server-wide setting `:request-threads` chooses how handlers get threads. `:per-request`, the default, keeps one new virtual thread per request: the request callback hands the task to the executor, and the only addition to that path is one branch on the worker's `:request-dispatch` field, which is nil in this mode. `:reuse-within-connection` lets quick requests of one connection run one after another on one virtual thread. It is opt-in because it breaks the one-thread-per-request promise for thread-local state, which only the application can judge. Sharing is per connection, never per worker, so thread-local state never crosses from one client's connection to another's. The design was chosen over one that shared a thread among all requests of a worker, which carried thread state across connections; a prototype of that version measured 866,840 against 602,509 requests per second for h2-get on 16 CPUs with 8 workers, and this design keeps a similar gain, as the figures below show.

The shim passes H2O's connection id with every request. H2O numbers HTTP/1, HTTP/2 and HTTP/3 connections from one process-wide counter and never reuses a number. The connection address does not do: H2O frees a connection without waiting for Busker's handlers, so a new connection can get the address of one whose drainer still runs.

Each worker keeps, for the worker thread only, an open-addressing table from connection id to drain state, a free list of drain states, and a list of the states with queued tasks. A drain state holds a task queue, an active flag, the time its drainer's run started, a move counter, a count of drainer threads running or starting on it, and a flag and link for a stack of states whose last drainer ended. The state is the Runnable the executor starts, so a request that arrives alone costs a table lookup and insert beyond the default mode and allocates nothing more.

A request whose connection has no active drainer starts a virtual thread through the executor; that thread, the drainer, runs its request, then the requests of the same connection the worker queued meanwhile, and ends when the queue is empty. A request must not wait behind other handlers for longer than waking a carrier costs, of the order of 100 microseconds. The drainer records the time when it starts its first task and keeps it until it finds the queue empty, so the time measures its whole back-to-back run. A request that arrives while its connection's drainer has run for longer than 100 microseconds goes to the executor directly. Once per loop turn, and on each zero-wait turn of the hot poll while requests are queued, the worker starts a new thread for every queued request of such a connection, and while requests remain queued it waits at most 1 ms in `h2o_evloop_run`. Nothing wakes the worker when a drainer passes 100 microseconds, so the wait in the queue is bounded by about 1 ms plus one loop turn, not by the threshold, counted from when the drainer starts running: while a drainer waits for a carrier its run is not timed, and the requests behind it stay queued. That pass visits only states with queued requests and states whose drainers ended, so a slow drainer of one connection never delays another connection's requests and the pass does not grow with idle connections.

The worker offers a task, then reads the active flag; a drainer clears the flag, then polls once more. Either the worker reads false and starts a new drainer, or the clear follows the read and the drainer's poll follows the offer. A drainer whose last poll finds a task sets the flag again; if a newer drainer took it first, the older one keeps running tasks of its connection, untimed, until the queue is empty. If starting a new drainer throws, a rejection or any other Throwable such as an OutOfMemoryError from thread creation, the worker undoes the drainer count, clears the flag, and fails its request as the default mode does when it can still remove it from the queue.

A drainer that brings the drainer count to 0 pushes its state on the ended stack with one compare-and-set. Once per loop turn the worker takes the stack and releases every state whose count is 0 and whose queue is empty: out of the table, onto the free list. Only the worker queues tasks and starts drainers, so a released state holds no task and no thread, and a closed connection, whose id never returns, cannot hold a task back. Release looks at the count rather than the active flag because a drainer that lost the flag still polls its state's queue, and would otherwise poll another connection's queue once the state is reused. The table holds at most one state per connection of the worker with a drainer running, starting or ended since the last turn. Closed connections whose handlers still run count too, and `:max-connections` does not count them, so the bound is the worker's open connections plus its closed connections with running handlers, the same bound as the handler threads of the default mode.

When a stop or reload begins, before the executor shuts down, each worker's dispatch is marked as stopping and the workers are woken; stop waits up to 1 s in total for each worker's next sweep, which starts a new thread for every queued request whatever its drainer does. Later requests go to the executor directly, as in the default mode. If a worker does not sweep within the second, or the stopping thread is interrupted, which ends every wait at once and keeps the interrupt set, the executor shuts down anyway and the queued requests of every worker that has not swept yet run one after another on their drainer, as they all would without the wait. Without this, requests queued at that moment would run one after another on their drainer, and two requests of one connection where one waits for the other would wait until the stop timeout. Requests that arrive after the executor shuts down are rejected and answered with 503. Drainers are executor tasks, so stop waits for them, and a drainer runs every task still queued at shutdown. The loop-turn move does not move tasks while the executor is shut down. If the executor rejects a moved task because shutdown raced the move, the task goes back in the queue and the move stops; the worker marks the move's start before it checks that a timed drainer still runs, and that drainer's last poll, if it finds the queue empty during a move, waits for the move to end and polls again, so a task put back always runs on a drainer that stop waits for. Before `shutdownNow`, stop makes drainers interrupt themselves before each remaining task. A throwing handler is reported through the thread's uncaught-exception handler and the drainer continues. The interrupt flag is cleared between tasks.

## Required verification

Tests show that:

- an invalid `:request-threads` value is rejected at start, and `:per-request` is the default;
- with `:per-request`, every request of an HTTP/2 burst and of an HTTP/1.1 keep-alive connection runs on a new virtual thread;
- a task dispatched alone runs on a thread started through the executor;
- tasks of one connection dispatched while its drainer is active run on its thread in order, the drainer ends when the queue is empty, and the next loop turn releases the connection's state;
- tasks of two connections never share a drainer, and a released state then serves another connection alone;
- once a drainer has run for longer than the threshold, in one long handler or in many short ones, a new task of its connection goes to the executor directly, the loop-turn move starts a new thread for each queued task, and another connection's task starts a separate drainer;
- after shutdown the loop-turn move leaves queued tasks to the drainer, a task whose move the executor rejects goes back to the drainer, and a new task the executor rejects is rejected with its connection's state released;
- a task the move puts back while the drainer makes its last poll runs on that drainer, and a drainer that lost the active flag to a newer one keeps its state with its connection until it ends;
- a task dispatched as its connection's drainer finds the queue empty runs, over 20,000 forced interleavings with real virtual threads;
- a throwing task does not stop later tasks, the interrupt flag does not carry from one task to the next, and after the stop hook each remaining task starts interrupted;
- one dispatching thread and real virtual threads run every task of 300,000 over interleaved, retiring connection ids exactly once, no thread runs tasks of two connections, and states of ended connections are released and reused, so far fewer states exist than connection ids;
- queued tasks limit the worker's native wait to 1 ms, and the hot poll's native turn moves queued tasks of a slow drainer;
- an Error from the executor when a drainer starts leaves the connection usable;
- requests queued when a stop begins each get a thread before the executor shuts down: five 200 ms requests of one connection finish in about 200 ms, a request that waits for a later one of its connection completes, and a request dispatched after the stop began goes to the executor directly;
- a connection id of 0 is refused;
- an interrupted stop returns at once with the executor shut down, the generation stopping and the interrupt still set, and an interrupted wait for the workers' sweeps still reports a sweep that already ran;
- an Error from starting a drainer is thrown from the dispatch or, when an older drainer already took the request, reported;
- through the public API with `:reuse-within-connection`: a burst of quick HTTP/2 requests on one connection uses fewer threads than requests, all virtual; two connections at once on one worker never share a thread over HTTP/2, HTTP/1.1 keep-alive and HTTP/3; blocking handlers on shared connections run concurrently; quick requests are not held behind a slow handler; and requests in flight during a reload or a stop are answered; and
- full validation and the large-payload tests pass.

## Consequences

Measured as requests per second, the median of 8 rounds for h3-get and 3 rounds for the others, against main with the same H2O version as the base, with zero errors in every run: h3-get is 4 QUIC connections with 32 requests in flight each, h2-get is h2load with 8 connections of 32 streams, h1-wrk is wrk with 64 connections, and h1-wrk-pipelined is wrk with 16 connections at depth 16.

16 CPUs (shared), 8 workers:

| Scenario | Base | `:per-request` | `:reuse-within-connection` |
|---|---:|---:|---:|
| h3-get | 390,350 | 390,005 | 614,535 |
| h2-get | 603,055 | 590,039 | 843,899 |
| h1-wrk | 351,034 | 348,499 | 356,655 |
| h1-wrk-pipelined | 398,902 | 398,051 | 385,790 |

4 dedicated CPUs, 2 workers:

| Scenario | Base | `:per-request` | `:reuse-within-connection` |
|---|---:|---:|---:|
| h3-get | 149,765 | 151,718 | 150,350 |
| h2-get | 288,476 | 288,863 | 299,509 |
| h1-wrk | 116,409 | 116,757 | 113,720 |
| h1-wrk-pipelined | 132,347 | 131,468 | 125,375 |

The default mode is within about 2% of the base on both machines, inside the run-to-run variation. Reuse pays on a machine with many CPUs for many quick requests per connection over HTTP/2 and HTTP/3: on 16 CPUs h3-get reaches 1.57 times the base and h2-get 1.40 times, and in h3-get the 99th percentile latency is 0.34 ms against 0.74 ms, allocation 2,959 against 3,247 bytes per request, and virtual-thread carriers use about 2.7 CPUs against 4.2. HTTP/1 keep-alive is level there and pipelined HTTP/1 loses about 3%. On 4 CPUs reuse gains about 4% on HTTP/2, is level on HTTP/3, and loses about 2 to 3% on HTTP/1 keep-alive and about 5% on pipelined HTTP/1. A request that arrives alone allocates no drain state, and HTTP/1 allocation per request equals the default mode's.

Pipelined HTTP/1 loses because the second of two requests that arrive together on a connection runs after the first handler, where a per-task executor runs them on two carriers at once.

With `:reuse-within-connection`, anything bound to the thread can still be present when the next request of the same connection starts, so handlers must not assume a fresh thread for each request. A request can wait in the queue for up to about 1 ms plus one worker loop turn, counted from when its drainer starts running, before it gets a new thread, because a worker with queued requests wakes at least once per millisecond. A stop or reload waits up to 1 s for the workers' sweeps. The default mode is unchanged.
