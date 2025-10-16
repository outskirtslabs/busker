# Async Response Generator Wake Strategy

The version of h2o referenced in this document is `c58132197415a3cf285230de9c38940de48f5051` from 2025-10-07.

## Purpose

This document describes the wake strategy used to coordinate response streaming between the writer thread (a virtual thread producing body chunks) and the worker thread (a platform thread that calls libh2o).
The strategy ensures that `h2o_proceed_response` callbacks fire promptly after socket writes complete, eliminating multi-second stalls in large payload transfers.

## Context

The response queue bridges two execution contexts.
The writer thread is a virtual thread spawned per-request that produces response body chunks by writing to an `OutputStream`.
The worker thread is a dedicated platform thread that owns the libh2o event loop and is the only thread permitted to call libh2o functions.

Communication between these threads follows a strict contract: exactly one `h2o_sendvec` call may be in flight at any time.
An `h2o_sendvec` operation is considered in flight from the moment the function is called until libh2o invokes the generator's `proceed` callback.
This constraint is enforced by a `scheduled?_` `AtomicBoolean` that acts as a CAS gate.

The libh2o generator contract requires that the application must not call `h2o_sendvec` again until the previous call's `proceed` callback has been invoked.
Violating this contract results in crashes.

## Problem Discovery

During testing with 1 worker thread and a 31MB payload response transfer on localhost, the system exhibited pauses of approximately 850 to 1000 milliseconds between successive `h2o_sendvec` operations.
The logs showed that the writer thread was blocked in `bbq/put` waiting for queue space while the worker thread was blocked in `epoll_wait` inside libh2o.

Instrumentation of libh2o revealed the following sequence during a pause:

```
[42:16.081960][diag] do_write complete fd=109 bytes_written_loop=8422
[42:16.081960][diag] evloop_do_proceed enter max_wait=1000
[42:16.084608][diag] evloop_do_proceed adjusted max_wait=876
[42:16.961481][diag] evloop_do_proceed wake nevents=0
[42:16.961481][diag][http1] on_send_next conn=0x7fd07c8db600 fd=109
```

The `do_write complete` log indicates that the socket write finished at timestamp 42:16.081960.
The `evloop_do_proceed wake nevents=0` log shows that `epoll_wait` returned 876 milliseconds later due to timeout expiration with zero events.
The `on_send_next` callback (which calls `h2o_proceed_response`) fired immediately after `epoll_wait` returned.

The initial hypothesis was that wake messages sent via `h2o_multithread_send_message` were being silently dropped when the worker's message queue was non-empty.
Analysis of h2o's multithread implementation confirmed that NULL messages (used for wakes) skip the eventfd write when `_messages` is not empty.
However, further investigation revealed this was not the root cause.

The Clojure-side logs showed that during the pause, the writer called `schedule-drain!` four times to enqueue chunks, but all four calls failed the CAS check because `scheduled?_` was already true.
This behavior is correct by design: a sendvec was in flight, and the writer was properly prevented from scheduling another one.
The issue was that no wake message was sent during these failed CAS attempts, leaving the worker asleep in `epoll_wait` until its timeout expired.

## Root Cause Analysis

The root cause is a timing mismatch between libh2o's event loop order of operations and our wake strategy.

The libh2o event loop (`evloop_do_proceed` in `lib/common/socket/evloop/epoll.c.h`) executes the following sequence on each iteration:

1. Call `update_status` to drain the `_statechanged` list and update epoll registrations
2. Call `adjust_max_wait` to compute the timeout based on pending timers
3. Call `epoll_wait` with the computed timeout, blocking until an event occurs or the timeout expires
4. Call `update_now` to refresh the timestamp
5. Process epoll events if any were returned
6. Drain the `_pending` list to invoke completion callbacks

When `h2o_sendvec` is called, it enqueues data to the socket write buffer and marks the socket for write.
The `do_write` function in `lib/common/socket/evloop.c.h` performs the actual kernel write operation.
When all buffers have been written and no TLS data remains in flight, `do_write` sets the `H2O_SOCKET_FLAG_IS_WRITE_NOTIFY` flag and calls `link_to_pending` to queue the socket for completion processing.
The completion callback is not invoked immediately; it is deferred until the next event loop iteration processes the `_pending` list.

This sets the stage for our stalling condition.
When the worker calls `h2o_sendvec` the evloop then returns to `epoll_wait`, the socket write may complete nearly instantly (especially on localhost), but the completion callback will not fire until after `epoll_wait` returns.
If no other events occur and no wake is sent, `epoll_wait` will block for its full timeout before returning and processing the pending completion.

The call path from write completion to the `proceed` callback is something like:

```
do_write (lib/common/socket/evloop.c.h)
  -> link_to_pending / link_to_statechanged
  -> return to event loop

Event loop iteration
  -> evloop_do_proceed
      -> adjust_max_wait
      -> epoll_wait (blocks until timeout or event)
  -> evloop_do_proceed returns

Event loop drains pending list
  -> on_write_complete (lib/common/socket.c)
      -> sock->_cb.write (HTTP/1)
          -> on_send_next (lib/http1.c)
              -> h2o_proceed_response
                  -> clj_generator_proceed (shim)
                      -> response_queue/on-proceed (Clojure)
```

The root cause is: `h2o_proceed_response` is called after `epoll_wait` returns, not when the write completes.

If the writer enqueues more chunks while `epoll_wait` is blocking, and no wake is sent, the worker will sleep for the full timeout even though new data is available and the previous `h2o_sendvec` has already completed its write operation.

## Solution

The fix ensures that every call to `schedule-drain!` either enqueues a drain task or wakes the worker, and move the enforcement of the inflight rule to the send-vecs worker function.

When `schedule-drain!` is called:

1. Attempt to CAS `scheduled?_` from false to true
2. If the CAS succeeds, enqueue a `:h2o/sendvec` message to the worker's mailbox (which also sends an `evloop/wake`)
3. If the CAS fails (because `scheduled?_` is already true), call `evloop/wake` to send a wake message to the worker

The `evloop/wake` operation calls `h2o_multithread_send_message` with a NULL message pointer.
This writes to the eventfd associated with the worker's wakeup receiver, causing `epoll_wait` to return immediately with the eventfd readable.
The worker then processes its mailbox and pending work, including any completion callbacks that have fired since the last iteration.

The second part of the fix is having the function that is responsible for draining chunks and sending them to h2o check our `in-flight_` AtomicReference.
This reference is nil if there is no sendvec in flight
If it is not nil, then it indicates a sendvec is in flight (i.e., a proceed callback has not yet been received for it)
The data inside the `in-flight_` reference is chunks, arena, and memory segments that cannot be GCed until the proceed cb is received.

This approach balances correctness and throughput:

- The one-sendvec-in-flight constraint is maintained.
- Subsequent write calls while a sendvec is in flight only wake the worker without enqueuing additional tasks.
- The worker is guaranteed to wake promptly when new data arrives because every `schedule-drain!` call results in either a new drain task or a wake signal.
- The bounded mailbox is protected from overflow because failed CAS attempts on `scheduled?_` do not enqueue messages.

The worker must not rely on `epoll_wait` timeouts to make progress because the timeout may be arbitrarily long (up to 1000ms in our configuration).

