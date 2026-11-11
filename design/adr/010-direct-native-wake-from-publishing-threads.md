# ADR-010 Direct native wake from publishing threads

## Status

Accepted. Supersedes [ADR-004](004-platform-notifier-for-native-wakes.md).

## Context

Ring handlers, response producers and request-body readers run on virtual threads. Each worker runs its H2O event loop on a platform thread and sleeps inside `h2o_evloop_run` when it has no work. A thread that publishes a response or a mailbox command must end that sleep.

`clj_h2o_mt_wakeup` takes the loop's multithread-queue mutex, and a virtual thread stays mounted on its carrier during a foreign call. That cost is small and bounded. The worker holds the mutex only while it detaches its message list, which takes microseconds, and the eventfd or pipe write is nonblocking. Handing the wake to another thread costs more than the time a carrier can spend waiting for that mutex.

## Decision

The publishing thread calls the native wake itself.

Each worker stores:

- a monotonically increasing Java wake generation;
- `sleep-armed?_`, true only while the worker may sleep in `h2o_evloop_run`;
- `wake-sent?_`, true once a publisher has sent the native wake for the current sleep; and
- one wake endpoint holding the worker's receiver reference, its state and a count of native wake calls in progress.

A wake request increments the wake generation. If the worker is armed and the publisher's compare-and-set of `wake-sent?_` from false to true succeeds, the publisher calls `wake-now!` on the endpoint from the publishing thread. Other publishers in the same sleep see the flag already set and send nothing, so a worker receives at most one native wake per sleep.

Before a native wait, the worker resets `wake-sent?_` to false and then sets `sleep-armed?_` to true, in that order, and rechecks its mailbox and wake generation. New work found by the recheck prevents the wait. The worker resets `wake-sent?_` nowhere else. A publisher's compare-and-set fails only when an earlier publisher won it after this reset, and that publisher sent the native wake after the reset. That wake is either consumed by a turn before the worker sleeps, which comes back to the reset, or it ends the sleep. Work published during a sleep therefore always has a native wake on its way.

`wake-now!` counts itself in the endpoint's in-flight counter, does nothing unless the endpoint is open and has a receiver, retries a failed native call once and logs a second failure without throwing. The H2O receiver stays a wake-only receiver: commands, response bytes and Java request references never enter H2O's multithread message list.

During context disposal the event-loop worker moves the endpoint from open to quiescing, waits for in-flight `wake-now!` calls, closes the endpoint, then unregisters and frees the receiver. A later `wake-now!` on the closed endpoint does not enter native code.

## Required verification

Tests show that:

- `wake-now!` calls the native wake on the calling thread with the endpoint's receiver;
- a failed native wake is retried once and a second failure is logged, not thrown;
- endpoint quiescence waits for an in-flight wake, after which `wake-now!` does nothing;
- a closed endpoint does not enter native code;
- an unarmed worker records a wake without a native call;
- several publications during one sleep produce one native wake, and a new sleep allows another;
- a worker does not sleep when work appears during its arm-and-recheck sequence;
- a response published from a virtual thread reaches an idle worker and is drained; and
- full validation and the large-payload tests pass.

## Consequences

An idle worker wakes after two thread wake-ups: the publisher's carrier and the worker. A publishing virtual thread may spend a few microseconds in native code while it takes the queue mutex and writes the eventfd.

Under saturation an active worker rarely arms sleep, so publishers seldom reach the native wake.
