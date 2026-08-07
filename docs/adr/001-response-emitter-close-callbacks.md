# ADR-001 Response emitter close callback executor

## Status

Accepted

## Context

A response emitter can reach its terminal state on an H2O event-loop thread when libh2o disposes a request.

Application callbacks must not run on that thread because they can block, throw, or delay socket processing.

The request executor cannot dispatch these callbacks during generation shutdown because it stops before the network workers dispose their remaining requests.

Graceful draining must leave admitted response streams active until they finish.
Stopping only the JVM response writer would prevent a final response while leaving the native request active.

## Decision

Each runtime generation uses a dedicated virtual-thread executor for response emitter close callbacks.

The request bridge passes its callback dispatcher to every emitter created for that generation.

An emitter atomically changes from `:open` through `:closing` to `:closed` and clears its registered callback vector when it reaches `:closed`.

Explicit close, final response completion, and native request cleanup can make the emitter terminal.
Beginning generation shutdown cannot.

Callbacks registered before termination are submitted as one ordered sequence.
A later registration is chained after that sequence and submitted promptly.
Each callback failure is caught so it cannot prevent the remaining callbacks from running.

The dispatcher falls back to `Thread/startVirtualThread` when its generation executor rejects new work.
This prevents request cleanup from running application code on an event-loop thread.

Request cleanup stops the response writer before it changes the emitter to `:closed`.
This releases writers blocked by response backpressure before application callback execution begins.

`generation/begin-stop!` starts graceful connection draining and shuts down the request executor while leaving active emitters and the callback executor running.

`generation/stop!` joins the network workers after active requests drain.
It then shuts down and awaits the callback executor before releasing the remaining generation resources.
It uses the generation stop timeout for that await and interrupts callbacks that still run after the first timeout.

## Consequences

Callbacks always run on virtual threads, including callbacks caused by cleanup on H2O event-loop threads.

Graceful shutdown lets active emitters finish.
An application that leaves an emitter open can therefore delay shutdown indefinitely.

Forced shutdown requires a separate operation that terminates the native request before request cleanup stops the writer and dispatches callbacks.
Stopping the JVM writer alone is not a valid forced-shutdown mechanism.

A callback that ignores interruption can extend shutdown for up to two stop timeouts.

Callback work does not enter the response queue, so response backpressure remains independent from application lifecycle work.
