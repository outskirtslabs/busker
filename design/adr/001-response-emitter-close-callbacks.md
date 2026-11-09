# ADR-001 Response emitter close callback executor

## Status

Accepted

## Response lifecycle

The unified emitter uses `emit!`, `flush!`, `close!` and `on-close!`. Close stops admission and settles pending receipts immediately; it does not wait for producer completion, mailbox capacity or transmission. Accepted output finishes through an ordered end-prefix request. Producer-private output is discarded when that producer returns.

The controller notifies the emitter after releasing its state lock. The emitter detaches its callback sequence and dispatches it on virtual threads. Receipt settlement is independent of close callbacks, and native buffer release remains a separate cleanup event. A late callback joins the ordered callback chain. Graceful generation draining does not close active emitters.

## Context

A response emitter can reach its terminal state on an H2O event-loop thread when libh2o disposes a request.

Application callbacks must not run on that thread because they can block, throw, or delay socket processing.

The request executor cannot dispatch these callbacks during generation shutdown because it stops before the network workers dispose their remaining requests.

Graceful draining must leave admitted response streams active until they finish.
Stopping only the JVM response writer would prevent a final response while leaving the native request active.

## Decision

Each runtime generation uses a dedicated virtual-thread executor for response emitter close callbacks.

The request bridge passes its callback dispatcher to every emitter created for that generation.

An emitter atomically changes from `:open` to `:closed`, detaches its registered callback vector and settles its last receipt. The streaming controller settles all pending receipts under its state monitor.

Explicit close, final response completion, and native request cleanup can make the emitter terminal.
Beginning generation shutdown cannot.

Callbacks registered before termination are submitted as one ordered sequence.
A later registration is chained after that sequence and submitted promptly.
Each callback failure is caught so it cannot prevent the remaining callbacks from running.

The dispatcher falls back to `Thread/startVirtualThread` when its generation executor rejects new work.
This prevents request cleanup from running application code on an event-loop thread.

An emitter initializes its response writer for ordered header/body output or an explicit close before completion. A flush alone does not initialize it. Fixed-final publication makes the emitter terminal without creating streaming queue state.

Request cleanup ends native use of retained payloads and makes the emitter terminal. Stop closes admission and wakes capacity waiters, but does not free a native in-flight send. Private producers retain their storage until they return.

`generation/begin-stop!` starts graceful connection draining and shuts down the request executor while leaving active emitters and the callback executor running.

`generation/stop!` joins the network workers after active requests drain.
It then shuts down and awaits the callback executor before releasing the remaining generation resources.
It uses the generation stop timeout for that await and interrupts callbacks that still run after the first timeout.

## Consequences

Callbacks always run on virtual threads, including callbacks caused by cleanup on H2O event-loop threads.

Graceful shutdown lets active emitters finish.
An application that leaves an emitter open can delay retirement until its timeout. A timeout reports failure and retains resources that may still be used.

Worker failure settles application receipts and closes emitters even if native storage must remain allocated. Stopping the JVM writer alone does not establish that H2O has stopped reading payload bytes.

A callback that ignores interruption can extend shutdown for up to two stop timeouts.

Callback work does not enter the response queue, so response backpressure remains independent from application lifecycle work.

Fixed-final responses avoid generic queue and writer allocation. Streaming responses initialize that state once and enforce pending-byte, receipt and block limits.
