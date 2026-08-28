# ADR-003 Coalesce worker mailbox wakeups

## Status

Accepted

## Context

Each accepted worker-mailbox message previously called the native wake helper. The fixed-final path adds ordinary-data response commands to that mailbox, so it must preserve FIFO order with informational and streaming control messages while reducing repeated wake calls.

The mailbox remains bounded. Producers must not block virtual threads. A wake can fail after a message enters the mailbox, and a consumer can observe an empty mailbox while a producer is adding work.

## Decision

Each worker stores an atomic mailbox-signal generation and a short admission counter. Even generations mean no active signal. Odd generations identify a signal batch.

A producer checks that admission remains open, increments the counter, checks again, and offers to the mailbox. It decrements the counter before signalling. A successful offer advances an even generation to an odd token and then calls the native wake helper. Other producers that observe an odd token leave that batch signalled without another native wake.

If a wake throws, the producer resets only its exact odd token to the next even generation. It then retries once after acquiring a new odd token. A second failure resets only that retry token, logs the accepted message and both failures, and returns successful admission. A later producer can acquire the resulting even generation and retry wake delivery.

The consumer drains FIFO messages. After an empty poll, it clears an odd token to its next even generation and polls again. If the second poll finds a message, it marks the generation odd and continues draining before entering the native event loop. If a producer adds work after the second poll, that producer sees an even generation and wakes the event loop.

Direct `wake` calls for streaming flow control remain immediate.

After a worker drain handles any mailbox message, the immediately following native loop iteration uses zero wait.
Native downcalls made while draining can add H2O pending callbacks while `h2o_evloop_run` is inactive.
H2O polls before it runs those callbacks, so using the ordinary idle wait would delay response completion and HTTP/1.1 request reuse.

Streaming response proceed callbacks drain the next queued chunk directly on the worker thread instead of posting another mailbox command. H2O invokes proceed only after consuming the previous chunk, so the direct call preserves its required `sendvec → proceed → sendvec` sequence and lets H2O update write polling in the same event-loop turn.

Each response writer uses an atomic three-state drain marker: idle, active, or active with a producer signal. A producer changes idle to active before posting the first mailbox command. If a command or native send is already active, the producer records a signal and wakes the worker. An empty worker drain retires to idle only when no signal exists; otherwise it consumes the signal and checks the queue again.
Stop first closes admission, waits for admission sections already in progress, marks stop requested, and wakes the worker. The worker drains every admitted mailbox message before it exits. Later offers return `false`.

## Consequences

Many accepted mailbox messages can share one native wake. The design retains one FIFO mailbox, its existing capacity, and ordering between informational and final response operations.

The worker adds two small atomic fields and exact-token retry logic. Deterministic tests cover coalescing, clear-and-recheck, newer-signal preservation, repeated failures, full and stopped admission, stop coordination, and FIFO delivery.

Long streaming responses avoid one mailbox command per proceed callback. The three-state marker prevents producer work from becoming stranded when enqueue and empty-drain retirement overlap. Deterministic tests cover direct proceed draining and the concurrent producer interleaving; controlled 1-GiB upload and download tests cover sustained backpressure.

A separate fixed-final inbox was rejected. It would require ordering coordination with informational messages, overflow routing, a second shutdown drain, and additional state without evidence that per-command temporary staging dominates the measured delay.
