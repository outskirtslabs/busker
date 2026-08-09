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

Stop first closes admission, waits for admission sections already in progress, marks stop requested, and wakes the worker. The worker drains every admitted mailbox message before it exits. Later offers return `false`.

## Consequences

Many accepted mailbox messages can share one native wake. The design retains one FIFO mailbox, its existing capacity, and ordering between informational and final response operations.

The worker adds two small atomic fields and exact-token retry logic. Deterministic tests cover coalescing, clear-and-recheck, newer-signal preservation, repeated failures, full and stopped admission, stop coordination, and FIFO delivery.

A separate fixed-final inbox was rejected. It would require ordering coordination with informational messages, overflow routing, a second shutdown drain, and additional state without evidence that per-command temporary staging dominates the measured delay.
