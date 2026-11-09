# ADR-003 Coalesce worker mailbox wakeups

## Status

Accepted

## Context

Application virtual threads publish commands for a platform worker. Commands must preserve FIFO order without requiring a native wake for every message. A wake can fail after acceptance, and an empty poll can race a producer. Neither event permits accepted work to disappear.

## Decision

Each worker has one mailbox with capacity for 256 commands, including reserved, unpublished permits. All producers use the same accounting. Reserving a permit does not insert an empty FIFO position, so a slow producer cannot delay published commands. A valid permit permits publication without another capacity check. Preparation must either publish or release its permit.

Nonblocking worker submission returns `:accepted`, `:overloaded` or `:closed`. Required submission waits interruptibly through Java synchronization and returns `:accepted` or `:closed`. Polling a command or releasing a permit wakes capacity waiters. Closing admission wakes them and rejects new reservations; valid permits and queued work remain accounted for until publication, execution or discard.

Each worker stores an atomic mailbox-signal generation. Even values mean no active signal; odd values identify a signal batch. Publication changes an even value to an odd token and requests a wake. Other producers that observe an odd token share that signal. The platform notifier performs native wake I/O.

If notification throws, the producer resets only its exact odd token, acquires a fresh token and retries once. A second failure clears that retry token and logs the failure without changing acceptance. A later producer can request another wake.

The worker handles at most 256 mailbox commands per pass before native polling. After an empty poll it clears the signal and polls again. Work found by that recheck restores the signal. Turn exhaustion keeps the signal pending. After handling commands, the next native poll uses zero wait: native calls during mailbox processing can schedule H2O callbacks that otherwise wait behind an idle poll.

Flush and end requests use coalesced per-response sequence prefixes, not mailbox permits. A worker applies a prefix only after its accepted records have arrived or been discarded. Close therefore does not wait for command, byte or receipt capacity. Fixed responses use separate preallocated slots and wait or report full when those slots are exhausted.

Streaming proceed drains directly on the worker, preserving H2O's send/proceed/send ordering. A bounded, nonrecursive send loop handles synchronous callbacks. Response work registration and removal share the response monitor; a producer cannot signal between an empty check and removal and leave work untracked. Runnable work, native waiting and unflushed output are tracked separately.

## Consequences

Several commands can share one native wake while informational and body records retain their order. A normal stop rejects new reservations and waits for all admitted publication or discard obligations. Worker failure discards reachable commands and settles responses without assuming native payloads are safe to free.

Deterministic tests cover capacity, publication after close, FIFO delivery, wake coalescing and failure, clear-and-recheck, pending permits during stop, and direct proceed draining. Real slow-reader tests check sustained backpressure and native polling fairness.
