# ADR-005 Platform executor for lifecycle operations

## Status

Accepted

## Context

Busker start, reload, and stop are synchronous public operations. They create and dispose H2O resources, coordinate generation retirement, and wait for platform workers. When a virtual thread calls these functions directly, foreign calls run while the virtual thread remains mounted. A long native operation can retain its carrier.

Request wakes are sent directly by the publishing thread and never wait on lifecycle work. Lifecycle work runs on a separate platform thread because shutdown waits and worker joins can take arbitrarily long.

## Decision

Each runtime uses one single-thread platform lifecycle executor. Initial start creates the executor before native setup. Public start, reload, and stop submit lifecycle work to this executor and wait on Java synchronization.

The synchronous public API remains unchanged. A virtual thread waiting for a lifecycle result can unmount. Exceptions from executor tasks are rethrown with their original cause and data.

Reload calls use the same executor and remain serialized. Stop publishes one shared result so concurrent or repeated callers wait for the same operation. The stop task retires all generations and then shuts down the lifecycle executor. Failed initial startup also shuts down the executor.

## Required verification

Tests must show that:

- virtual-thread start, reload, and stop calls run generation lifecycle work on platform threads;
- failed startup shuts down its lifecycle executor;
- concurrent reload calls remain serialized;
- concurrent and repeated stop calls return the same completed result;
- generation retirement finishes before executor shutdown;
- public exception data remains unchanged; and
- request and response behavior remains exact.

## Consequences

Lifecycle operations gain one Java executor handoff. This work is outside the request hot path. Each running Busker runtime adds one mostly idle platform thread. Runtime stop removes that thread.
