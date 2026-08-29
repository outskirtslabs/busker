# ADR-006 Worker-side response head staging

## Status

Accepted

## Context

Ring handlers and response producers run on virtual threads. H2O response changes run on the request event-loop platform worker.

The fixed-final path already transfers a pure Java command and stages native headers on that worker. Streaming response starts and informational responses instead allocated FFM header segments on the application virtual thread. Their mailbox closures retained those segments until the worker executed them.

A virtual thread remains mounted during foreign work. Native staging on an application path also makes segment lifetime harder to verify.

## Decision

Streaming starts and informational responses use immutable Java commands. Each command contains:

- generation-local request identity;
- response status;
- normalized Java header strings; and
- start-response scalar settings when required.

The event-loop worker resolves the live request immediately before execution. It rejects stale identity by doing nothing. It creates a confined arena, serializes native header descriptors and strings, calls the H2O adapter, and closes the arena after the adapter copies headers into the H2O request pool.

Stable proceed and stop callback pointers come from the live request on the worker. They are not carried in the command.

## Required verification

Tests must show that:

- command construction on a virtual thread performs no FFM allocation or serialization;
- command data contains Java values rather than native segments;
- native staging matches the shim header layout;
- execution outside the request worker fails before native use;
- repeated header values and informational lowercase names remain exact;
- stale commands never touch a native request; and
- streaming and informational response ordering remains exact.

## Consequences

Application virtual threads no longer allocate native response-head memory. Worker execution adds a short confined-arena lifetime to each streaming start and informational response. Fixed-final behavior and streaming body backpressure do not change.
