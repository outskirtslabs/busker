# ADR-006 Worker-side response head staging

## Status

Accepted

## Publication

Normalized headers travel in accepted output records with body descriptors and sequence numbers. A map containing headers and a bounded body reserves and accepts both together. A producer exception or rejected admission commits neither. Informational records remain ordered without committing final headers; body-first output installs status 200.

The worker stages headers in a confined arena and installs callback pointers from the live request. Prefix controls cannot overtake an accepted header record that has not yet reached the mailbox. Header-only records consume bounded command capacity but no body receipt.

## Context

Ring handlers and response producers run on virtual threads. H2O response changes run on the request event-loop platform worker.

Streaming starts and informational responses require native header descriptors only while the shim copies them into the H2O request pool. Keeping that temporary allocation on the worker gives it a confined lifetime and keeps native allocation off the application thread.

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
- repeated header values stay exact;
- stale commands never touch a native request; and
- streaming and informational response ordering remains exact.

## Consequences

Application virtual threads prepare Java header data for streaming and informational responses. Worker execution gives each native head a short confined-arena lifetime. Fixed-final headers are packed into a preallocated slot and copied into the H2O request pool during worker execution.
