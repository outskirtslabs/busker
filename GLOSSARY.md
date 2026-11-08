# Busker

Busker is an embedded Clojure HTTP server for serving Ring applications over HTTP/1.1, HTTP/2, and HTTP/3.
It unifies ingress, request dispatch, TLS certificate management, response streaming, and generation-based reloads.

## Language

### Runtime lifecycle

`Busker server`:
A logical in-process HTTP server whose identity remains stable across config changes.
While running, it has one active generation and may also have draining generations.
Avoid: Server instance, runtime

`Server handle`:
An opaque value through which an application controls one Busker server across its lifetime.
Avoid: Runtime handle, server map

`Config`:
Ordinary Clojure data describing the desired state of a Busker server.
The same config shape starts a server and requests a reload.
Avoid: Configuration file, settings object

`Config snapshot`:
The effective pure-data config after Busker applies defaults and expands shorthand.
It is the value used to recognize unchanged config and report server state.
Avoid: Runtime snapshot, compiled config

`Runtime generation`:
A live realization of one config snapshot that owns the connections and requests admitted under it.
Its lifetime is independent of the Busker server so a superseded generation can drain safely.
Avoid: Server instance, runtime snapshot

`Candidate generation`:
A runtime generation prepared during reload but not yet selected for new work.
Failure to prepare it leaves the active generation unchanged.
Avoid: New server

`Active generation`:
The runtime generation selected to receive newly admitted work.
A running Busker server has exactly one active generation.
Avoid: Current server

`Draining generation`:
A superseded or stopping runtime generation that accepts no new work but completes the work it already owns.
A Busker server may have several draining generations at once.
Avoid: Old server, dead generation

`Reload`:
A serialized attempt to replace the active generation from a new config.
A reload activates a prepared candidate, reports an unchanged config, or fails while preserving the active generation.
Avoid: Restart, in-place mutation

### Network ingress

`Entrypoint`:
A named ingress policy that groups one or more bind addresses with their HTTP protocol and TLS settings.
Its stable keyword identifier lets dispatchers refer to ingress without repeating network details.
Avoid: Endpoint, listener

`Bind address`:
A user-facing network location attached to an entrypoint.
It identifies a network host and port or a Unix domain socket.
Avoid: URL, endpoint

`Listener`:
A live network boundary for one entrypoint bind address.
It retains the entrypoint identity while accepting the protocols enabled by that entrypoint.
Avoid: Entrypoint

### Request dispatch

`Dispatch pipeline`:
The ordered sequence of dispatchers applied after Busker identifies a request's entrypoint.
It supports both request transformation and selection of a final Ring response.
Avoid: Router, routing table

`Dispatcher`:
One conditional stage in the dispatch pipeline.
It may restrict entrypoints, test a match predicate, invoke a middleware-wrapped handler, and terminate the pipeline.
Avoid: Route, dispatch rule

`Match predicate`:
An optional function that decides whether a dispatcher applies to the current pipeline value.
Avoid: Matcher

`Dispatcher handler`:
The function invoked when a dispatcher matches.
Its result either becomes the input to later dispatchers or ends the pipeline when the dispatcher is terminal.
Avoid: Callback, native handler

`Dispatcher group`:
A set of mutually exclusive dispatchers identified by one group value.
Only the first matching dispatcher in a group runs for a request.
Avoid: Route group

`Terminal dispatcher`:
A dispatcher whose result ends the dispatch pipeline immediately.
Avoid: Final route

### TLS and certificates

`Certificate source`:
A configured source from which Busker loads a certificate chain and private key during activation.
Avoid: Certificate provider

`Static certificate`:
A certificate chain and private key supplied through a certificate source rather than obtained automatically.
Avoid: Loaded certificate, manual identity

`Managed subject name`:
A DNS subject name for which Busker should obtain and renew a certificate.
Wildcard subject names are managed subject names as well.
Avoid: Domain, hostname

`Managed certificate`:
A certificate obtained and renewed by certificate automation for a managed subject name.
Avoid: Automatic certificate

`Certificate automation`:
The background lifecycle that obtains, renews, stores, and serves managed certificates.
Avoid: Automatic HTTPS when referring only to certificate management

`TLS storage`:
The storage boundary configured for certificate automation and, when selected, session ticket state.
Avoid: Certificate storage

`Session ticket policy`:
The TLS policy that controls whether session resumption is enabled and how ticket keys persist, rotate, and expire.
Avoid: STEK config, ticket store config

`Memory persistence`:
The session ticket mode in which ticket state remains process-local and may be carried across compatible reloads.
The state is lost when the process ends.
Avoid: Ephemeral storage

`Storage persistence`:
The session ticket mode in which ticket state is saved through TLS storage.
Processes using the same storage can resume sessions from the shared state.
Avoid: File persistence

### Response streaming

`Response emitter`:
An open HTTP response that a handler can write to over time until it closes.
It can emit informational responses, one final response, and subsequent body data.
Avoid: Response channel, output stream
