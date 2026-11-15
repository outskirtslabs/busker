(ns ol.busker.protocols
  "Public response-body and response-emitter protocols for Ring applications.

  [[SizableResponseBody]] supplies known byte sizes for complete responses.
  [[ResponseEmitter]] supports informational responses, incremental output,
  Server-Sent Events and long polling. Ordinary Ring response maps need no emitter
  calls. An application that uses the request's emitter returns it as `:body` and
  sends output from virtual threads through [[emit!]].")

;; Ring content-length protocol: https://github.com/ring-clojure/ring/blob/content-length/ring-core/src/ring/middleware/content_length.clj
(defprotocol SizableResponseBody
  (body-size-in-bytes [body ring-response]
    "Returns the serialized byte count of `body`, or nil if it is not known.
  `ring-response` supplies metadata such as the string charset."))

(defprotocol ResponseEmitter
  "Accepts ordered output for one HTTP response and reports transport progress.

  The response closes on [[close!]], an accepted end operation, native request
  termination or the end of a handler that does not return this emitter as
  `:body`. Graceful generation draining leaves active responses usable.
  [[on-close!]] runs application cleanup independently of receipt settlement.
  Emitter calls do not pass through Ring middleware.

  Busker learns of a disconnected client only from libh2o. It sees an idle
  HTTP/1.1 disconnect only when the application writes or closes the stream, or,
  with `:http1-req-io-timeout-while-generating?`, when the request I/O timeout
  expires. See the HTTP/1 streaming timeouts section of the Configuration page
  for heartbeats."
  (open? [this]
    "Returns whether this response accepts output.")
  (committed? [this]
    "Returns whether final response headers have been accepted.")
  (emit! [this data] [this data opts]
    "Accepts a Ring response map, body value or native-buffer producer.

  A producer requires `:capacity` and takes one contiguous writable native
  ByteBuffer of that capacity. It runs synchronously on the calling virtual
  thread and finishes before this call returns. Its ByteBuffer and derived
  views expire on callback return. Its final position is the byte count. Do not
  flip it or access it from another thread. Do not keep or release it. Unsafe
  or JNI access that bypasses Java scope checks is outside this API. Capacity
  must be a positive integer at most `:output-buffer-size` (B, default 32768).
  Only producers accept it. A producer that never returns can delay the release
  of native memory, also past a shutdown deadline.

  Strings, byte arrays, ByteBuffers, nil and empty bodies are supported in both
  modes. Nonblocking encoded data must fit B. ByteBuffer input uses its remaining
  range without changing its position or limit. Mutable input is copied before
  return. Do not change it during the call. Streams, sequences and custom Ring
  body writers require blocking mode. Nonblocking calls throw without reading
  them.
  Strings use the response charset, with UTF-8 as the default or invalid-charset
  fallback. Malformed or unmappable characters use encoder replacement bytes. A
  string or sequence in a charset that cannot encode throws.

  A `content-length` header must be a non-negative integer. With `:end? true`
  and a complete body of known length, it must equal that length, or `emit!`
  throws. Other output that passes the declared length or ends short of it
  fails the response: HTTP/1.1 closes the connection, HTTP/2 and HTTP/3 reset
  the stream. No byte past the declared length is sent. An `:end? true`
  producer sent from a fixed slot throws instead: on an emitter without earlier
  output, with capacity at most the fixed body limit, the smaller of
  `:fixed-response-max-body-size` and `:output-buffer-size`, not for HEAD and
  not for status 204, 205 or 304.

  A map without `:body` is header-only. `:body nil` is an empty body write with
  a receipt. Informational statuses 100–199 except 101 reject bodies and `:end?`.
  Status 101 throws. Final headers commit once. Body-first output commits status
  200. A second final head throws. Unknown lengths use protocol framing. HEAD
  responses and statuses 204, 205 and 304 send no body bytes.

  | Option | Default | Meaning |
  |--------|---------|---------|
  | `:wait?` | `true` | Park for capacity or for a competing call if necessary. |
  | `:flush?` | `false` | Make this write and preceding output ready to send. |
  | `:end?` | `false` | End the response after this accepted output. |
  | `:capacity` | none | Maximum bytes the producer function may write. |

  Returns `{:status :accepted :progress receipt}` for body output,
  `{:status :accepted}` for headers, `{:status :full}` without waiting, or
  `{:status :closed}`. Rejection changes no headers or body output.
  Invalid input, unknown options and oversized nonblocking data throw while
  open. Calls after closure return closed first. Boolean options require booleans.
  One producer runs per response. A competing nonblocking call returns full.
  Recursive emission on the same response throws. Full or known-closed calls
  skip the producer. Closure during preparation can discard its output after
  the callback runs. A callback exception before acceptance publishes nothing.

  Waiting covers admission, not transmission. One response can mix both modes.
  Pending bytes are limited to B, with at most 64 pending body receipts. At
  most one native send of up to B bytes is in flight. Each worker has 256
  streaming blocks of B bytes, and a small nonempty write takes a whole block.
  Pressure can flush accepted output before waiting or returning full.
  `:end? true` also flushes and closes. Its receipt may already be closed.

  A receipt is a promise delivering `{:status :proceeded}`, `{:status :closed}`
  or `{:status :failed :cause error}`. Callers may inspect or dereference it,
  but must not deliver it. Proceed describes native transport progress, not
  acknowledgement by the remote application. HTTP/1.1 and HTTP/2 proceed
  follow socket-write progress. HTTP/3 may proceed during packet preparation.
  Final sends need not proceed. Close and failure settle unresolved receipts
  immediately.

  Blocking streams and custom Ring body writers can publish incrementally,
  with one receipt for all chunks. Failure after their first accepted record
  returns an accepted result with the settled receipt, rather than throwing a
  rejection of the whole call. A source failure ends the response and settles
  the receipt as failed. A close settles it as closed.")
  (flush! [this]
    "Makes the accepted output prefix ready to send without waiting for capacity.
  Does not wait for transmission or a producer. Repeated flushes coalesce.
  Returns the last body receipt, or nil when no body was accepted. During an
  incremental write, that receipt can wait for EOF even though the prefix sends.
  After close, returns the same receipt or nil without scheduling more work.
  An empty write or flush alone is not a network heartbeat.")
  (close! [this]
    "Stops new output, settles pending receipts and requests a final send after
  accepted data. Wakes blocked callers. Returns true only for the first close.
  Does not wait for a producer, capacity or transmission, and does not release
  native in-flight storage. Private output from a producer that returns later
  is discarded.")
  (on-close! [this callback]
    "Registers a zero-argument callback, called once on a virtual thread.
  Callbacks run after the response closes, in registration order. A failure
  does not suppress later ones.
  A callback registered after close also runs promptly."))
