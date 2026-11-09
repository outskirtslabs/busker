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
    "Returns the serialized byte count of `body`, or nil if it is not known."))

(defprotocol ResponseEmitter
  "Accepts ordered output for one HTTP response and reports transport progress.

  The response closes on [[close!]], an accepted end operation or native request
  termination. Graceful generation draining leaves active responses usable.
  [[on-close!]] runs application cleanup independently of receipt settlement."
  (open? [this]
    "Returns whether this response accepts output.")
  (committed? [this]
    "Returns whether final response headers have been accepted.")
  (emit! [this data] [this data opts]
    "Accepts a Ring response map, body value or native-buffer producer.

     A producer requires `:capacity`, runs synchronously on the calling virtual
     thread and finishes before this call returns. Its ByteBuffer and derived
     views expire on callback return; its final position is the byte count. Do not
     flip it or access it from another thread. Capacity must be a positive integer
     at most `:output-buffer-size` (B, default 32768); only producers accept it.

     Strings, byte arrays, ByteBuffers, nil and empty bodies are supported in both
     modes. Nonblocking encoded data must fit B. ByteBuffer input uses its remaining
     range without changing its position or limit; mutable input is copied before
     return. Streams, sequences and custom Ring body writers require blocking mode.
     Strings use the response charset, with UTF-8 as the default or invalid-charset
     fallback. Malformed or unmappable characters use encoder replacement bytes.

     A map without `:body` is header-only; `:body nil` is an empty body write with
     a receipt. Informational statuses 100–199 except 101 reject bodies and `:end?`.
     Final headers commit once; body-first output commits status 200.

     | Option | Default | Meaning |
     |--------|---------|---------|
     | `:wait?` | `true` | Park for capacity if necessary. |
     | `:flush?` | `false` | Make this write and preceding output ready to send. |
     | `:end?` | `false` | End the response after this accepted output. |
     | `:capacity` | none | Maximum bytes the producer function may write. |

     Returns `{:status :accepted :progress receipt}` for body output,
     `{:status :accepted}` for headers, `{:status :full}` without waiting, or
     `{:status :closed}`. Rejection changes no headers or body output.
     Invalid input, unknown options and oversized nonblocking data throw while
     open; calls after closure return closed first. Boolean options require booleans.
     One producer runs per response; a competing nonblocking call returns full.
     Recursive emission on the same response throws. Full or known-closed calls
     skip the producer. Closure during preparation can discard its output after
     the callback runs; a callback exception before acceptance publishes nothing.

     Waiting covers admission, not transmission. Pending bytes are limited to B,
     with at most 64 pending body receipts. Each worker has 256 streaming blocks
     of B bytes. Pressure can flush accepted output before waiting or returning
     full. `:end? true` also flushes and closes; its receipt may already be closed.

     A receipt is a promise delivering `{:status :proceeded}`, `{:status :closed}`
     or `{:status :failed :cause error}`. Callers may inspect or dereference it,
     but must not deliver it. Proceed describes native transport progress, not
     acknowledgement by the remote application. HTTP/1.1 and HTTP/2 proceed
     follow socket-write progress; HTTP/3 may proceed during packet preparation.
     Final sends need not proceed. Close settles unresolved receipts immediately.

     Blocking streams and custom Ring body writers can publish incrementally.
     Failure after their first accepted record returns an accepted result with
     the settled receipt, rather than throwing a rejection of the whole call.")
  (flush! [this]
    "Makes the accepted output prefix ready to send without waiting for capacity.
     Returns the last body receipt, or nil when no body was accepted. During an
     incremental write, that receipt can wait for EOF even though the prefix sends.
     After close, returns the same receipt or nil without scheduling more work.
     An empty write or flush alone is not a network heartbeat.")
  (close! [this]
    "Stops new output, settles pending receipts and requests a final send after
     accepted data. Returns true only for the first close. Does not wait for a
     producer, capacity or transmission, and does not release native in-flight
     storage. Private output from a producer that returns later is discarded.")
  (on-close! [this callback]
    "Registers a zero-argument callback, called once on a virtual thread.
     Callbacks run in registration order; a failure does not suppress later ones.
     A callback registered after close also runs promptly."))
