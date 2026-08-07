(ns ol.busker.protocols
  (:refer-clojure :exclude [flush]))

;; This protocol is from https://github.com/ring-clojure/ring/blob/content-length/ring-core/src/ring/middleware/content_length.clj
(defprotocol SizableResponseBody
  (body-size-in-bytes [body ring-response]
    "Return the number of bytes that `body` will require when it is
     serialized as a response body. If the number of bytes cannot be ascertained,
     nil is returned. The ring-response is used for determining supporting
     information, such as the charset in the case of String bodies."))

(defprotocol ResponseEmitter
  "Controls an asynchronous server-to-client stream for one HTTP response.

   Use this protocol for Server-Sent Events (SSE), response streaming, and long polling.
   The stream closes when application code calls [[close]], a final response completes,
   or libh2o reports request termination."

  (open? [this]
    "Returns true while the emitter accepts writes.

     [[close]] changes this value to false before it dispatches close callbacks.")

  (committed? [this]
    "Returns true after a final response is committed.

     Writing body data before a final response commits status 200.
     Accepted informational responses do not commit the final response.")

  (emit! [this data] [this data opts]
    "Writes `data` to the client.

     Returns true when the writer accepts the data.
     Returns false when the native writer has stopped.
     Returns nil when a response map is ignored because another final response is already committed.
     The call may throw when `data` is invalid or the stream closes during a write.
     Emitter writes do not pass through Ring middleware.

     `data` may be a response map or body data.
     A response map follows these rules:

     - Statuses 100 through 199 except 101 are informational and reject `:body`
     - Status 101 is unsupported
     - A status of at least 200 commits the only final response
     - Header names must be lowercase strings, with one value per name

     Body data before a final response commits status 200.
     With `:close-after? false`, chunks may have these types:

     - `nil`, `byte[]`, `Number`, or `String`
     - `java.nio.ByteBuffer` or `java.io.InputStream`
     - Sequential collections of these types

     With `:close-after? true`, Busker writes a complete response body.
     When `ring-core-protocols` is present, the body must satisfy `ring.core.protocols/StreamableResponseBody`.
     Without it, Busker uses its fallback chunk types.
     Busker calculates `content-length` when the body size is known, but it does not infer `content-type`.

     Options:

     | key             | description |
     |-----------------|-------------|
     | `:close-after?` | Close the stream after writing `data` (default `false`) |")

  (flush [this]
    "Flushes buffered response data to the client.

     This call may throw when the stream is closing or closed.")

  (close [this]
    "Closes the stream and begins callback delivery.

     Returns true only for the call that claims an open emitter.
     Later calls return false.
     Registered callbacks run after the writer is closed.")

  (on-close [this callback]
    "Registers a zero-argument `callback` to run when the emitter closes.

     Callbacks run exactly once on virtual threads after explicit close, final response
     completion, or request termination reported by libh2o.
     Busker cannot run the callback for a client disconnect until libh2o reports it.

     An idle HTTP/1.1 client can disconnect without libh2o noticing right away.
     If the application sends no more data, the callback may wait until a later write
     or explicit close. Graceful server shutdown waits for the active stream rather than
     closing it. For HTTP/2 and HTTP/3, the callback runs after an idle client disconnect
     without waiting for another application write.

     Callbacks registered before termination run in registration order, and an earlier
     callback failure does not suppress later callbacks.
     A callback registered after termination runs promptly on a virtual thread."))
