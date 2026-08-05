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
   The stream remains active until application code closes it or the request ends."

  (open? [this]
    "Returns false after the native response writer stops.

     This value may remain true briefly after [[close]] requests stream completion.")

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
    "Requests stream completion.

     Returns false if the native writer has already stopped and true otherwise.")

  (on-close [this callback]
    "Records `callback`, but callback delivery is not implemented.

     The current emitter never invokes callbacks registered by this method."))
