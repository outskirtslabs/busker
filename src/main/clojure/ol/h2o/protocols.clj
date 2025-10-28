(ns ol.h2o.protocols
  (:refer-clojure :exclude [flush]))

;; This protocol is from https://github.com/ring-clojure/ring/blob/content-length/ring-core/src/ring/middleware/content_length.clj
(defprotocol SizableResponseBody
  (body-size-in-bytes [body ring-response]
    "Return the number of bytes that `body` will require when it is
     serialized as a response body. If the number of bytes cannot be ascertained,
     nil is returned. The ring-response is used for determining supporting
     information, such as the charset in the case of String bodies."))

(defprotocol ResponseEmitter
  "Asynchronous server→client stream over a single HTTP response.
   Suitable for Server-Sent Events (SSE), HTTP chunked streaming, and long-polling.

   A ResponseEmitter represents one open HTTP response that the server can write
   to over time until it is closed."

  (open? [this]
    "Returns true iff the stream is currently open.")

  (committed? [this]
    "Returns true iff a final (non-1xx) response has been sent.
     This becomes true either when a non-1xx status/headers have been sent, or
     when a body is written before any final response, causing the implementation
     to implicitly commit a default final response (typically 200).
     Returns false if no or if only informational (1xx) responses have been sent so far.")

  (emit! [this data] [this data opts]
    "Writes data to the client. Returns true if the data was successfully sent,
     or false if the stream is closed. No ring middleware is applied.

     `data` form:
       - Ring-style map with any of: {:status} {:status :headers} or {:status :headers :body}

         Informational responses (1xx): You may emit any number of 1xx by
         calling `emit!` with {:status 1xx :headers h}. Each 1xx is sent
         immediately without committing the final response. Including a :body
         with a 1xx :status will throw an exception.

         Final responses (2xx–5xx): At most one non-1xx may be sent. Use
         {:status 2xx–5xx :headers h}, optionally with or without :body.  Once a
         non-1xx is sent, the response is committed.  If :body is provided, it
         is sent immediately after the headers and the emitter remains open for
         more data.

       - Body-like value (any StreamableResponseBody: String, byte[], File,
         InputStream, etc.).

         If no final response has been emitted yet, implicitly commits a default
         final response with status 200 before writing the first bytes.

         If no content-length header is specified and `close-after?` is true,
         then it may be calculated automatically.

         If no content-type header is specified, it may be detected
         automatically by inspecting the initial 512 bytes of body data.

     Options: `opts` is a map containing

       - `:close-after`
                        When `close-after?` is true, the stream is closed
                        immediately after emiting this chunk / response.
                        (default: false)")

  (flush [this]
    "Sends any buffered data to the client. Has no effect if the stream is
     unbuffered or already closed.")

  (close [this]
    "Closes the stream. Idempotent: returns true if the stream was actually
     closed, or false if it was already closed.")

  (on-close [this callback]
    "Registers a callback `(fn [info])` invoked at most once when the stream
     ends for any reason.

     The callback receives a map with at least `:reason`. Any
     additional keys/values are implementation-defined.

     Reason is one of:
       :server-closed   closed by the server via `close`
       :client-aborted   client disconnected before completion"))
