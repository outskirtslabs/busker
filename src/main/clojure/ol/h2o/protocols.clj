(ns ol.h2o.protocols)

;; This protocol is from https://github.com/ring-clojure/ring/blob/content-length/ring-core/src/ring/middleware/content_length.clj
(defprotocol SizableResponseBody
  (body-size-in-bytes [body response]
    "Return the number of bytes that an object will require when it is
    serialized as a response body. This number will be placed in the
    Content-Length header of the response by the wrap-content-length
    middleware. If the number of bytes cannot be ascertained, nil is
    returned."))
