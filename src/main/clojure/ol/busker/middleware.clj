(ns ol.busker.middleware
  "Ring middleware for common HTTP handling patterns.")

(set! *warn-on-reflection* true)

(def ^:private idempotent-methods
  "HTTP methods that are safe to replay."
  #{:get :head :options :trace})

(defn wrap-reject-early-data
  "Middleware that rejects non-idempotent requests arriving via 0-RTT.

  0-RTT data can be replayed by attackers. This middleware returns
  HTTP 425 Too Early for POST, PUT, DELETE, PATCH requests that
  arrive before the TLS handshake completes.

  Options:
    :methods - set of methods to reject (default: all except GET, HEAD, OPTIONS, TRACE)
    :on-reject - fn called with request when rejecting (for logging)

  Example:
    (wrap-reject-early-data handler)
    (wrap-reject-early-data handler {:on-reject #(log/warn \"Rejected early\" %)})"
  ([handler]
   (wrap-reject-early-data handler {}))
  ([handler {:keys [methods on-reject]
             :or {methods (complement idempotent-methods)}}]
   (fn [req]
     (if (and (:ol.busker/early-data? req)
              (if (set? methods)
                (contains? methods (:request-method req))
                (methods (:request-method req))))
       (do
         (when on-reject (on-reject req))
         {:status 425
          :headers {"content-type" "text/plain"}
          :body "Too Early"})
       (handler req)))))
