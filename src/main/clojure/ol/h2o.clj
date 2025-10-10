(ns ol.h2o
  (:require
   [ring.core.protocols :as ring-protocols]))

#_(:import
   (io.cljnet Headers Server StreamingResponseOutputStream))
(comment
  (defn ring-headers->java-headers
    "Convert Ring headers map to Java Headers object.
   Ring headers are a map of string keys to string values."
    [ring-headers-map]
    (let [headers (Headers.)]
      (when ring-headers-map
        (doseq [[name value] ring-headers-map]
          (.add headers (str name) (str value))))
      headers))

  (defn stream-ring-response-body
    "Stream a Ring response body using the Ring StreamableResponseBody protocol."
    [ring-body ring-response-map ^StreamingResponseOutputStream streaming-output]
    (ring-protocols/write-body-to-stream ring-body ring-response-map streaming-output))

  (defn stream-ring-response
    "Stream a Ring response using the streaming output.
   This is the bridge between Ring protocol and our streaming layer."
    [ring-response-map ^StreamingResponseOutputStream streaming-output]
    (when (nil? ring-response-map)
      (throw (NullPointerException. "Ring response map is nil")))

    (let [status (:status ring-response-map 200)
          headers (ring-headers->java-headers (:headers ring-response-map))]
      (.setResponseHeaders streaming-output status headers))

    (if-let [ring-body (:body ring-response-map)]
      (stream-ring-response-body ring-body ring-response-map streaming-output)
      (.close streaming-output))

    ring-response-map)

  (defn make-ring-streaming-adapter [user-ring-handler]
    (fn [ring-request streaming-output]
      (let [ring-response (user-ring-handler ring-request)]
        (stream-ring-response ring-response streaming-output)
        nil)))

  (defn start-server
    "Start HTTP server with Ring compatibility and streaming response bodies.

   Configuration format:
   {:listeners [{:host \"0.0.0.0\" :port 8080}
                {:host \"0.0.0.0\" :port 8443 :ssl {:certificate-file \"cert.pem\" :private-key-file \"key.pem\"}}]
    :handler ring-handler-fn
    :num-threads 0      ; optional: 0 = auto-detect based on CPU cores
    :max-connections 0  ; optional: 0 = use default limit
   }

   SSL configuration: {:certificate-file \"/path/to/cert.pem\" :private-key-file \"/path/to/key.pem\"}

   Architecture:
   1. User provides Ring handler: request-map -> response-map
   2. We create streaming-adapter-fn: (request-map, streaming-output) -> response-map
   3. Server calls our adapter, which calls user handler and streams the response"
    [config]
    (let [{:keys [handler listeners num-threads max-connections]
           :or {num-threads 0 max-connections 0}} config
          server (Server.)]
      (when-not listeners
        (throw (IllegalArgumentException. "Config requires :listeners to be specified")))
      (when-not handler
        (throw (IllegalArgumentException. "Config requires :handler to be specified")))

      (let [java-listeners (mapv (fn [listener]
                                   (let [base-map (java.util.HashMap.)
                                         ssl-config (:ssl listener)]
                                     (.put base-map "host" (str (:host listener)))
                                     (.put base-map "port" (Integer/valueOf (:port listener)))
                                     (when ssl-config
                                       (let [ssl-java-map (java.util.HashMap.)]
                                         (.put ssl-java-map "certificate-file" (str (:certificate-file ssl-config)))
                                         (.put ssl-java-map "private-key-file" (str (:private-key-file ssl-config)))
                                         (.put base-map "ssl" ssl-java-map)))
                                     base-map))
                                 listeners)]

        (.setRingAdapter server (make-ring-streaming-adapter handler) java-listeners)
        (.start server java-listeners (int num-threads) (int max-connections)))

      {:server server}))

  (defn stop-server
    "Stop the given server"
    [server-state]
    (when-let [^Server server (:server server-state)]
      (.stop server))))
