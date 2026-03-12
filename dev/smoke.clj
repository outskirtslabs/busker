(ns smoke
  (:require
   [clojure.string :as str]
   [ol.busker.protocols :as h2o]
   [ol.busker.server :as server]
   [ol.clave.storage.file :as file-storage]))

(def mib (* 1024 1024))

(defn read-body
  [req]
  (when-let [body (:body req)]
    (slurp body)))

(def chunk-8k
  (apply str (repeat 8192 "a")))

(defn streamed-body
  [size]
  (repeat (quot size (count chunk-8k)) chunk-8k))

(def abcs (cycle "abcdefghijklmnopqrstuvwxyz"))

(defn echo-handler
  [req]
  (let [body-str (read-body req)]
    {:status 200
     :headers {"content-type" (get-in req [:headers "content-type"] "text/plain")}
     :body ((fn step [s]
              (lazy-seq
               (Thread/sleep 100)
               (when (seq s)
                 (let [n 2
                       chunk (apply str (take n s))]
                   (cons chunk (step (drop n s)))))))
            (take 26 abcs))
     #_#_:body (or body-str "No body provided")}))

(defn json-handler
  [req]
  (let [body-str (read-body req)]
    {:status 200
     :headers {"content-type" "application/json"}
     :body (str "{\"received\":\"" body-str "\",\"length\":" (count (or body-str "")) "}")}))

(defn early-hints-handler
  [{emitter :ol.busker.request/emitter}]
  (future
    (h2o/emit! emitter {:status 103 :headers {"Link" "</style.css>; rel=preload; as=style"}})
    (h2o/flush emitter)
    (Thread/sleep 1000)
    (h2o/emit! emitter {:status 200 :headers {"content-type" "text/html"}})
    (h2o/emit! emitter "<!doctype html><h1>Hello world</h1>")
    (h2o/close emitter))
  {:body emitter})

(defn sse-handler
  [{emitter :ol.busker.request/emitter}]
  (future
    (h2o/emit! emitter {:status 200 :headers {"content-type" "text/event-stream" "connection" "keep-alive"}})
    (h2o/emit! emitter "event: hello\ndata: first\n\n")
    (h2o/flush emitter)
    (Thread/sleep 500)
    (h2o/emit! emitter "event: world\ndata: second\n\n")
    (h2o/flush emitter)
    (Thread/sleep 500)
    (h2o/emit! emitter "event: close\ndata:\n\n")
    (h2o/close emitter))
  {:body emitter})

(defn large-body-handler
  [req]
  (if (= :post (:request-method req))
    (let [size (let [buf (byte-array 8192)]
                 (loop [total 0]
                   (let [n (.read (:body req) buf)]
                     (if (neg? n)
                       total
                       (recur (+ total n))))))]
      {:status 200
       :headers {"content-type" "text/plain"
                 "x-body-size" (str size)}
       :body (str "got body with size: " size)})
    (let [size (* 1024 mib)]
      {:status 200
       :headers {"content-type" "text/plain"
                 "x-body-size" (str size)}
       :body (streamed-body size)})))

(defn query-handler
  [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body (str "Query string: " (or (:query-string req) "none"))})

(defn headers-handler
  [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body (str "Request headers:\n" (pr-str (:headers req)))})

(defn status-handler
  [req]
  (let [uri (:uri req)
        status (case uri
                 "/status/200" 200
                 "/status/201" 201
                 "/status/400" 400
                 "/status/404" 404
                 "/status/500" 500
                 200)]
    {:status status
     :headers {"content-type" "text/plain"}
     :body (str "Status: " status)}))

(defn router
  [req]
  (let [uri (:uri req)]
    (cond
      (= uri "/") {:status 200
                   :headers {"content-type" "text/plain"}
                   :body "Hello World"}
      (= uri "/echo") (echo-handler req)
      (= uri "/json") (json-handler req)
      (= uri "/large") (large-body-handler req)
      (= uri "/query") (query-handler req)
      (= uri "/headers") (headers-handler req)
      (= uri "/sse") (sse-handler req)
      (= uri "/early-hints") (early-hints-handler req)
      (str/starts-with? uri "/status/") (status-handler req)
      :else {:status 404
             :headers {"content-type" "text/plain"}
             :body "Not Found"})))

(defn- env
  [name default]
  (let [value (System/getenv name)]
    (if (str/blank? value)
      default
      value)))

(defn -main
  [& _]
  (let [domain (env "BUSKER_SMOKE_DOMAIN" "example.com")
        http-bind (env "BUSKER_HTTP_BIND" ":8081")
        https-bind (env "BUSKER_HTTPS_BIND" ":8082")
        acme-directory-url (env "BUSKER_ACME_DIRECTORY_URL"
                                "https://localhost:14000/dir")
        acme-trust-store (env "BUSKER_ACME_TRUST_STORE"
                              "src/test/fixtures/pebble-truststore.p12")
        acme-trust-store-pass (env "BUSKER_ACME_TRUST_STORE_PASS" "changeit")
        acme-storage-dir (env "BUSKER_ACME_STORAGE_DIR" "target/busker-main-acme")
        s (server/run-server router {:domains [domain]
                                     :default-domain domain
                                     :compress-brotli-level 11
                                     :compress-gzip-level 5
                                     :entrypoints [{:name :http
                                                    :bind http-bind
                                                    :tls false}
                                                   {:name :https
                                                    :bind https-bind
                                                    :tls {:issuers [{:directory-url acme-directory-url}]
                                                          :http-client {:ssl-context
                                                                        {:trust-store acme-trust-store
                                                                         :trust-store-pass acme-trust-store-pass}}
                                                          :storage (file-storage/file-storage acme-storage-dir)}}]})]
    (println "Server started with managed TLS")
    (println "HTTP bind:" http-bind "HTTPS bind:" https-bind)
    (println "Managed domain:" domain)
    (println "Default domain:" domain)
    (println "ACME directory:" acme-directory-url)
    (println "ACME trust store:" acme-trust-store)
    (println "ACME storage dir:" acme-storage-dir)
    (println "Listening for connections...")
    (println "\nAvailable endpoints:")
    (println "  GET  /              - Hello World")
    (println "  POST /echo          - Echo request body")
    (println "  POST /json          - JSON handler")
    (println "  GET  /large         - Large response (1GiB)")
    (println "  POST /large         - Large request (prints size of body)")
    (println "  GET  /query?foo=bar - Query string parsing")
    (println "  GET  /headers       - Show all headers")
    (println "  GET  /status/404    - Custom status codes")
    @(promise)
    (println "Shutting down...")
    (server/stop-server s)
    (println "Server stopped")))
