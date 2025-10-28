(ns ol.h2o.main
  (:require
   [clojure.java.io :as io]
   [ol.h2o.protocols :as h2o]
   [clojure.string :as str]
   [ol.h2o.server :as server]))

(def gib (* 1024 1024 1024))
(def mib (* 1024 1024))
(def kib 1024)

(defn read-body
  "Read request body as string"
  [req]
  (when-let [body (:body req)]
    (slurp body)))

(defn repeat-input-stream ^java.io.InputStream
  [n b]
  (let [b   (bit-and (int b) 0xFF)        ; 0..255
        bb  (unchecked-byte b)
        cnt (java.util.concurrent.atomic.AtomicLong. n)]
    (proxy [java.io.InputStream] []
      (read
        ([] (let [r (.get cnt)]
              (if (pos? r)
                (do (.decrementAndGet cnt) b) ; returns 0..255
                -1)))
        ([buf]
         (let [r (.get cnt)]
           (if (zero? r)
             -1
             (let [k (int (min r (alength buf)))]
               (java.util.Arrays/fill buf 0 k bb)
               (.addAndGet cnt (- k))
               k))))
        ([buf off len]
         (let [r (.get cnt)]
           (if (zero? r)
             -1
             (let [k (int (min r len))]
               (java.util.Arrays/fill buf off (+ off k) bb)
               (.addAndGet cnt (- k))
               k))))))))

(def abcs (cycle "abcdefghijklmnopqrstuvwxyz"))
(defn echo-handler
  "Echo back the request body"
  [req]
  (let [body-str (read-body req)]
    (println "Echo handler - Method:" (:request-method req) "Body length:" (count (or body-str "")))
    {:status 200
     :headers {"content-type" (get-in req [:headers "content-type"] "text/plain")}
     :body  ((fn step [s]
               (lazy-seq
                (println "STEP")
                (Thread/sleep 100)
                (when (seq s)
                  (let [n 2
                        chunk (apply str (take n s))]
                    (cons chunk (step (drop n s)))))))
             (take 26 abcs))

     #_#_:body (or body-str "No body provided")}))

(defn json-handler
  "Parse JSON request and return JSON response"
  [req]
  (let [body-str (read-body req)]
    (println "JSON handler - Body:" body-str)
    {:status 200
     :headers {"content-type" "application/json"}
     :body (str "{\"received\":\"" body-str "\",\"length\":" (count (or body-str "")) "}")}))

(defn early-hints-handler [{emitter :ol.h2o.request/emitter}]
  (future
    (h2o/emit! emitter {:status 103 :headers {"Link" "</style.css>; rel=preload; as=style"}})
    (h2o/flush emitter)
    (Thread/sleep 1000)
    (h2o/emit! emitter {:status 200 :headers {"content-type" "text/html"}})
    (h2o/emit! emitter "<!doctype html><h1>Hello world</h1>")
    (h2o/close emitter))
  {:body emitter})

(defn sse-handler [{emitter :ol.h2o.request/emitter}]
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
  "Return a large response body"
  [req]
  (if (= :post (:request-method req))
    (let [size (let [buf (byte-array 8192)]
                 (loop [total 0]
                   (let [n (.read (:body req) buf)]
                     (if (neg? n)
                       total
                       (recur (+ total n))))))]
      {:status  200
       :headers {"content-type" "text/plain"
                 "x-body-size"  (str size)}
       :body    (str "got body with size: " size)})
    (let [size (* 1024 mib)
          body (repeat-input-stream size \a)]
      {:status  200
       :headers {"content-type" "text/plain"
                 "x-body-size"  (str size)}
       :body    body})))

(defn query-handler
  "Show query string parsing"
  [req]
  (let [query (:query-string req)]
    (println "Query handler - Query:" query)
    {:status 200
     :headers {"content-type" "text/plain"}
     :body (str "Query string: " (or query "none"))}))

(defn headers-handler
  "Show all request headers"
  [req]
  (println "Headers handler - Headers:" (:headers req))
  (let [headers-str (pr-str (:headers req))]
    {:status 200
     :headers {"content-type" "text/plain"}
     :body (str "Request headers:\n" headers-str)}))

(defn status-handler
  "Return different status codes based on path"
  [req]
  (let [uri (:uri req)
        status (case uri
                 "/status/200" 200
                 "/status/201" 201
                 "/status/400" 400
                 "/status/404" 404
                 "/status/500" 500
                 200)]
    (println "Status handler - URI:" uri "Status:" status)
    {:status status
     :headers {"content-type" "text/plain"}
     :body (str "Status: " status)}))

(defn router
  "Route requests to different handlers"
  [req]
  (let [uri (:uri req)
        method (:request-method req)]
    (println "Router - Method:" method "URI:" uri " Scheme: " (:scheme req))
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

(def  cert-file (.getAbsolutePath (io/file "src/test/fixtures/server.crt")))
(def  key-file (.getAbsolutePath (io/file "src/test/fixtures/server.key")))

(defn -main [& _]
  (let [s (server/run-server router {:compress-brotli-level 15
                                     :compress-gzip-level 5
                                     :listeners [{:port 8080}
                                                 {:port 8081
                                                  :tls  {:cert-file cert-file
                                                         :key-file  key-file}}]})]
    (println "Server started on port 8080")
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
    #_(Thread/sleep 10000)
    (println "Shutting down...")
    (server/stop-server s)
    (println "Server stopped")))
