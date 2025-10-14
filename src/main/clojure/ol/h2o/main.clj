(ns ol.h2o.main
  (:require
   [clojure.string :as str]
   [ol.h2o.server :as server]))

(defn read-body
  "Read request body as string"
  [req]
  (when-let [body (:body req)]
    (slurp body)))

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

(defn large-response-handler
  "Return a large response body"
  [_req]
  (println "Large response handler")
  (let [size (* 100 1024) ; 100 KB
        body (apply str (repeat size "X"))]
    {:status 200
     :headers {"content-type" "text/plain"
               "x-body-size" (str size)}
     :body body}))

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
    (println "Router - Method:" method "URI:" uri)
    (cond
      (= uri "/") {:status 200
                   :headers {"content-type" "text/plain"}
                   :body "Hello World"}

      (= uri "/echo") (echo-handler req)

      (= uri "/json") (json-handler req)

      (= uri "/large") (large-response-handler req)

      (= uri "/query") (query-handler req)

      (= uri "/headers") (headers-handler req)

      (str/starts-with? uri "/status/") (status-handler req)

      :else {:status 404
             :headers {"content-type" "text/plain"}
             :body "Not Found"})))

(defn -main [& _]
  (let [s (server/create-server {:handler router})
        s (server/start-server s)]
    (println "Server started on port 8080")
    (println "Listening for connections...")
    (println "\nAvailable endpoints:")
    (println "  GET  /              - Hello World")
    (println "  POST /echo          - Echo request body")
    (println "  POST /json          - JSON handler")
    (println "  GET  /large         - Large response (100KB)")
    (println "  GET  /query?foo=bar - Query string parsing")
    (println "  GET  /headers       - Show all headers")
    (println "  GET  /status/404    - Custom status codes")
    @(promise)
    #_(Thread/sleep 10000)
    (println "Shutting down...")
    (server/stop-server s)
    (println "Server stopped")))
