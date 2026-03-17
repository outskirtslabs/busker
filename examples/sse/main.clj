(ns main
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [ol.busker.protocols :as proto]
   [ol.busker.server :as busker]))

(defn env
  [name default]
  (let [value (System/getenv name)]
    (if (str/blank? value) default value)))

(def source-lines
  (delay (vec (line-seq (io/reader (io/file "main.clj"))))))

(defn respond
  [req status body]
  {:status status
   :headers {"content-type" "text/plain"
             "x-protocol" (:protocol req)}
   :body body})

(defn stream
  [{:ol.busker.request/keys [emitter] :as req}]
  (future
    (try
      (proto/emit! emitter {:status 200
                            :headers {"cache-control" "no-cache"
                                      "content-type" "text/event-stream"
                                      "x-protocol" (:protocol req)}})
      (doseq [line (cycle @source-lines)]
        (proto/emit! emitter (str "data: " line "\n\n"))
        (proto/flush emitter)
        (Thread/sleep 1000))
      (catch Throwable _)
      (finally
        (proto/close emitter))))
  {:body emitter})

(defn handler
  [req]
  (case (:uri req)
    "/" (stream req)
    (respond req 404 "Not found")))

(def config
  {:compress? true
   :compress-min-size 1
   :compress-brotli-level 4
   :tls {:certificates
         {:load [{:type :pem
                  :cert-file (.getPath (io/file "../../src/test/fixtures/server.crt"))
                  :key-file (.getPath (io/file "../../src/test/fixtures/server.key"))}]}}
   :entrypoints {:http {:bind (env "BUSKER_HTTP_BIND" "127.0.0.1:8082")
                        :http3? false
                        :tls false}
                 :https {:bind (env "BUSKER_HTTPS_BIND" "127.0.0.1:8443")
                         :tls {:tls-compatibility-mode :modern}}}
   :dispatch [{:handler handler}]})

(defn -main
  [& _]
  (let [server (busker/run-server config)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(busker/stop-server server)))
    (println "HTTP  http://127.0.0.1:8082")
    (println "HTTPS https://localhost.examp1e.net:8443")
    @(promise)))
