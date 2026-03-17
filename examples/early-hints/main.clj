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

(def html
  "<!doctype html><html><head><title>Busker Early Hints</title><link rel=\"stylesheet\" href=\"/app.css\"></head><body><main><h1>Busker Early Hints</h1><p>The stylesheet is sent as a real asset.</p></main></body></html>")

(def css
  "body { font-family: serif; margin: 3rem; background: #f7f4ec; color: #1f2933; } main { max-width: 40rem; } h1 { margin: 0 0 0.5rem; }")

(defn respond
  [req status body content-type]
  {:status status
   :headers {"content-type" content-type
             "x-protocol" (:protocol req)}
   :body body})

(defn page
  [{:ol.busker.request/keys [emitter] :as req}]
  (future
    (when (#{"HTTP/2.0" "HTTP/3.0"} (:protocol req))
      (proto/emit! emitter {:status 103
                            :headers {"link" "</app.css>; rel=preload; as=style"}})
      (proto/flush emitter)
      (Thread/sleep 100))
    (proto/emit! emitter {:status 200
                          :headers {"content-type" "text/html; charset=utf-8"
                                    "x-protocol" (:protocol req)}})
    (proto/emit! emitter html)
    (proto/close emitter))
  {:body emitter})

(defn handler
  [req]
  (case (:uri req)
    "/" (page req)
    "/app.css" (respond req 200 css "text/css; charset=utf-8")
    (respond req 404 "Not found" "text/plain")))

(def config
  {:tls {:certificates
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
