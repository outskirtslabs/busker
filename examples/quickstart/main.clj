(ns main
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [ol.busker :as busker]))

(defn env
  [name default]
  (let [value (System/getenv name)]
    (if (str/blank? value) default value)))

(def mib (* 1024 1024))

(def chunk-8k
  (apply str (repeat 8192 "a")))

(defn streamed-body
  [size]
  (repeat (quot size (count chunk-8k)) chunk-8k))

(defn respond
  [req status body & [headers]]
  {:status status
   :headers (merge {"content-type" "text/plain"
                    "x-protocol" (:protocol req)}
                   headers)
   :body body})

(defn handler
  [req]
  (case [(:request-method req) (:uri req)]
    [:get "/"]
    (respond req 200 "Hello from Busker o/")

    [:get "/large"]
    {:status 200
     :headers {"content-type" "text/plain"
               "x-body-size" (str mib)
               "x-protocol" (:protocol req)}
     :body (streamed-body mib)}

    [:post "/echo"]
    (respond req
             200
             (or (some-> (:body req) slurp) "")
             {"content-type" (get-in req [:headers "content-type"] "text/plain")})

    (respond req 404 "Not found")))

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
  (let [server (busker/start! config)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(busker/stop! server)))
    (println "HTTP  http://127.0.0.1:8082")
    (println "HTTPS https://localhost.examp1e.net:8443")
    @(promise)))
