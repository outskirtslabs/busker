(ns main
  (:require
   [ol.busker :as busker]))

(defn handler
  [req]
  {:status 200
   :headers {"content-type" "text/plain; charset=utf-8"}
   :body (str "Hello from Busker over " (:protocol req) ".\n"
              "Busker can compress this response automatically when the client asks for it.\n")})

(def config
  {:entrypoints {:http {:bind "127.0.0.1:8080"
                        :tls false}}
   :dispatch [{:handler handler}]})

(def server (atom nil))

(defn start!
  []
  (reset! server (busker/start! config))
  nil)

(defn stop!
  []
  (busker/stop! @server)
  (reset! server nil))
