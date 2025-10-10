(ns ol.h2o.main
  (:require [ol.h2o.server :as server]))

(defn -main [& _]
  (let [s (server/create-server {})
        s (server/start-server s)]
    (println "Server started on port 8080")
    (println "Listening for connections...")
    (Thread/sleep 10000)
    (println "Shutting down...")
    (server/stop-server s)
    (println "Server stopped")))
