(ns ol.h2o.main
  (:require [ol.h2o.server :as server]))

(defn -main [& _]
  (let [s (server/create-server {})
        s (server/start-server s)]
    (println "Server created successfully")
    (Thread/sleep 3000)
    (println "Test passed!")))
