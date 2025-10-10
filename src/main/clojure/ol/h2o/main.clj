(ns ol.h2o.main
  (:require [ol.h2o.server :as server]))

(defn -main [& _]
  (let [s (server/create-server {})]
    (Thread/sleep 3000)
    (server/stop-server s)))
