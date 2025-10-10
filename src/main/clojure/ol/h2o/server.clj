(ns ol.h2o.server
  (:require [ol.h2o.evloop :as evloop]))

(defn default-loop-fn
  "Default loop iteration body - placeholder sleep."
  [{:keys [max-wait-ms args]}]
  (prn "looping " args)
  (when (pos? max-wait-ms)
    (Thread/sleep (long (min 1 max-wait-ms)))))

(def default-server {:max-connections 1024})

(defn create-server [{:keys [n-workers]
                      :or {n-workers 2}}]
  (let [sys (evloop/create-system)]
    (doseq [i (range n-workers)]
      (evloop/start-worker! sys default-loop-fn {:i i}))
    {:n-workers n-workers
     :sys sys}))

(defn stop-server [server]
  (evloop/stop-all! (:sys server)))

(comment

  (def _server (create-server {}))
  (stop-server _server)

;;
  )
