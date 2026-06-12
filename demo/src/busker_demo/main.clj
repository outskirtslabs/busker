(ns busker-demo.main
  (:require
   [ol.busker :as busker]))

(def domain
  "busker.outskirtslabs.com")

(defn text-response
  ([status body]
   (text-response status body {}))
  ([status body headers]
   {:status status
    :headers (merge {"content-type" "text/plain"} headers)
    :body body}))

(defn handler
  [req]
  (case (:request-method req)
    :get
    (case (:uri req)
      "/" (text-response 200 "Hello from Busker.\n")
      (text-response 404 "Not found.\n"))

    (text-response 405 "Method not allowed.\n" {"allow" "GET"})))

(defn config
  []
  {:tls {:certificates {:manage [domain]}}
   :entrypoints {:http {:bind ":80"
                        :http3? false
                        :tls false}
                 :https {:bind ":443"
                         :http3? true
                         :tls {}}}
   :dispatch [{:handler handler}]})

(defonce server_
  (atom nil))

(defn start!
  []
  (let [server (busker/start! (config))]
    (reset! server_ server)
    server))

(defn stop!
  []
  (when-let [server @server_]
    (reset! server_ nil)
    (busker/stop! server)))

(defn -main
  [& _]
  (let [server (start!)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(busker/stop! server)))
    (println "Busker demo listening on http://busker.outskirtslabs.com and https://busker.outskirtslabs.com")
    @(promise)))
