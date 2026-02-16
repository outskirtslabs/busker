(ns ol.busker.http3-no-sni-miss-repro
  (:require
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.server :as server]
   [ol.busker.test-utils :as util]))

(defn -main
  [& _args]
  (let [port 18462
        runtime {:system {:id ::runtime}
                 :lookup-fn (fn [_hostname] nil)}]
    (with-redefs [clave-adapter/build-managed-plan
                  (fn [_]
                    {:domains ["fallback.example"]
                     :managed-entrypoints [{:name :tls
                                            :bind (str "127.0.0.1:" port)
                                            :http3? true
                                            :tls {:issuers [{:directory-url "https://acme.example/directory"}]}}]
                     :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}})
                  clave-adapter/start! (fn [_] runtime)
                  clave-adapter/wrap-handler (fn [handler _] handler)
                  clave-adapter/stop! (fn [_] nil)]
      (server/run-server (fn [_] {:status 200 :body "h3-no-default"})
                         {:entrypoints [{:name :tls
                                         :bind (str "127.0.0.1:" port)
                                         :http3? true
                                         :tls {:issuers [{:directory-url "https://acme.example/directory"}]}}]})
      (Thread/sleep 300)
      (let [result (util/curl :https :h3 port "/" :host "127.0.0.1" :max-time 5)]
        (println "curl-exit" (:exit result))
        (println "curl-err" (:err result))
        (flush)
        (System/exit (if (zero? (:exit result)) 2 0))))))
