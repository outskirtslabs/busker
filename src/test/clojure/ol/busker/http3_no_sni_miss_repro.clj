(ns ol.busker.http3-no-sni-miss-repro
  (:require
   [ol.busker :as busker]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.test-utils :as util]))

(defn -main
  [& _args]
  (let [port 18462
        runtime {:system {:id ::runtime}
                 :lookup-fn (fn [_hostname] nil)}]
    (with-redefs [clave-adapter/build-managed-plan
                  (fn [_]
                    {:subject-names ["fallback.example"]
                     :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}})
                  clave-adapter/start! (fn [_] runtime)
                  clave-adapter/wrap-handler (fn [handler _] handler)
                  clave-adapter/stop! (fn [_] nil)]
      (busker/start!
       {:tls {:certificates {:manage ["fallback.example"]}
              :issuers [{:directory-url "https://acme.example/directory"}]}
        :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                            :http3? true
                            :tls {:tls-compatibility-mode :modern}}}
        :dispatch [{:handler (fn [_] {:status 200 :body "h3-no-default"})}]})
      (Thread/sleep 300)
      (let [result (util/curl :https :h3 port "/" :host "127.0.0.1" :max-time 5)]
        (println "curl-exit" (:exit result))
        (println "curl-err" (:err result))
        (flush)
        (System/exit (if (zero? (:exit result)) 2 0))))))
