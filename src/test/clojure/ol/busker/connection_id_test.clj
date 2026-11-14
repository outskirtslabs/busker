(ns ol.busker.connection-id-test
  "The shim passes H2O's connection id with every request: the same for all
  requests of one connection, different for every connection."
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.native :as native]
   [ol.busker.request :as request]
   [ol.busker.test-utils :as util]))

(defn- connection-ids-per-run
  "Calls each function of `runs` in turn and returns, for each, the connection
  ids that came with the requests it sent."
  [runs]
  (let [ids_ (atom [])
        on-request request/on-request]
    (with-redefs [request/on-request
                  (fn [executor close-dispatch config handler req-ctx-ptr]
                    (swap! ids_ conj (native/request-connection-id req-ctx-ptr))
                    (on-request executor close-dispatch config handler req-ctx-ptr))]
      (mapv (fn [run]
              (reset! ids_ [])
              (run)
              @ids_)
            runs))))

(defn- curl-run
  "Returns a function that fetches `n` URLs with one curl process, which sends
  them on one connection."
  [scheme proto port n & args]
  (fn []
    (let [url (str (name scheme) "://" (if (= :https scheme) "localhost.examp1e.net" "127.0.0.1")
                   ":" port "/")
          result (util/curl scheme proto port "/"
                            :max-time 10
                            :args (concat ["-o" "/dev/null" "-w" "%{http_code}\n"]
                                          args
                                          (mapcat (fn [_] ["-o" "/dev/null" url]) (range (dec n)))))]
      (is (= 0 (:exit result)) (:err result))
      (is (= (repeat n "200") (str/split-lines (:out result)))))))

(defn- check-ids
  [[first-run second-run]]
  (is (= 3 (count first-run) (count second-run)))
  (is (= 1 (count (distinct first-run))) "one connection, one id")
  (is (= 1 (count (distinct second-run))) "one connection, one id")
  (is (not= (first first-run) (first second-run)) "two connections, two ids")
  (is (every? pos? (concat first-run second-run))))

(deftest http1-and-http2-requests-carry-their-connection-id-test
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler (fn [_] {:status 200 :body "ok"})
                  {:entrypoints {:plain {:bind (str "127.0.0.1:" port) :tls false :http3? false}}
                   :n-workers 2}))]
    (try
      (util/wait-for-curl-ready! :http :h1 port "/")
      (testing "HTTP/1.1 keep-alive"
        (check-ids (connection-ids-per-run [(curl-run :http :h1 port 3)
                                            (curl-run :http :h1 port 3)])))
      (testing "HTTP/2 with prior knowledge, parallel streams"
        (check-ids (connection-ids-per-run
                    [(curl-run :http nil port 3 "--http2-prior-knowledge" "--parallel")
                     (curl-run :http nil port 3 "--http2-prior-knowledge" "--parallel")])))
      (finally
        (busker/stop! server)))))

(deftest http3-requests-carry-their-connection-id-test
  (let [port (util/free-port)
        server (busker/start!
                (util/with-static-tls
                  (util/with-handler (fn [_] {:status 200 :body "ok"})
                    {:entrypoints {:tls {:bind (str "127.0.0.1:" port) :http3? true
                                         :tls {:tls-compatibility-mode :modern}}}
                     :n-workers 2})))]
    (try
      (util/wait-for-curl-ready! :https :h3 port "/" :max-time 5)
      (check-ids (connection-ids-per-run [(curl-run :https :h3 port 3)
                                          (curl-run :https :h3 port 3)]))
      (finally
        (busker/stop! server)))))
