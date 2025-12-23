(ns ol.busker.http3-test
  (:require
   [coffi.mem :as mem]
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as proto]
   [ol.busker.server :as server]
   [ol.busker.test-utils :as util]))

(def cert-file (.getAbsolutePath (io/file "src/test/fixtures/server.crt")))
(def key-file (.getAbsolutePath (io/file "src/test/fixtures/server.key")))

(defn- udp-port-bound?
  "Check if a UDP socket is bound on the given port.
   Uses ss command to check for UDP listeners."
  [port]
  (let [result (p/shell {:out :string :err :string :continue true}
                        "ss" "-uln" (str "sport = :" port))]
    (and (= 0 (:exit result))
         (str/includes? (:out result) (str ":" port)))))

(deftest http3-enabled-by-default-test
  (testing "TLS listeners should have HTTP/3 enabled by default"
    (let [listener {:port 8443 :tls {:cert-file cert-file :key-file key-file}}]
      (is (true? (server/http3-enabled? listener))
          "http3-enabled? should return true for TLS listener without explicit :http3? flag"))))

(deftest http3-can-be-disabled-test
  (testing "TLS listeners can opt-out of HTTP/3 with :http3? false"
    (let [listener {:port 8443 :tls {:cert-file cert-file :key-file key-file :http3? false}}]
      (is (false? (server/http3-enabled? listener))
          "http3-enabled? should return false when :http3? is explicitly false"))))

(deftest non-tls-listener-no-http3-test
  (testing "Non-TLS listeners should not have HTTP/3"
    (let [listener {:port 8080}]
      (is (false? (server/http3-enabled? listener))
          "http3-enabled? should return false for non-TLS listener"))))

(deftest create-ptls-context-test
  (testing "ptls context creation succeeds with valid cert/key"
    (let [ctx (h2o/http3-create-ptls-ctx cert-file key-file)]
      (is (some? ctx) "http3-create-ptls-ctx should return non-nil context")
      (is (not (mem/null? ctx)) "http3-create-ptls-ctx should return non-null pointer")
      (when (and ctx (not (mem/null? ctx)))
        (h2o/http3-free-ptls-ctx ctx)))))

(deftest create-ptls-context-invalid-cert-test
  (testing "ptls context creation returns nil with invalid cert"
    (let [ctx (h2o/http3-create-ptls-ctx "nonexistent.crt" "nonexistent.key")]
      (is (or (nil? ctx) (mem/null? ctx))
          "http3-create-ptls-ctx should return nil or null for invalid cert/key"))))

(deftest create-quicly-context-test
  (testing "quicly context creation succeeds with valid ptls context"
    (let [ptls-ctx (h2o/http3-create-ptls-ctx cert-file key-file)]
      (when (and ptls-ctx (not (mem/null? ptls-ctx)))
        (let [globalconf-ptr (mem/alloc (h2o/globalconf-size))
              _ (h2o/create-global-conf globalconf-ptr nil)
              quic-ctx (h2o/http3-create-quicly-ctx ptls-ctx globalconf-ptr)]
          (is (some? quic-ctx) "http3-create-quicly-ctx should return non-nil context")
          (is (not (mem/null? quic-ctx)) "http3-create-quicly-ctx should return non-null pointer")
          (when (and quic-ctx (not (mem/null? quic-ctx)))
            (h2o/http3-free-quicly-ctx quic-ctx))
          (h2o/http3-free-ptls-ctx ptls-ctx)
          (h2o/config-dispose globalconf-ptr))))))

(deftest server-creates-udp-listener-test
  (testing "Server with TLS listener creates UDP socket on same port"
    (let [port 18443
          server (server/run-server (fn [_] {:status 200 :body "ok"})
                                    {:listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file}}]})]
      (try
        (Thread/sleep 100) ; Give server time to bind sockets
        (is (udp-port-bound? port)
            (str "UDP socket should be bound on port " port " for HTTP/3"))
        (finally
          (server/stop-server server))))))

(deftest server-no-udp-when-http3-disabled-test
  (testing "Server with :http3? false does not create UDP socket"
    (let [port 18444
          server (server/run-server (fn [_] {:status 200 :body "ok"})
                                    {:listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file
                                                        :http3? false}}]})]
      (try
        (Thread/sleep 100) ; Give server time to bind sockets
        (is (not (udp-port-bound? port))
            (str "UDP socket should NOT be bound on port " port " when :http3? is false"))
        (finally
          (server/stop-server server))))))

(deftest http3-request-response-test
  (testing "HTTP/3 request receives correct response"
    (let [port 18445
          server (server/run-server (fn [_] {:status 200 :body "hello http3"})
                                    {:listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file}}]})]
      (try
        (Thread/sleep 200) ; Give server time to bind sockets
        (let [result (util/curl :https :h3 port "/" :max-time 5)]
          (is (= 0 (:exit result))
              (str "HTTP/3 curl request should succeed. stderr: " (:err result)))
          (is (= "hello http3" (:out result))
              "HTTP/3 response body should match expected"))
        (finally
          (server/stop-server server))))))

(deftest http3-request-with-streaming-body-test
  (testing "HTTP/3 streaming response works correctly"
    (let [port 18446
          server (server/run-server
                  (fn [_]
                    {:status 200
                     :headers {"content-type" "text/plain"}
                     :body (list "chunk1-" "chunk2-" "chunk3")})
                  {:listeners [{:port port
                                :tls {:cert-file cert-file
                                      :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        (let [result (util/curl :https :h3 port "/")]
          (is (= 0 (:exit result))
              (str "HTTP/3 streaming request should succeed. stderr: " (:err result)))
          (is (= "chunk1-chunk2-chunk3" (:out result))
              "Streamed chunks should be concatenated correctly"))
        (finally
          (server/stop-server server))))))

(deftest http3-graceful-shutdown-test
  (testing "Server gracefully shuts down HTTP/3 connections"
    (let [port 18447
          first-chunk-sent (promise)
          chunk-delay-ms 100
          server (server/run-server
                  (fn [{emitter :ol.busker.request/emitter}]
                    (future
                      (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                      (doseq [idx (range 5)]
                        (proto/emit! emitter (str "chunk-" idx "-"))
                        (proto/flush emitter)
                        (when (zero? idx)
                          (deliver first-chunk-sent true))
                        (Thread/sleep chunk-delay-ms))
                      (proto/close emitter))
                    {:body emitter})
                  {:listeners [{:port port
                                :tls {:cert-file cert-file
                                      :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        (let [request-future (future
                               (util/curl :https :h3 port "/"))]
          ;; Wait for first chunk to be sent before initiating shutdown
          (is (deref first-chunk-sent 5000 false)
              "First chunk should be sent before shutdown")
          (let [stop-future (future
                              (server/stop-server server 5 java.util.concurrent.TimeUnit/SECONDS))]
            (is (= ::timeout (deref stop-future 200 ::timeout))
                "stop-server should block while stream is in progress")
            ;; Verify all chunks are received despite shutdown
            (let [result (deref request-future 10000 nil)]
              (is (some? result) "Request should complete during graceful shutdown")
              (when result
                (is (zero? (:exit result))
                    (str "curl should exit successfully. stderr: " (:err result)))
                (is (= "chunk-0-chunk-1-chunk-2-chunk-3-chunk-4-" (:out result))
                    "All chunks should be received during graceful shutdown")))
            (is (not= ::timeout (deref stop-future 5000 ::timeout))
                "stop-server should complete after stream drains")))
        (finally
          nil)))))

(deftest http3-connection-limit-test
  (testing "HTTP/3 connections respect global max-connections limit"
    (let [port 18450
          stream-duration-ms 3000
          chunk-interval-ms 200
          ;; Create a streaming handler that keeps connections open
          handler (fn [{emitter :ol.busker.request/emitter}]
                    (future
                      (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                      (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                        (doseq [idx (range num-chunks)]
                          (proto/emit! emitter (str "chunk-" idx "-"))
                          (proto/flush emitter)
                          (Thread/sleep chunk-interval-ms)))
                      (proto/close emitter))
                    {:body emitter})
          server (server/run-server handler
                                    {:max-connections 2
                                     :listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        ;; Start 2 long-lived HTTP/3 connections
        (let [conn1 (future (util/curl :https :h3 port "/" :max-time 10))
              conn2 (future (util/curl :https :h3 port "/" :max-time 10))]
          ;; Wait a bit for connections to establish
          (Thread/sleep 500)

          ;; Third connection should fail (timeout) because limit is reached
          (let [conn3 (util/curl :https :h3 port "/" :max-time 1)]
            (is (not= 0 (:exit conn3))
                "Third HTTP/3 connection should fail when at limit"))

          ;; Wait for long-lived connections to complete
          (let [result1 (deref conn1 15000 nil)
                result2 (deref conn2 15000 nil)]
            (is (some? result1) "First connection should complete")
            (is (some? result2) "Second connection should complete")
            (when result1
              (is (zero? (:exit result1))
                  (str "First connection should succeed. stderr: " (:err result1))))
            (when result2
              (is (zero? (:exit result2))
                  (str "Second connection should succeed. stderr: " (:err result2)))))

          ;; After connections close, new connection should succeed
          (Thread/sleep 200)
          (let [conn4 (util/curl :https :h3 port "/" :max-time 5)]
            (is (zero? (:exit conn4))
                (str "New HTTP/3 connection should succeed after others close. stderr: " (:err conn4)))))
        (finally
          (server/stop-server server))))))

(deftest http3-and-http2-share-limit-test
  (testing "HTTP/3 and HTTP/2 connections share the global limit"
    (let [port 18451
          stream-duration-ms 3000
          chunk-interval-ms 200
          handler (fn [{emitter :ol.busker.request/emitter}]
                    (future
                      (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                      (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                        (doseq [idx (range num-chunks)]
                          (proto/emit! emitter (str "chunk-" idx "-"))
                          (proto/flush emitter)
                          (Thread/sleep chunk-interval-ms)))
                      (proto/close emitter))
                    {:body emitter})
          server (server/run-server handler
                                    {:max-connections 3
                                     :listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        ;; Start 2 HTTP/3 connections and 1 HTTP/2 connection
        (let [h3-conn1 (future (util/curl :https :h3 port "/" :max-time 10))
              h3-conn2 (future (util/curl :https :h3 port "/" :max-time 10))
              h2-conn (future (util/curl :https :h2 port "/" :max-time 10))]
          ;; Wait a bit for connections to establish
          (Thread/sleep 500)

          ;; Fourth connection (either protocol) should fail
          (let [conn4 (util/curl :https :h3 port "/" :max-time 1)]
            (is (not= 0 (:exit conn4))
                "Fourth connection should fail when at limit"))

          ;; Wait for all connections to complete
          (let [result1 (deref h3-conn1 15000 nil)
                result2 (deref h3-conn2 15000 nil)
                result3 (deref h2-conn 15000 nil)]
            (is (some? result1) "First HTTP/3 connection should complete")
            (is (some? result2) "Second HTTP/3 connection should complete")
            (is (some? result3) "HTTP/2 connection should complete")
            (when result1
              (is (zero? (:exit result1))
                  (str "First HTTP/3 connection should succeed. stderr: " (:err result1))))
            (when result3
              (is (zero? (:exit result3))
                  (str "HTTP/2 connection should succeed. stderr: " (:err result3))))))
        (finally
          (server/stop-server server))))))

(deftest http3-global-limit-across-workers-test
  (testing "Connection limit is global across workers, not per-worker"
    (let [port 18452
          stream-duration-ms 3000
          chunk-interval-ms 200
          handler (fn [{emitter :ol.busker.request/emitter}]
                    (future
                      (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                      (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                        (doseq [idx (range num-chunks)]
                          (proto/emit! emitter (str "chunk-" idx "-"))
                          (proto/flush emitter)
                          (Thread/sleep chunk-interval-ms)))
                      (proto/close emitter))
                    {:body emitter})
          ;; 2 workers with max 4 connections total
          ;; If it was per-worker, we'd have 8 connections allowed
          server (server/run-server handler
                                    {:n-workers 2
                                     :max-connections 4
                                     :listeners [{:port port
                                                  :tls {:cert-file cert-file
                                                        :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        ;; Start 4 connections (should reach limit)
        (let [conns (vec (for [_ (range 4)]
                           (future (util/curl :https :h3 port "/" :max-time 10))))]
          ;; Wait a bit for connections to establish
          (Thread/sleep 500)

          ;; Fifth connection should fail because global limit is 4, not 8
          (let [conn5 (util/curl :https :h3 port "/" :max-time 1)]
            (is (not= 0 (:exit conn5))
                "Fifth connection should fail when global limit of 4 is reached"))

          ;; Wait for all connections to complete
          (doseq [[idx conn-future] (map-indexed vector conns)]
            (let [result (deref conn-future 15000 nil)]
              (is (some? result) (str "Connection " idx " should complete"))
              (when result
                (is (zero? (:exit result))
                    (str "Connection " idx " should succeed. stderr: " (:err result)))))))
        (finally
          (server/stop-server server))))))
