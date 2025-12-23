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
