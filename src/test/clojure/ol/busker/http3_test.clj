(ns ol.busker.http3-test
  (:require
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [coffi.mem :as mem]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as proto]
   [ol.busker.server :as server]
   [ol.busker.test-utils :as util]))

(def cert-file (.getAbsolutePath (io/file "src/test/fixtures/server.crt")))
(def key-file (.getAbsolutePath (io/file "src/test/fixtures/server.key")))
(def tls-sni-host "localhost.examp1e.net")

(defn- fixture-tls-bundle
  []
  {:certificate [(slurp cert-file)]
   :private-key (slurp key-file)})

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
    (let [ctx (h2o/http3-create-ptls-ctx cert-file key-file mem/null mem/null)]
      (is (some? ctx) "http3-create-ptls-ctx should return non-nil context")
      (is (not (mem/null? ctx)) "http3-create-ptls-ctx should return non-null pointer")
      (when (and ctx (not (mem/null? ctx)))
        (h2o/http3-free-ptls-ctx ctx)))))

(deftest create-ptls-context-invalid-cert-test
  (testing "ptls context creation returns nil with invalid cert"
    (let [ctx (h2o/http3-create-ptls-ctx "nonexistent.crt" "nonexistent.key" mem/null mem/null)]
      (is (or (nil? ctx) (mem/null? ctx))
          "http3-create-ptls-ctx should return nil or null for invalid cert/key"))))

(deftest create-quicly-context-test
  (testing "quicly context creation succeeds with valid ptls context"
    (let [ptls-ctx (h2o/http3-create-ptls-ctx cert-file key-file mem/null mem/null)]
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
                                    {:entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
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
                                    {:entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? false
                                                    :tls {:cert-file cert-file
                                                          :key-file key-file}}]})]
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
                                    {:entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
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

(deftest http3-no-sni-default-domain-success-test
  (testing "HTTP/3 handshake without SNI succeeds when :default-domain is configured"
    (let [port 18456
          runtime {:system {:id ::runtime}
                   :lookup-fn (fn [hostname]
                                (when (= hostname "fallback.example")
                                  (fixture-tls-bundle)))}]
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
        (let [server (server/run-server (fn [_] {:status 200 :body "h3-fallback-ok"})
                                        {:default-domain "fallback.example"
                                         :domains ["fallback.example"]
                                         :entrypoints [{:name :tls
                                                        :bind (str "127.0.0.1:" port)
                                                        :http3? true
                                                        :tls {:issuers [{:directory-url "https://acme.example/directory"}]}}]})]
          (try
            (Thread/sleep 200)
            (let [result (util/curl :https :h3 port "/" :host "127.0.0.1" :max-time 5)]
              (is (= 0 (:exit result))
                  (str "HTTP/3 no-SNI request should succeed. stderr: " (:err result)))
              (when (zero? (:exit result))
                (is (= "h3-fallback-ok" (:out result)))))
            (finally
              (server/stop-server server))))))))

(deftest http3-request-with-streaming-body-test
  (testing "HTTP/3 streaming response works correctly"
    (let [port 18446
          server (server/run-server
                  (fn [_]
                    {:status 200
                     :headers {"content-type" "text/plain"}
                     :body (list "chunk1-" "chunk2-" "chunk3")})
                  {:entrypoints [{:name :tls
                                  :bind (str "127.0.0.1:" port)
                                  :http3? true
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
                  {:entrypoints [{:name :tls
                                  :bind (str "127.0.0.1:" port)
                                  :http3? true
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
                                     :entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
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
                                     :entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
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
                                     :entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
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

(defn- curl-ssl-sessions-supported?
  "Check if curl supports --ssl-sessions option."
  []
  (let [result (p/shell {:out :string :err :string :continue true}
                        "curl" "--ssl-sessions" "/dev/null" "-s" "-o" "/dev/null" "https://127.0.0.1:1")]
    ;; If the error contains "does not support", the feature is unavailable
    (not (str/includes? (:err result) "does not support"))))

(defn- curl-0rtt
  "Make a curl request with session ticket support for 0-RTT testing.

   session-file: path to store/load session tickets
   early-data?: if true, attempt to send early data with --tls-earlydata
   proto: :h3 for HTTP/3, :h2 for HTTP/2, :h1 for HTTP/1.1

   Returns map with :exit, :out, :err, and :tls-earlydata (bytes sent as early data)"
  [session-file early-data? port path & {:keys [proto max-time]
                                         :or {proto :h3 max-time 10}}]
  (let [proto-args (case proto
                     :h1 ["--http1.1"]
                     :h2 ["--http2"]
                     :h3 ["--http3-only"]
                     [])
        session-args ["--ssl-sessions" session-file]
        early-args (when early-data? ["--tls-earlydata"])
        write-out ["-w" "\n%{tls_earlydata}"]
        default-args ["-k" "-s" "--max-time" (str max-time)]
        resolve-args ["--resolve" (str tls-sni-host ":" port ":127.0.0.1")]
        url (str "https://" tls-sni-host ":" port path)
        curl-args (concat proto-args session-args early-args write-out
                          default-args resolve-args [url])
        result (apply p/shell {:out :string :err :string :continue true} "curl" curl-args)
        lines (str/split-lines (:out result))
        body (str/join "\n" (butlast lines))
        tls-earlydata (parse-long (last lines))]
    (assoc result
           :body body
           :tls-earlydata (or tls-earlydata 0))))

(deftest http3-early-data-key-present-test
  (testing "HTTP/3 requests include :ol.busker/early-data? key"
    (let [port 18453
          early-data-value (atom nil)
          handler (fn [req]
                    (reset! early-data-value (:ol.busker/early-data? req))
                    {:status 200 :body "ok"})
          server (server/run-server handler
                                    {:entrypoints [{:name :tls
                                                    :bind (str "127.0.0.1:" port)
                                                    :http3? true
                                                    :tls {:cert-file cert-file
                                                          :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        (let [result (util/curl :https :h3 port "/")]
          (is (= 0 (:exit result))
              (str "HTTP/3 request should succeed. stderr: " (:err result)))
          (when (zero? (:exit result))
            ;; The key should exist (either true or false)
            (is (some? @early-data-value)
                ":ol.busker/early-data? key should be present in request")
            ;; First request on fresh connection should not be early data
            (is (false? @early-data-value)
                "First request should NOT be early data")))
        (finally
          (server/stop-server server))))))

(deftest http3-zero-rtt-test
  (testing "HTTP/3 0-RTT early data detection"
    (when-not (curl-ssl-sessions-supported?)
      (throw (Exception. "your curl was not compiled with --enable-ssls-export and thus does not support --ssl-sessions")))
    (let [port         18454
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-sessions-" port ".txt")
          ;; Handler returns EDN with early-data? flag
          handler      (fn [req]
                         {:status  200
                          :headers {"content-type" "application/edn"}
                          :body    (pr-str {:early-data (boolean (:ol.busker/early-data? req))
                                            :method     (name (:request-method req))})})
          server       (server/run-server handler
                                          {:entrypoints [{:name :tls
                                                          :bind (str "127.0.0.1:" port)
                                                          :http3? true
                                                          :tls {:cert-file cert-file
                                                                :key-file key-file}}]})]
      (try
        (Thread/sleep 200)

        ;; Cleanup any existing session file
        (io/delete-file session-file true)

        ;; HTTP/3 0-RTT requires multiple warmup requests to accumulate session tickets.
        ;; picotls sends 1 NewSessionTicket per connection by default (max_count=2 is a cap).
        ;; curl needs 2+ tickets before sending early data. Pattern verified:
        ;; - Request 1: establishes connection, receives ticket
        ;; - Request 2: session resumption, receives another ticket
        ;; - Request 3 with --tls-earlydata: may not send early data yet
        ;; - Request 4 with --tls-earlydata: sends early data (77 bytes observed)

        ;; Warmup request 1 - establish session
        (let [result1 (curl-0rtt session-file false port "/")]
          (is (= 0 (:exit result1))
              (str "Warmup request 1 should succeed. stderr: " (:err result1))))

        (is (.exists (io/file session-file))
            "Session file should be created after first request")

        ;; Warmup request 2 - session resumption, accumulates tickets
        (let [result2 (curl-0rtt session-file false port "/")]
          (is (= 0 (:exit result2))
              (str "Warmup request 2 should succeed. stderr: " (:err result2))))

        ;; Warmup request 3 with early data flag - may not send early data yet
        (let [result3 (curl-0rtt session-file true port "/")]
          (is (= 0 (:exit result3))
              (str "Warmup request 3 should succeed. stderr: " (:err result3))))

        ;; Final request - should send early data
        (let [result4 (curl-0rtt session-file true port "/")]
          (is (= 0 (:exit result4))
              (str "Final request should succeed. stderr: " (:err result4)))
          (when (zero? (:exit result4))
            ;; The key proof that 0-RTT works: curl sends early data (tls_earlydata > 0).
            ;; Server-side detection via h2o_conn_is_early_data() is inherently racy -
            ;; it checks if handshake is in progress, but by the time the handler runs,
            ;; the QUIC handshake may have completed even though data arrived via 0-RTT.
            (is (pos? (:tls-earlydata result4))
                (str "Final request should send early data (0-RTT). Got tls_earlydata="
                     (:tls-earlydata result4)))))

        (finally
          (server/stop-server server)
          (io/delete-file session-file true))))))

(defn- openssl-session-test
  "Test TLS session resumption using openssl s_client.
   More reliable than curl for testing ticket mechanisms.
   Returns {:new? true/false :reused? true/false} based on handshake output."
  [port tls-version session-file save?]
  (let [tls-arg (case tls-version :tls1.2 "-tls1_2" :tls1.3 "")
        sess-arg (if save? "-sess_out" "-sess_in")
        cmd (str "(sleep 1; echo Q) | timeout 5 openssl s_client -connect 127.0.0.1:" port
                 " -servername " tls-sni-host
                 " " tls-arg " " sess-arg " " session-file " 2>&1")
        result (p/shell {:out :string :err :string :continue true} "bash" "-c" cmd)
        out (:out result)]
    {:exit (:exit result)
     :new? (str/includes? out "New,")
     :reused? (str/includes? out "Reused,")}))

(deftest tcp-tls-session-resumption-test
  (testing "TCP TLS 1.2 session resumption via tickets"
    ;; TLS 1.2 session resumption works with our SSL_CTX_set_tlsext_ticket_key_cb callback.
    ;; This validates the ticket encryption/decryption implementation.
    ;; Note: TLS 1.3 TCP session resumption requires BoringSSL's SSL_CTX_set_ticket_aead_method
    ;; which is not yet implemented. HTTP/3 uses picotls which has separate ticket handling.
    (let [port         18455
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-tls12-sessions-" port ".pem")
          handler      (fn [_] {:status 200 :body "ok"})
          server       (server/run-server handler
                                          {:entrypoints [{:name :tls
                                                          :bind (str "127.0.0.1:" port)
                                                          :http3? true
                                                          :tls {:cert-file cert-file
                                                                :key-file key-file}}]})]
      (try
        (Thread/sleep 200)
        (io/delete-file session-file true)

        ;; First connection - establish session with TLS 1.2
        (let [r1 (openssl-session-test port :tls1.2 session-file true)]
          (is (= 0 (:exit r1)) "First TLS 1.2 connection should succeed")
          (is (:new? r1) "First connection should be new (not resumed)"))

        ;; Verify session file was created
        (is (.exists (io/file session-file))
            "TLS 1.2 session ticket file should be created")

        ;; Second connection - resume session with TLS 1.2
        (let [r2 (openssl-session-test port :tls1.2 session-file false)]
          (is (= 0 (:exit r2)) "Second TLS 1.2 connection should succeed")
          (is (:reused? r2) "Second connection should resume session (ticket decryption worked)"))

        (finally
          (server/stop-server server)
          (io/delete-file session-file true))))))
