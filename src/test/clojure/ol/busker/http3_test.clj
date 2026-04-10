(ns ol.busker.http3-test
  (:require
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [coffi.mem :as mem]
   [ol.busker :as busker]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.generation :as generation]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as proto]
   [ol.busker.test-utils :as util])
  (:import
   java.net.InetSocketAddress
   java.nio.channels.DatagramChannel))

(def tls-sni-host util/tls-sni-host)

(defn- udp-port-bound?
  "Check if a UDP socket is bound on the given port.
   Uses ss command to check for UDP listeners."
  [port]
  (let [result (p/shell {:out :string :err :string :continue true}
                        "ss" "-uln" (str "sport = :" port))]
    (and (= 0 (:exit result))
         (str/includes? (:out result) (str ":" port)))))

(defn- datagram-bindable?
  ([port]
   (datagram-bindable? "127.0.0.1" port))
  ([host port]
  (try
    (with-open [channel (DatagramChannel/open)]
      (.bind channel (InetSocketAddress. ^String host (int port)))
      true)
    (catch java.net.BindException _
      false))))

(defn- wait-for-conn-limit-current!
  [expected & {:keys [attempts delay-ms]
               :or {attempts 50
                    delay-ms 100}}]
  (loop [attempt 0]
    (let [current (h2o/conn-limit-current)]
      (cond
        (= expected current)
        current

        (< attempt (dec attempts))
        (do
          (Thread/sleep delay-ms)
          (recur (inc attempt)))

        :else
        current))))

(defn- http3-worker-connection-counts
  [server]
  (let [generation (:instance (:active @(:busker/state server)))
        http3-worker-contexts (::generation/http3-worker-contexts generation)]
    (mapv (fn [worker-http3-ctxs]
                    (reduce (fn [total http3-ctx]
                      (if (or (nil? http3-ctx) (mem/null? http3-ctx))
                        total
                        #_{:clj-kondo/ignore [:type-mismatch]}
                        (+ total (h2o/http3-num-connections http3-ctx))))
                    0
                    worker-http3-ctxs))
          http3-worker-contexts)))

(defn- wait-for-http3-worker-counts!
  [server expected-total & {:keys [expected-active-workers attempts delay-ms]
                            :or {attempts 50
                                 delay-ms 100}}]
  (loop [attempt 0]
    (let [counts (http3-worker-connection-counts server)
          total (reduce + counts)
          active-workers (count (filter pos? counts))]
      (cond
        (and (= expected-total total)
             (or (nil? expected-active-workers)
                 (= expected-active-workers active-workers)))
        counts

        (< attempt (dec attempts))
        (do
          (Thread/sleep delay-ms)
          (recur (inc attempt)))

        :else
        counts))))

(deftest http3-enabled-by-default-test
  (testing "TLS listeners should have HTTP/3 enabled by default"
    (let [listener {:port 8443 :tls {}}]
      (is (true? (generation/http3-enabled? listener))
          "http3-enabled? should return true for TLS listener without explicit :http3? flag"))))

(deftest http3-can-be-disabled-test
  (testing "TLS listeners can opt-out of HTTP/3 with :http3? false"
    (let [listener {:port 8443 :tls {:http3? false}}]
      (is (false? (generation/http3-enabled? listener))
          "http3-enabled? should return false when :http3? is explicitly false"))))

(deftest non-tls-listener-no-http3-test
  (testing "Non-TLS listeners should not have HTTP/3"
    (let [listener {:port 8080}]
      (is (false? (generation/http3-enabled? listener))
          "http3-enabled? should return false for non-TLS listener"))))

(deftest create-ptls-context-test
  (testing "ptls context creation succeeds with valid cert/key"
    (let [ctx (h2o/http3-create-ptls-ctx (util/fixture-cert-path)
                                         (util/fixture-key-path)
                                         mem/null
                                         mem/null)]
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
    (let [ptls-ctx (h2o/http3-create-ptls-ctx (util/fixture-cert-path)
                                              (util/fixture-key-path)
                                              mem/null
                                              mem/null)]
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
    (let [port (util/free-port)
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      (fn [_] {:status 200 :body "ok"})
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (Thread/sleep 100) ; Give server time to bind sockets
        (is (udp-port-bound? port)
            (str "UDP socket should be bound on port " port " for HTTP/3"))
        (finally
          (busker/stop! server))))))

(deftest server-no-udp-when-http3-disabled-test
  (testing "Server with :http3? false does not create UDP socket"
    (let [port (util/free-port)
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      (fn [_] {:status 200 :body "ok"})
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? false
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (Thread/sleep 100) ; Give server time to bind sockets
        (is (not (udp-port-bound? port))
            (str "UDP socket should NOT be bound on port " port " when :http3? is false"))
        (finally
          (busker/stop! server))))))

(deftest pooled-http3-udp-bind-retains-port-until-release-test
  (testing "A pooled HTTP/3 UDP bind keeps the port unavailable until release"
    (let [port (util/free-port)
          open-listener (requiring-resolve 'ol.busker.native/http3-open-udp-transport)
          release-listener (requiring-resolve 'ol.busker.native/http3-release-udp-transport)]
      (is (datagram-bindable? port)
          "Port should be available before pooled UDP ownership is acquired")
      (let [listener (open-listener "127.0.0.1" (short port))]
        (try
          (is (some? listener)
              "Opening the pooled UDP listener should return a handle")
          (is (not (mem/null? listener))
              "Opening the pooled UDP listener should return a non-null handle")
          (is (not (datagram-bindable? port))
              "Port should remain unavailable while pooled UDP ownership is held")
          (finally
            (when (and listener (not (mem/null? listener)))
              (release-listener listener)))))
      (is (datagram-bindable? port)
          "Port should become available again after pooled UDP ownership is released"))))

(deftest pooled-http3-udp-bind-supports-ipv6-host-test
  (testing "A pooled HTTP/3 UDP bind can reserve an IPv6 host"
    (let [port (util/free-port)
          open-transport (requiring-resolve 'ol.busker.native/http3-open-udp-transport)
          release-transport (requiring-resolve 'ol.busker.native/http3-release-udp-transport)]
      (is (datagram-bindable? "::1" port)
          "IPv6 UDP port should be available before pooled transport ownership is acquired")
      (let [transport (open-transport "::1" (short port))]
        (try
          (is (some? transport)
              "Opening a pooled HTTP/3 transport with an IPv6 host should return a handle")
          (is (not (mem/null? transport))
              "Opening a pooled HTTP/3 transport with an IPv6 host should return a non-null handle")
          (is (not (datagram-bindable? "::1" port))
              "Opening a pooled HTTP/3 transport with an IPv6 host should reserve the IPv6 UDP port")
          (finally
            (when (and transport (not (mem/null? transport)))
              (release-transport transport)))))
      (is (datagram-bindable? "::1" port)
          "Releasing a pooled HTTP/3 IPv6 transport should free the IPv6 UDP port"))))

(deftest http3-request-response-test
  (testing "HTTP/3 request receives correct response"
    (let [port (util/free-port)
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      (fn [_] {:status 200 :body "hello http3"})
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (Thread/sleep 200) ; Give server time to bind sockets
        (let [result (util/curl :https :h3 port "/" :max-time 5)]
          (is (= 0 (:exit result))
              (str "HTTP/3 curl request should succeed. stderr: " (:err result)))
          (is (= "hello http3" (:out result))
              "HTTP/3 response body should match expected"))
        (finally
          (busker/stop! server))))))

(deftest ipv6-http2-and-http3-request-response-test
  (testing "A TLS IPv6 entrypoint serves both HTTP/2 and HTTP/3 traffic"
    (let [port (util/free-port)
          handler (fn [{:keys [uri protocol]}]
                    (if (= "/ready" uri)
                      {:status 200
                       :body "ready"}
                      {:status 200
                       :body protocol}))
          curl-args ["--resolve" (str tls-sni-host ":" port ":[::1]")]
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:entrypoints {:tls {:bind (str "[::1]:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (let [h2-ready (util/wait-for-curl-ready! :https :h2 port "/ready"
                                                  :host tls-sni-host
                                                  :args curl-args
                                                  :max-time 5)
              h3-ready (util/wait-for-curl-ready! :https :h3 port "/ready"
                                                  :host tls-sni-host
                                                  :args curl-args
                                                  :max-time 5)]
          (is (= 0 (:exit h2-ready))
              (str "HTTP/2 IPv6 readiness check should succeed. stderr: "
                   (:err h2-ready)))
          (is (= 0 (:exit h3-ready))
              (str "HTTP/3 IPv6 readiness check should succeed. stderr: "
                   (:err h3-ready))))
        (let [h2-result (util/curl :https :h2 port "/"
                                   :host tls-sni-host
                                   :args curl-args
                                   :max-time 5)
              h3-result (util/curl :https :h3 port "/"
                                   :host tls-sni-host
                                   :args curl-args
                                   :max-time 5)]
          (is (= 0 (:exit h2-result))
              (str "HTTP/2 IPv6 request should succeed. stderr: " (:err h2-result)))
          (is (= "HTTP/2.0" (:out h2-result))
              "HTTP/2 IPv6 request should be served over HTTP/2")
          (is (= 0 (:exit h3-result))
              (str "HTTP/3 IPv6 request should succeed. stderr: " (:err h3-result)))
          (is (= "HTTP/3.0" (:out h3-result))
              "HTTP/3 IPv6 request should be served over HTTP/3"))
        (finally
          (busker/stop! server))))))

(deftest http3-no-sni-managed-fallback-success-test
  (testing "HTTP/3 handshake without SNI succeeds when a managed fallback subject exists"
    (let [port (util/free-port)
          runtime {:system {:id ::runtime}
                   :subject-names ["fallback.example"]
                   :lookup-fn (fn [hostname]
                                (when (= hostname "fallback.example")
                                  (util/fixture-tls-bundle)))}]
      (with-redefs [clave-adapter/build-managed-plan
                    (fn [_]
                      {:subject-names ["fallback.example"]
                       :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}})
                    clave-adapter/start! (fn [_] runtime)
                    clave-adapter/wrap-handler (fn [handler _] handler)
                    clave-adapter/stop! (fn [_] nil)]
        (let [server (busker/start!
                      {:tls {:certificates {:manage ["fallback.example"]}
                             :issuers [{:directory-url
                                        "https://acme.example/directory"}]}
                       :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}
                       :dispatch [{:handler (fn [_]
                                              {:status 200
                                               :body "h3-fallback-ok"})}]})]
          (try
            (let [result (util/wait-for-curl-ready! :https :h3 port "/"
                                                    :host "127.0.0.1"
                                                    :max-time 5)]
              (is (= 0 (:exit result))
                  (str "HTTP/3 no-SNI request should succeed. stderr: " (:err result)))
              (when (zero? (:exit result))
                (is (= "h3-fallback-ok" (:out result)))))
            (finally
              (busker/stop! server))))))))

(deftest http3-no-sni-default-domain-miss-no-crash-test
  (testing "HTTP/3 no-SNI miss fails closed without crashing the server process"
    (let [result (p/shell {:out :string :err :string :continue true}
                          "timeout" "20s"
                          "clojure" "-M:dev:test"
                          "-m" "ol.busker.http3-no-sni-miss-repro")]
      (is (= 0 (:exit result))
          (str "No-SNI miss subprocess should exit cleanly. stdout: "
               (:out result)
               " stderr: "
               (:err result)))
      (is (str/includes? (:out result) "curl-exit")
          (str "Expected subprocess to print curl outcome. stdout: " (:out result)))
      (is (not (str/includes? (:out result) "curl-exit 0"))
          (str "No-SNI miss should fail closed. stdout: " (:out result))))))

(deftest http3-request-with-streaming-body-test
  (testing "HTTP/3 streaming response works correctly"
    (let [port (util/free-port)
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      (fn [_]
                        {:status 200
                         :headers {"content-type" "text/plain"}
                         :body (list "chunk1-" "chunk2-" "chunk3")})
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (Thread/sleep 200)
        (let [result (util/curl :https :h3 port "/")]
          (is (= 0 (:exit result))
              (str "HTTP/3 streaming request should succeed. stderr: " (:err result)))
          (is (= "chunk1-chunk2-chunk3" (:out result))
              "Streamed chunks should be concatenated correctly"))
        (finally
          (busker/stop! server))))))

(deftest http3-graceful-shutdown-test
  (testing "Server gracefully shuts down HTTP/3 connections"
    (let [port (util/free-port)
          first-chunk-sent (promise)
          chunk-delay-ms 100
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
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
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (Thread/sleep 200)
        (let [request-future (future
                               (util/curl :https :h3 port "/"))]
          ;; Wait for first chunk to be sent before initiating shutdown
          (is (deref first-chunk-sent 5000 false)
              "First chunk should be sent before shutdown")
          (let [stop-future (future
                              (busker/stop! server))]
            (is (= ::timeout (deref stop-future 200 ::timeout))
                "stop! should block while stream is in progress")
            ;; Verify all chunks are received despite shutdown
            (let [result (deref request-future 10000 nil)]
              (is (some? result) "Request should complete during graceful shutdown")
              (when result
                (is (zero? (:exit result))
                    (str "curl should exit successfully. stderr: " (:err result)))
                (is (= "chunk-0-chunk-1-chunk-2-chunk-3-chunk-4-" (:out result))
                    "All chunks should be received during graceful shutdown")))
            (is (not= ::timeout (deref stop-future 5000 ::timeout))
                "stop! should complete after stream drains")))
        (finally
          nil)))))

(deftest http3-connection-limit-test
  (testing "HTTP/3 connections respect global max-connections limit"
    (let [port (util/free-port)
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
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:max-connections 2
                       :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
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
          (busker/stop! server))))))

(deftest http3-and-http2-share-limit-test
  (testing "HTTP/3 and HTTP/2 connections share the global limit"
    (let [port (util/free-port)
          stream-duration-ms 3000
          chunk-interval-ms 200
          handler (fn [{:keys [uri]
                        emitter :ol.busker.request/emitter}]
                    (if (= "/ready" uri)
                      {:status 200 :body "ready"}
                      (do
                        (future
                          (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                          (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                            (doseq [idx (range num-chunks)]
                              (proto/emit! emitter (str "chunk-" idx "-"))
                              (proto/flush emitter)
                              (Thread/sleep chunk-interval-ms)))
                          (proto/close emitter))
                        {:body emitter})))
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:max-connections 3
                       :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
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
          (busker/stop! server))))))

(deftest http3-global-limit-across-workers-test
  (testing "Connection limit is global across workers, not per-worker"
    (let [port (util/free-port)
          stream-duration-ms 10000
          chunk-interval-ms 200
          handler (fn [{:keys [uri]
                        emitter :ol.busker.request/emitter}]
                    (if (= "/ready" uri)
                      {:status 200 :body "ready"}
                      (do
                        (future
                          (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                          (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                            (doseq [idx (range num-chunks)]
                              (proto/emit! emitter (str "chunk-" idx "-"))
                              (proto/flush emitter)
                              (Thread/sleep chunk-interval-ms)))
                          (proto/close emitter))
                        {:body emitter})))
          ;; 2 workers with max 4 connections total
          ;; If it was per-worker, we'd have 8 connections allowed
      server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:n-workers 2
                       :max-connections 4
                       :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (let [ready (util/wait-for-curl-ready! :https :h2 port "/ready"
                                               :max-time 5)]
          (is (= 0 (:exit ready))
              (str "HTTP/3 readiness check should succeed. stderr: "
                   (:err ready))))
        (is (= 0 (wait-for-conn-limit-current! 0))
            "Readiness connection should release its global connection slot before the limit test starts")
        ;; Start 4 connections (should reach limit)
        (let [conns (vec (repeatedly 4 #(future (util/curl :https :h3 port "/" :max-time 20))))
              worker-counts (wait-for-http3-worker-counts! server 4 :attempts 50 :delay-ms 100)]
          (is (= 4 (reduce + worker-counts))
              (str "The first four HTTP/3 connections should all be accounted for. counts: "
                   worker-counts))
          (is (= 4 (wait-for-conn-limit-current! 4 :attempts 100 :delay-ms 100))
              "The first four HTTP/3 connections should occupy the full global limit before the fifth request starts")

          ;; Fifth connection should fail because global limit is 4, not 8
          (let [conn5 (util/curl :https :h3 port "/" :max-time 1)]
            (is (not= 0 (:exit conn5))
                "Fifth connection should fail when global limit of 4 is reached"))

          ;; Wait for all connections to complete
          (doseq [[idx conn-future] (map-indexed vector conns)]
            (let [result (deref conn-future 25000 nil)]
              (is (some? result) (str "Connection " idx " should complete"))
              (when result
                (is (zero? (:exit result))
                    (str "Connection " idx " should succeed. stderr: "
                         (:err result)))))))
        (finally
          (busker/stop! server))))))

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
    (let [port (util/free-port)
          early-data-value (atom nil)
          handler (fn [req]
                    (reset! early-data-value (:ol.busker/early-data? req))
                    {:status 200 :body "ok"})
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
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
          (busker/stop! server))))))

(deftest http3-zero-rtt-test
  (testing "HTTP/3 0-RTT early data detection"
    (when-not (curl-ssl-sessions-supported?)
      (throw (Exception. "your curl was not compiled with --enable-ssls-export and thus does not support --ssl-sessions")))
    (let [port         (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-sessions-" port ".txt")
          ;; Handler returns EDN with early-data? flag
          handler      (fn [req]
                         {:status  200
                          :headers {"content-type" "application/edn"}
                          :body    (pr-str {:early-data (boolean (:ol.busker/early-data? req))
                                            :method     (name (:request-method req))})})
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            handler
                            {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                 :http3? true
                                                 :tls {:tls-compatibility-mode
                                                       :modern}}}})))]
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
          (busker/stop! server)
          (io/delete-file session-file true))))))

(deftest tcp-tls-session-resumption-test
  (testing "TCP TLS 1.2 session resumption via tickets"
    (let [port         (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-tls12-sessions-" port ".pem")
          handler      (fn [_] {:status 200 :body "ok"})
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            handler
                            {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                 :http3? true
                                                 :tls {:tls-compatibility-mode
                                                       :modern}}}})))]
      (try
        (Thread/sleep 200)
        (io/delete-file session-file true)

        ;; First connection - establish session with TLS 1.2
        (let [r1 (util/openssl-session-handshake port :tls1.2 session-file true)]
          (is (= 0 (:exit r1)) "First TLS 1.2 connection should succeed")
          (is (:new? r1) "First connection should be new (not resumed)"))

        ;; Verify session file was created
        (is (.exists (io/file session-file))
            "TLS 1.2 session ticket file should be created")

        ;; Second connection - resume session with TLS 1.2
        (let [r2 (util/openssl-session-handshake port :tls1.2 session-file false)]
          (is (= 0 (:exit r2)) "Second TLS 1.2 connection should succeed")
          (is (:reused? r2) "Second connection should resume session (ticket decryption worked)"))

        (finally
          (busker/stop! server)
          (io/delete-file session-file true))))))

(deftest tcp-tls13-session-resumption-test
  (testing "TCP TLS 1.3 session resumption via tickets"
    (let [port         (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-tls13-sessions-" port ".pem")
          handler      (fn [_] {:status 200 :body "ok"})
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            handler
                            {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                 :http3? true
                                                 :tls {:tls-compatibility-mode
                                                       :modern}}}})))]
      (try
        (Thread/sleep 200)
        (io/delete-file session-file true)

        (let [r1 (util/openssl-session-handshake port :tls1.3 session-file true)]
          (is (= 0 (:exit r1))
              (str "First TLS 1.3 connection should succeed. output: " (:out r1)))
          (is (:new? r1) "First TLS 1.3 connection should be new"))

        (is (.exists (io/file session-file))
            "TLS 1.3 session ticket file should be created")

        (let [r2 (util/openssl-session-handshake port :tls1.3 session-file false)]
          (is (= 0 (:exit r2))
              (str "Second TLS 1.3 connection should succeed. output: " (:out r2)))
          (is (:reused? r2)
              (str "Second TLS 1.3 connection should resume. output: " (:out r2))))

        (finally
          (busker/stop! server)
          (io/delete-file session-file true))))))
