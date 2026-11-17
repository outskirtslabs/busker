(ns ol.busker.http3-test
  (:require
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [babashka.ffi :as mem]
   [ol.busker :as busker]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.generation :as generation]
   [ol.busker.generation.tls :as tls]
   [ol.busker.middleware :as mw]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as proto]
   [ol.busker.test-utils :as util]
   [ol.busker.tickets :as tickets])
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
      (is (true? (tls/http3-enabled? listener))
          "http3-enabled? should return true for TLS listener without explicit :http3? flag"))))

(deftest http3-can-be-disabled-test
  (testing "TLS listeners can opt-out of HTTP/3 with :http3? false"
    (let [listener {:port 8443 :tls {:http3? false}}]
      (is (false? (tls/http3-enabled? listener))
          "http3-enabled? should return false when :http3? is explicitly false"))))

(deftest non-tls-listener-no-http3-test
  (testing "Non-TLS listeners should not have HTTP/3"
    (let [listener {:port 8080}]
      (is (false? (tls/http3-enabled? listener))
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
        (let [globalconf-ptr (mem/alloc (mem/auto-arena) (h2o/globalconf-size))
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
      (let [listener (open-listener "127.0.0.1" port)]
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
      (let [transport (open-transport "::1" port)]
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

(deftest http3-request-response-on-a-port-above-32767-test
  (testing "An HTTP/3 entrypoint binds and answers on a port that does not fit a signed short"
    (let [port (util/free-port 40000 60000)
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      (fn [_] {:status 200 :body "hello high port"})
                      {:entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? true
                                           :tls {:tls-compatibility-mode
                                                 :modern}}}})))]
      (try
        (is (udp-port-bound? port)
            (str "UDP socket should be bound on port " port " for HTTP/3"))
        (let [result (util/wait-for-curl-ready! :https :h3 port "/" :max-time 5)]
          (is (= {:exit 0 :out "hello high port"}
                 (select-keys result [:exit :out]))
              (str "HTTP/3 request should succeed. stderr: " (:err result))))
        (finally
          (busker/stop! server))))))

(defn- sized-body-config
  "Config for an HTTP/3 server whose response body is `(:size query)` bytes."
  ([port]
   (sized-body-config port {}))
  ([port tls]
   (util/with-static-tls
     (util/with-handler
       (fn [{:keys [query-string]}]
         {:status 200
          :body (apply str (repeat (parse-long (subs query-string 5)) "x"))})
       {:tls tls
        :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                            :http3? true
                            :tls {:tls-compatibility-mode :modern}}}}))))

(defn- start-sized-body-server
  [port]
  (busker/start! (sized-body-config port)))

(defn- large-http3-response
  [port]
  (util/wait-for-curl-ready!
   :https :h3 port "/?size=262144" :max-time 5
   :args ["-o" "/dev/null" "-w" "%{http_code} %{size_download}"]))

(deftest http3-large-response-test
  (testing "One HTTP/3 response larger than four initial congestion windows"
    (let [port (util/free-port)
          server (start-sized-body-server port)]
      (try
        (let [result (large-http3-response port)]
          (is (= {:exit 0 :out "200 262144"} (select-keys result [:exit :out]))
              (str "stderr: " (:err result))))
        (finally
          (busker/stop! server))))))

(deftest http3-large-response-without-session-tickets-test
  (testing "Without ticket keys, large HTTP/3 responses are served before and after a reload"
    (let [port (util/free-port)
          config (sized-body-config port {:session-tickets {:disabled? true}})
          server (busker/start! config)]
      (try
        (let [result (large-http3-response port)]
          (is (= {:exit 0 :out "200 262144"} (select-keys result [:exit :out]))
              (str "stderr: " (:err result))))
        (is (= :activated (busker/reload! server config {:force? true})))
        (let [result (large-http3-response port)]
          (is (= {:exit 0 :out "200 262144"} (select-keys result [:exit :out]))
              (str "stderr after reload: " (:err result))))
        (finally
          (busker/stop! server))))))

(deftest http3-many-responses-on-one-connection-test
  (testing "Small HTTP/3 responses on one connection adding up to more than 100 KB"
    (let [port (util/free-port)
          server (start-sized-body-server port)
          n 64
          url (str "https://" tls-sni-host ":" port "/?size=2048")]
      (try
        (util/wait-for-curl-ready! :https :h3 port "/?size=1" :max-time 5)
        (let [extra (mapcat (fn [_] ["-o" "/dev/null" url]) (range (dec n)))
              result (util/curl :https :h3 port "/?size=2048" :max-time 10
                                :args (concat ["-w" "%{http_code} %{size_download} %{num_connects}\n"]
                                              extra
                                              ["-o" "/dev/null"]))
              lines (str/split-lines (:out result))]
          (is (= 0 (:exit result)) (str "stderr: " (:err result)))
          (is (= (repeat n "200 2048") (map #(subs % 0 (str/last-index-of % " ")) lines)))
          (is (= 1 (reduce + (map #(parse-long (subs % (inc (str/last-index-of % " ")))) lines)))
              "curl reuses one connection for every URL"))
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
                            (proto/flush! emitter)
                            (when (zero? idx)
                              (deliver first-chunk-sent true))
                            (Thread/sleep chunk-delay-ms))
                          (proto/close! emitter))
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
                          (proto/flush! emitter)
                          (Thread/sleep chunk-interval-ms)))
                      (proto/close! emitter))
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
                              (proto/flush! emitter)
                              (Thread/sleep chunk-interval-ms)))
                          (proto/close! emitter))
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
                              (proto/flush! emitter)
                              (Thread/sleep chunk-interval-ms)))
                          (proto/close! emitter))
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

(defn- curl-0rtt
  "Runs one HTTP/3 curl request that offers 0-RTT with the tickets in
  `session-file`, with curl's qlog written under `qlog-dir`.

  Returns the curl result with the response `:status`, `:qlogs` (the number
  of qlog files curl wrote) and `:request-in-0rtt?`, true when the qlog shows
  the whole request stream, up to its end, sent in a 0-RTT packet."
  [session-file qlog-dir port method path]
  (.mkdirs (io/file qlog-dir))
  (let [result (p/shell {:out :string :err :string :continue true
                         :extra-env {"QLOGDIR" qlog-dir}}
                        "curl" "--http3-only" "-k" "-s" "--max-time" "10"
                        "--ssl-sessions" session-file "--tls-earlydata"
                        "-X" method "-o" "/dev/null" "-w" "%{http_code}"
                        "--resolve" (str tls-sni-host ":" port ":127.0.0.1")
                        (str "https://" tls-sni-host ":" port path))
        qlogs (filter #(str/ends-with? (.getName ^java.io.File %) ".sqlog")
                      (file-seq (io/file qlog-dir)))]
    (assoc result
           :status (parse-long (str/trim (:out result)))
           :qlogs (count qlogs)
           :request-in-0rtt? (boolean
                              (some #(re-find #"\"name\":\"transport:packet_sent\".*\"stream_id\":0,[^}]*\"fin\":true.*\"packet_type\":\"0RTT\""
                                              (slurp %))
                                    qlogs)))))

(deftest http3-zero-rtt-test
  (testing "requests sent in 0-RTT reach the handler flagged as early data"
    (when-not (curl-ssl-sessions-supported?)
      (throw (Exception. "your curl was not compiled with --enable-ssls-export and thus does not support --ssl-sessions")))
    (let [port         (util/free-port)
          tmp-dir      (str (System/getProperty "java.io.tmpdir") "/busker-test-0rtt-" port)
          session-file (str tmp-dir "/sessions.txt")
          seen         (atom [])
          record       (fn [req]
                         (swap! seen conj [(:request-method req) (:uri req)
                                           (:ol.busker/early-data? req)])
                         {:status 200 :body "ok"})
          guarded      (mw/wrap-reject-early-data record)
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            (fn [req]
                              (if (str/starts-with? (:uri req) "/guarded")
                                (guarded req)
                                (record req)))
                            {:entrypoints {:tls {:bind   (str "127.0.0.1:" port)
                                                 :http3? true}}})))
          request      (fn [label method path]
                         (let [result (curl-0rtt session-file (str tmp-dir "/qlog-" label)
                                                 port method path)]
                           (is (= 0 (:exit result)) (:err result))
                           (is (pos? (:qlogs result)) "curl writes a qlog when QLOGDIR is set")
                           result))]
      (try
        (util/wait-for-curl-ready! :https :h3 port "/" :max-time 5)
        (reset! seen [])
        (is (= 200 (:status (request "warmup" "GET" "/warmup"))))
        (doseq [[label method path status] [["get" "GET" "/plain" 200]
                                            ["guarded-post" "POST" "/guarded" 425]
                                            ["guarded-get" "GET" "/guarded" 200]]]
          (let [result (request label method path)]
            (is (:request-in-0rtt? result)
                (str "curl sends the " method " " path " request in 0-RTT"))
            (is (= status (:status result)) (str method " " path))))
        (is (= [[:get "/warmup" false]
                [:get "/plain" true]
                [:get "/guarded" true]]
               @seen))
        (finally
          (busker/stop! server)
          (run! #(io/delete-file % true) (reverse (file-seq (io/file tmp-dir)))))))))

(defn- curl-h3-handshake
  "Runs one verbose HTTP/3 curl request and returns its exit code with the
  key exchange group and the signature algorithm curl reports. The signature
  is `UNDEF` on a resumed handshake, which has no CertificateVerify."
  [session-file port curves]
  (let [result (util/curl :https :h3 port "/"
                          :args ["-v" "--ssl-sessions" session-file
                                 "--curves" curves])
        [_ group signature] (re-find #"SSL connection using TLSv1\.3 / \S+ / (\S+) / (\S+)"
                                     (:err result))]
    {:exit      (:exit result)
     :err       (:err result)
     :group     group
     :signature signature}))

(deftest http3-x25519-client-handshake-and-resumption-test
  (testing "a client that sends only an X25519 key share completes without a retry and resumes"
    (when-not (curl-ssl-sessions-supported?)
      (throw (Exception. "your curl was not compiled with --enable-ssls-export and thus does not support --ssl-sessions")))
    (let [port         (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-x25519-sessions-" port ".txt")
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            (fn [_] {:status 200 :body "ok"})
                            {:entrypoints {:tls {:bind   (str "127.0.0.1:" port)
                                                 :http3? true}}})))]
      (try
        (io/delete-file session-file true)
        (util/wait-for-curl-ready! :https :h3 port "/" :max-time 5)
        ;; A server without X25519 answers this ClientHello with a
        ;; HelloRetryRequest for P-256, and refuses resumption after one.
        (let [full (curl-h3-handshake session-file port "X25519:P-256")]
          (is (= 0 (:exit full)) (:err full))
          (is (= ["x25519" "RSASSA-PSS"] [(:group full) (:signature full)])
              (:err full)))
        (let [resumed (curl-h3-handshake session-file port "X25519:P-256")]
          (is (= 0 (:exit resumed)) (:err resumed))
          (is (= ["x25519" "UNDEF"] [(:group resumed) (:signature resumed)])
              (:err resumed)))
        (finally
          (busker/stop! server)
          (io/delete-file session-file true))))))

(deftest http3-tickets-use-busker-ticket-keys-test
  (testing "HTTP/3 session tickets are encrypted and decrypted with Busker's ticket keys"
    (when-not (curl-ssl-sessions-supported?)
      (throw (Exception. "your curl was not compiled with --enable-ssls-export and thus does not support --ssl-sessions")))
    (let [port         (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir") "/busker-test-ticket-keys-" port ".txt")
          server       (busker/start!
                        (util/with-static-tls
                          (util/with-handler
                            (fn [_] {:status 200 :body "ok"})
                            {:entrypoints {:tls {:bind   (str "127.0.0.1:" port)
                                                 :http3? true}}})))
          resumed?     (fn []
                         (let [result (curl-h3-handshake session-file port "X25519")]
                           (is (= 0 (:exit result)) (:err result))
                           (is (some? (:signature result)) (:err result))
                           (= "UNDEF" (:signature result))))]
      (try
        (io/delete-file session-file true)
        (util/wait-for-curl-ready! :https :h3 port "/" :max-time 5)
        (is (false? (resumed?)) "the first handshake is a full one")
        (is (true? (resumed?)) "a ticket issued under the current keys resumes")
        (let [native-mgr (::generation/native-ticket-mgr
                          (:instance (:active @(:busker/state server))))
              now-ms     (System/currentTimeMillis)]
          (tickets/sync-keys-to-native! native-mgr
                                        [(tickets/generate-key now-ms (* 3600 1000))]))
        (is (false? (resumed?)) "a ticket issued under replaced keys does not resume")
        (is (true? (resumed?)) "a ticket issued under the new keys resumes")
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
