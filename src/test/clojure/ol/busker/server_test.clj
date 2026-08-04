(ns ol.busker.server-test
  (:require
   [babashka.http-client :as http]
   [babashka.process :as p]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :as test :refer [deftest is testing]]
   [coffi.mem :as mem]
   [ol.busker :as busker]
   [ol.busker.clave-adapter :as clave-adapter]
   [ol.busker.generation :as generation]
   [ol.busker.native :as native]
   [ol.busker.protocols :as h2o]
   [ol.busker.test-utils :as util]
   [ol.busker.tickets :as tickets]
   [ol.clave.certificate :as clave-certificate]
   [ol.clave.storage.file :as file-storage])
  (:import
   java.io.File
   java.nio.file.Files
   [java.security.cert CertificateFactory X509Certificate]))

(defn- allocate-server-ports
  []
  (let [plain (util/free-port)
        tls (loop [port (util/free-port)]
              (if (= port plain)
                (recur (util/free-port))
                port))]
    {:plain plain
     :tls tls}))

(defonce server-ports_
  (atom (allocate-server-ports)))

(defn- refresh-server-ports!
  []
  (reset! server-ports_ (allocate-server-ports)))

(test/use-fixtures :each
  (fn [f]
    (refresh-server-ports!)
    (f)))

(defn- plain-port
  []
  (:plain @server-ports_))

(defn- tls-port
  []
  (:tls @server-ports_))

(defn- active-generation-instance
  [server]
  (:instance (:active @(:busker/state server))))

(defn- active-generation-keys-edn
  [server]
  (some-> server
          active-generation-instance
          ::generation/key-manager
          tickets/current-keys
          ((fn [keys]
             (mapv tickets/key->edn keys)))))

(defn- base
  []
  (str "http://127.0.0.1:" (plain-port)))

(defn test-server [handler & {:as opts}]
  (let [wrapped-handler (fn [req]
                          (if (= "/__ready" (:uri req))
                            {:status 200
                             :body "ready"}
                            (handler req)))
        server (busker/start!
                (util/with-handler
                  wrapped-handler
                  (merge {:entrypoints {:test-plain
                                        {:bind (str "127.0.0.1:" (plain-port))
                                         :http3? false
                                         :tls false}}
                          :compress-min-size 10
                          :max-connections 1024}
                         opts)))]
    (let [ready (util/wait-for-curl-ready! :http :h1 (plain-port) "/__ready"
                                           :max-time 2)]
      (is (= 0 (:exit ready))
          (str "Plain HTTP listener should become ready before the test proceeds. stderr: "
               (:err ready))))
    server))

(defn req [method path & {:as opts}]
  (->
   (http/request (merge {:timeout 5000 :throw false
                         :headers {"Accept-Encoding" []}}
                        opts
                        {:uri (str (base) path)
                         :method method}))
   (dissoc :request)))

(defn- openssl-no-sni-request
  [port path & {:keys [timeout-seconds]
                :or {timeout-seconds 5}}]
  (let [cmd (format
             "printf 'GET %s HTTP/1.1\\r\\nHost: fallback.example\\r\\nConnection: close\\r\\n\\r\\n' | timeout %d openssl s_client -connect 127.0.0.1:%d -noservername -quiet 2>&1"
             path timeout-seconds port)]
    (p/shell {:out :string :err :string :continue true}
             "bash" "-c" cmd)))

(defmacro with-server
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[server-sym init-expr] & body]
  `(let [~server-sym ~init-expr]
     (try
       ~@body
       (finally
         (busker/stop! ~server-sym)))))

(def abcs (cycle "abcdefghijklmnopqrstuvwxyz"))
(def large-payload-str (str "START" (str/join "" (take 1000000 (cycle "abcdefghijklmnopqrstuvwxyz"))) "END"))
(defn lazy-abcs [n] ((fn step [s]
                       (lazy-seq
                        (Thread/sleep 100)
                        (when (seq s)
                          (let [n 2
                                chunk (apply str (take n s))]
                            (cons chunk (step (drop n s)))))))
                     (take n abcs)))

(deftest test-simple-request
  (with-server [_server (test-server (fn [{:keys [uri] :as _req}]
                                       (cond
                                         (= "/simple" uri)
                                         {:status  200
                                          :headers {"content-type" "text/plain"}
                                          :body    "Hello, World Hello, World Hello, World"}

                                         (= "/chunked" uri)
                                         {:status  200
                                          :headers {"content-type" "text/plain"}
                                          :body    (lazy-abcs 26)}
                                         (= "/large" uri)
                                         {:status  201
                                          :headers {"content-type" "text/plain"}
                                          :body    large-payload-str}
                                         :else {:status 400})))]
    (testing "simple"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "Hello, World Hello, World Hello, World"
                         :headers {"connection"     "close"
                                   "content-length" "38"
                                   "content-type"   "text/plain"}}
                        (req :get "/simple"))))
    (testing "chunked"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "abcdefghijklmnopqrstuvwxyz"
                         :headers {"connection" "close" "content-type" "text/plain" "transfer-encoding" "chunked"}}
                        (req :get "/chunked"))))
    (testing "large"
      (is (util/submap? {:status  201
                         :version :http1.1
                         :body    large-payload-str
                         :headers {"connection" "close" "content-type" "text/plain" "content-length" "1000008"}}
                        (req :post "/large"))))
    (testing "gzip"
      (is (util/submap? {:status  200
                         :version :http1.1
                         :body    "Hello, World Hello, World Hello, World"
                         :headers {"connection"     "close"
                                   "content-encoding" "gzip"
                                   "vary" "accept-encoding"
                                   "content-type"   "text/plain"}}
                        (req :get "/simple" {:headers {"Accept-Encoding" ["gzip"]}}))))))

(deftest test-host-header-without-request-body
  (testing "domain Host headers do not make empty requests enter body streaming"
    (let [seen-body (promise)]
      (with-server [_server (test-server (fn [{:keys [body]}]
                                           (deliver seen-body body)
                                           {:status 200
                                            :headers {"content-type" "text/plain"}
                                            :body "ok"}))]
        (let [result (util/curl :http :h1 (plain-port) "/"
                                :max-time 3
                                :args ["-H" "Host: busker.outskirtslabs.com"])
              body-value (deref seen-body 1000 ::missing)]
          (is (= 0 (:exit result))
              (str "HTTP request with Host header should succeed. stderr: " (:err result)))
          (is (= "ok" (:out result)))
          (is (not= ::missing body-value))
          (is (nil? body-value)))))))

(deftest test-content-length
  (testing "content-length with get and head"
    (with-server [_server (test-server (fn [{:keys [request-method]}]
                                         (if (= :head request-method)
                                           {:status 200
                                            :headers {"content-type" "text/plain"
                                                      "content-length" "3"}}
                                           {:status 200
                                            :headers {"content-type" "text/plain"
                                                      "content-length" "3"}
                                            :body "wow"})))]
      (let [response (req :head "/")]
        (is (= 200 (:status response)))
        (is (= "3" (get-in response [:headers "content-length"])))
        (is (= "" (:body response))))
      (let [response (req :get "/")]
        (is (= 200 (:status response)))
        (is (= "3" (get-in response [:headers "content-length"])))
        (is (= "wow" (:body response)))))))

(deftest test-different-status-codes
  (testing "Different status codes are returned correctly"
    (with-server [_server (test-server (fn [{:keys [uri]}]
                                         (case uri
                                           "/200" {:status 200 :body "OK"}
                                           "/201" {:status 201 :body "Created"}
                                           "/204" {:status 204}
                                           "/400" {:status 400 :body "Bad Request"}
                                           "/404" {:status 404 :body "Not Found"}
                                           "/500" {:status 500 :body "Internal Server Error"}
                                           {:status 200 :body "Default"})))]
      (is (= 200 (:status (req :get "/200"))))
      (is (= 201 (:status (req :get "/201"))))
      (is (= 204 (:status (req :get "/204"))))
      (is (= 400 (:status (req :get "/400"))))
      (is (= 404 (:status (req :get "/404"))))
      (is (= 500 (:status (req :get "/500")))))))

(deftest test-query-strings
  (testing "Query strings are parsed correctly"
    (with-server [_server (test-server (fn [{:keys [query-string]}]
                                         {:status 200
                                          :headers {"content-type" "text/plain"}
                                          :body (str "Query: " (or query-string "none"))}))]
      (let [response (req :get "/test?foo=bar&baz=qux")]
        (is (= 200 (:status response)))
        (is (re-find #"Query: foo=bar&baz=qux" (:body response))))

      (let [response (req :get "/test")]
        (is (= 200 (:status response)))
        (is (re-find #"Query: none" (:body response)))))))

(deftest test-request-info
  (testing "Request info is correctly populated"
    (let [request-info (atom nil)]
      (with-server [_server (test-server (fn [req]
                                           (reset! request-info req)
                                           {:status 200 :body "OK"}))]
        (req :get "/test?foo=bar")
        (Thread/sleep 100)
        (let [req @request-info]
          (is (= :get (:request-method req)))
          (is (= "/test" (:uri req)))
          (is (= "foo=bar" (:query-string req)))
          (is (= "127.0.0.1" (:server-name req)))
          (is (= (plain-port) (:server-port req)))
          (is (= :http (:scheme req)))
          (is (string? (:protocol req)))
          (is (map? (:headers req))))))))

(deftest test-ipv6-request-info
  (testing "IPv6 listeners accept requests and expose correct Ring metadata"
    (let [port (util/free-port)
          request-info (atom nil)]
      (with-server [_server (busker/start!
                             (util/with-handler
                               (fn [req]
                                 (if (= "/__ready" (:uri req))
                                   {:status 200
                                    :body "ready"}
                                   (do
                                     (reset! request-info req)
                                     {:status 200
                                      :body "hello-ipv6"})))
                               {:entrypoints {:ipv6 {:bind (str "[::1]:" port)
                                                     :http3? false
                                                     :tls false}}}))]
        (let [ready (util/wait-for-curl-ready! :http :h1 port "/__ready"
                                               :host "::1"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "IPv6 listener should become ready. stderr: " (:err ready))))
        (let [response (util/curl :http :h1 port "/ipv6"
                                  :host "::1"
                                  :max-time 5)]
          (is (= 0 (:exit response))
              (str "IPv6 request should succeed. stderr: " (:err response)))
          (is (= "hello-ipv6" (:out response))))
        (Thread/sleep 100)
        (let [req @request-info]
          (is (= "/ipv6" (:uri req)))
          (is (= "::1" (:server-name req)))
          (is (= port (:server-port req)))
          (is (contains? #{"::1" "0:0:0:0:0:0:0:1"} (:remote-addr req))))))))

(deftest persistent-session-ticket-keys-survive-restart-test
  (testing "top-level tls storage persists session ticket keys across restart"
    (let [port (util/free-port)
          storage-root (str (Files/createTempDirectory "busker-session-storage"
                                                       (make-array java.nio.file.attribute.FileAttribute 0)))
          storage (file-storage/file-storage {:root storage-root})
          config (util/with-handler
                   (fn [_] {:status 200 :body "ok"})
                   (util/with-static-tls
                     {:tls {:storage storage
                            :session-tickets {:persistence :storage}}
                      :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}}))
          server-a (busker/start! config)]
      (try
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "TLS listener should become ready before restart. stderr: "
                   (:err ready))))
        (is (.exists (io/file storage-root "busker/session_tickets/keys.edn")))
        (let [keys-a (active-generation-keys-edn server-a)]
          (is (seq keys-a))
          (busker/stop! server-a)
          (let [server-b (busker/start! config)]
            (try
              (let [ready (util/wait-for-curl-ready! :https :h1 port "/"
                                                     :max-time 2)]
                (is (= 0 (:exit ready))
                    (str "TLS listener should become ready after restart. stderr: "
                         (:err ready))))
              (is (= keys-a
                     (active-generation-keys-edn server-b)))
              (finally
                (busker/stop! server-b)))))
        (finally
          (when (= :running (:phase (busker/state server-a)))
            (busker/stop! server-a)))))))

(deftest corrupt-persistent-session-ticket-file-fails-startup-test
  (testing "storage-backed startup fails against a corrupt persisted session ticket file"
    (let [port (util/free-port)
          storage-root (str (Files/createTempDirectory "busker-session-corrupt"
                                                       (make-array java.nio.file.attribute.FileAttribute 0)))
          storage (file-storage/file-storage {:root storage-root})
          ticket-path (io/file storage-root "busker/session_tickets/keys.edn")
          _ (.mkdirs (.getParentFile ticket-path))
          _ (spit ticket-path "{corrupt")
          config (util/with-handler
                   (fn [_] {:status 200 :body "ok"})
                   (util/with-static-tls
                     {:tls {:storage storage
                            :session-tickets {:persistence :storage}}
                      :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}}))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (busker/start! config))))))

(deftest persistent-session-ticket-resumption-survives-restart-test
  (testing "a saved TLS session resumes after restart when storage-backed tickets are enabled"
    (let [port (util/free-port)
          session-file (str (System/getProperty "java.io.tmpdir")
                            "/busker-restart-session-" port ".pem")
          storage-root (str (Files/createTempDirectory "busker-session-handshake-restart"
                                                       (make-array java.nio.file.attribute.FileAttribute 0)))
          storage (file-storage/file-storage {:root storage-root})
          config (util/with-handler
                   (fn [_] {:status 200 :body "ok"})
                   (util/with-static-tls
                     {:tls {:storage storage
                            :session-tickets {:persistence :storage}}
                      :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}}))
          server-a (busker/start! config)]
      (io/delete-file session-file true)
      (try
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
          (is (= 0 (:exit ready))))
        (let [r1 (util/openssl-session-handshake port :tls1.2 session-file true)]
          (is (= 0 (:exit r1)))
          (is (:new? r1)))
        (is (.exists (io/file session-file)))
        (busker/stop! server-a)
        (let [server-b (busker/start! config)]
          (try
            (let [ready (util/wait-for-curl-ready! :https :h1 port "/" :max-time 2)]
              (is (= 0 (:exit ready))))
            (let [r2 (util/openssl-session-handshake port :tls1.2 session-file false)]
              (is (= 0 (:exit r2))
                  (str "TLS 1.2 restart resume should succeed. output: " (:out r2)))
              (is (:reused? r2)
                  (str "TLS 1.2 restart resume should reuse the session. output: "
                       (:out r2))))
            (finally
              (busker/stop! server-b))))
        (finally
          (when (= :running (:phase (busker/state server-a)))
            (busker/stop! server-a))
          (io/delete-file session-file true))))))

(deftest persistent-session-ticket-resumption-works-across-peer-processes-test
  (testing "a saved TLS session resumes against a peer Busker process sharing the same storage root"
    (let [port-a (util/free-port)
          port-b (loop [candidate (util/free-port)]
                   (if (= candidate port-a)
                     (recur (util/free-port))
                     candidate))
          session-file (str (System/getProperty "java.io.tmpdir")
                            "/busker-peer-session-" port-a "-" port-b ".pem")
          storage-root (str (Files/createTempDirectory "busker-session-handshake-peer"
                                                       (make-array java.nio.file.attribute.FileAttribute 0)))
          storage (file-storage/file-storage {:root storage-root})
          make-config (fn [port body]
                        (util/with-handler
                          (fn [_] {:status 200 :body body})
                          (util/with-static-tls
                            {:tls {:storage storage
                                   :session-tickets {:persistence :storage}}
                             :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                 :http3? false
                                                 :tls {:tls-compatibility-mode :modern}}}})))
          server-a (busker/start! (make-config port-a "a"))
          server-b (busker/start! (make-config port-b "b"))]
      (io/delete-file session-file true)
      (try
        (let [ready-a (util/wait-for-curl-ready! :https :h1 port-a "/" :max-time 2)
              ready-b (util/wait-for-curl-ready! :https :h1 port-b "/" :max-time 2)]
          (is (= 0 (:exit ready-a)))
          (is (= 0 (:exit ready-b))))
        (let [r1 (util/openssl-session-handshake port-a :tls1.2 session-file true)]
          (is (= 0 (:exit r1)))
          (is (:new? r1)))
        (is (.exists (io/file session-file)))
        (let [r2 (util/openssl-session-handshake port-b :tls1.2 session-file false)]
          (is (= 0 (:exit r2))
              (str "Peer TLS 1.2 resume should succeed. output: " (:out r2)))
          (is (:reused? r2)
              (str "Peer TLS 1.2 resume should reuse the session. output: "
                   (:out r2))))
        (finally
          (busker/stop! server-a)
          (busker/stop! server-b)
          (io/delete-file session-file true))))))

(deftest tls-storage-alone-does-not-enable-ticket-persistence-test
  (testing "top-level tls storage without persistence selector remains memory only"
    (let [port (util/free-port)
          storage-root (str (Files/createTempDirectory "busker-session-memory"
                                                       (make-array java.nio.file.attribute.FileAttribute 0)))
          storage (file-storage/file-storage {:root storage-root})
          config (util/with-handler
                   (fn [_] {:status 200 :body "ok"})
                   (util/with-static-tls
                     {:tls {:storage storage}
                      :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls {:tls-compatibility-mode :modern}}}}))
          server-a (busker/start! config)]
      (try
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "TLS listener should become ready before restart. stderr: "
                   (:err ready))))
        (let [keys-a (active-generation-keys-edn server-a)]
          (busker/stop! server-a)
          (let [server-b (busker/start! config)]
            (try
              (let [ready (util/wait-for-curl-ready! :https :h1 port "/"
                                                     :max-time 2)]
                (is (= 0 (:exit ready))
                    (str "TLS listener should become ready after restart. stderr: "
                         (:err ready))))
              (is (not= keys-a
                        (active-generation-keys-edn server-b)))
              (is (not (.exists (io/file storage-root "busker/session_tickets/keys.edn"))))
              (finally
                (busker/stop! server-b)))))
        (finally
          (when (= :running (:phase (busker/state server-a)))
            (busker/stop! server-a)))))))

(deftest disabled-session-tickets-skip-native-ticket-manager-test
  (testing "disabled session tickets do not create ticket manager state"
    (let [port (util/free-port)
          server (busker/start!
                  (util/with-handler
                    (fn [_] {:status 200 :body "ok"})
                    (util/with-static-tls
                      {:tls {:session-tickets {:disabled? true}}
                       :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                           :http3? false
                                           :tls {:tls-compatibility-mode :modern}}}})))]
      (try
        (let [ready (util/wait-for-curl-ready! :https :h1 port "/"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "TLS listener should become ready. stderr: " (:err ready))))
        (let [generation (active-generation-instance server)]
          (is (nil? (::generation/key-manager generation)))
          (is (nil? (::generation/native-ticket-mgr generation))))
        (finally
          (busker/stop! server))))))

(deftest test-unix-socket-listener
  (testing "Unix socket listeners serve requests and clean up their socket path"
    (let [path (util/temp-unix-socket-path)
          server (busker/start!
                  (util/with-handler
                    (fn [{:keys [uri]}]
                      (if (= "/__ready" uri)
                        {:status 200
                         :body "ready"}
                        {:status 200
                         :body "hello-unix"}))
                    {:entrypoints {:unix {:bind (str "unix:" path)
                                          :http3? false
                                          :tls false}}}))]
      (try
        (let [ready (util/wait-for-curl-ready! :http :h1 nil "/__ready"
                                               :unix-socket path
                                               :host "localhost"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "Unix socket listener should become ready. stderr: " (:err ready))))
        (is (.exists (File. path))
            "Filesystem unix socket should exist while the server is running")
        (let [response (util/curl :http :h1 nil "/unix"
                                  :unix-socket path
                                  :host "localhost"
                                  :max-time 5)]
          (is (= 0 (:exit response))
              (str "Unix socket request should succeed. stderr: " (:err response)))
          (is (= "hello-unix" (:out response))))
        (finally
          (busker/stop! server)))
      (is (not (.exists (File. path)))
          "Filesystem unix socket path should be removed after stop"))))

(deftest test-ring-body-types
  (testing "All Ring StreamableResponseBody types work correctly"
    (with-server [_server (test-server (fn [{:keys [uri]}]
                                         (case uri
                                           "/string" {:status 200
                                                      :headers {"content-type" "text/plain"}
                                                      :body "Hello String"}

                                           "/bytes" {:status 200
                                                     :headers {"content-type" "application/octet-stream"}
                                                     :body (.getBytes "Hello Bytes" "UTF-8")}

                                           "/seq" {:status 200
                                                   :headers {"content-type" "text/plain"}
                                                   :body (lazy-abcs 27)}

                                           "/file" (let [temp-file (java.io.File/createTempFile "test" ".txt")]
                                                     (.deleteOnExit temp-file)
                                                     (spit temp-file "Hello from file")
                                                     {:status 200
                                                      :headers {"content-type" "text/plain"}
                                                      :body temp-file})

                                           "/stream" (let [data "Hello from stream"
                                                           input-stream (java.io.ByteArrayInputStream. (.getBytes data "UTF-8"))]
                                                       {:status 200
                                                        :headers {"content-type" "text/plain"}
                                                        :body input-stream})

                                           "/nil-body" {:status 200
                                                        :headers {"content-type" "text/plain"}
                                                        :body nil}

                                           "/no-body" {:status 204
                                                       :headers {}}

                                           {:status 404})))]
      (testing "String body"
        (let [response (req :get "/string")]
          (is (= 200 (:status response)))
          (is (= "Hello String" (:body response)))))

      (testing "Byte array body"
        (let [response (req :get "/bytes")]
          (is (= 200 (:status response)))
          (is (= "Hello Bytes" (:body response)))))

      (testing "Sequence body"
        (let [response (req :get "/seq")]
          (is (= 200 (:status response)))
          (is (= "abcdefghijklmnopqrstuvwxyza" (:body response)))))

      (testing "File body"
        (let [response (req :get "/file")]
          (is (= 200 (:status response)))
          (is (= "Hello from file" (:body response)))))

      (testing "InputStream body"
        (let [response (req :get "/stream")]
          (is (= 200 (:status response)))
          (is (= "Hello from stream" (:body response)))))

      (testing "Nil body"
        (let [response (req :get "/nil-body")]
          (is (= 200 (:status response)))
          (is (= "" (:body response)))))

      (testing "No body (204)"
        (let [response (req :get "/no-body")]
          (is (= 204 (:status response)))
          (is (= "" (:body response))))))))

(defn- load-fixture-certificate
  ^X509Certificate
  []
  (with-open [in (io/input-stream (util/fixture-cert-path))]
    (.generateCertificate (CertificateFactory/getInstance "X.509") in)))

(deftest test-tls-listener
  (testing "TLS listener with HTTPS connections"
    (with-server [_server (test-server
                           (fn [{:keys [scheme uri]}]
                             {:status  200
                              :headers {"content-type" "text/plain"}
                              :body    (str "Hello via " (name scheme) " at " uri)})
                           :entrypoints {:plain {:bind (str "127.0.0.1:" (plain-port))
                                                 :http3? false
                                                 :tls false}
                                         :tls {:bind (str "127.0.0.1:" (tls-port))
                                               :http3? false
                                               :tls {:tls-compatibility-mode
                                                     :modern}}}
                           :tls util/static-tls)]
      (testing "plaintext HTTP endpoint works"
        (let [response (req :get "/hello")]
          (is (= 200 (:status response)))
          (is (= "Hello via http at /hello" (:body response)))))

      (testing "HTTPS endpoint works"
        (let [result (util/curl :https :h1 (tls-port) "/secure")]
          (is (= 0 (:exit result)) "HTTPS request should succeed")
          (is (re-find #"Hello via https at /secure" (:out result)) "HTTPS should return expected response")))

      (testing "HTTPS with HTTP/2 ALPN negotiation"
        (let [result (util/curl :https :h2 (tls-port) "/h2" :args ["-v"])]
          (is (= 0 (:exit result)) "HTTP/2 request should succeed")
          (is (re-find #"Hello via https at /h2" (:out result)) "HTTP/2 should return expected response")
          (is (re-find #"ALPN.*h2" (:err result)) "Should negotiate HTTP/2 via ALPN"))))))

;; TODO: Implement these tests once streaming support is complete
#_(deftest test-exceptions
    (testing "Throwing an exception in a handler causes a 500"
      (with-server [_server (test-server (fn [_]
                                           (throw (RuntimeException. "elephant"))))]
        (let [response (req :get "/")]
          (is (= 500 (:status response)))
          (is (re-find #"elephant" (:body response))))))

    (testing "Throwing an exception in the middle of writing StreamableResponseBody cannot turn a 200 into a 500"
      (with-server [_server (test-server (fn [_]
                                           {:status 200
                                            :body   (take 5 (concat ["chunk1" "chunk2" "chunk3"]
                                                                    (lazy-seq (throw (Exception. "Failed after 3 chunks")))))}))]
        (let [response (req :get "/")]
          (println (:body response))
          (is (= 200 (:status response)))))))

(deftest test-async
  (testing "async response"
    (with-server [_ (test-server (fn [{emitter :ol.busker.request/emitter}]
                                   (future
                                     (h2o/emit! emitter {:status 200 :body "Hello Async"}))
                                   {:body emitter})
                                 {:entrypoints {:plain {:bind (str "127.0.0.1:" (plain-port))
                                                        :http3? false
                                                        :tls false}
                                                :tls {:bind (str "127.0.0.1:" (tls-port))
                                                      :http3? false
                                                      :tls {:tls-compatibility-mode
                                                            :modern}}}
                                  :tls util/static-tls})]
      (let [{:keys [status body]} (req :get "/")]
        (is (= 200 status))
        (is (= "Hello Async" body)))))
  (testing "103 early hints" ;; 103 early hints requires h2 or h3
    (with-server [_ (test-server (fn [{emitter :ol.busker.request/emitter}]
                                   (future
                                     (h2o/emit! emitter {:status 103 :headers {"Link" "</style.css>; rel=preload; as=style"}})
                                     (h2o/emit! emitter {:status 200 :headers {"content-type" "text/html"}})
                                     (h2o/emit! emitter "<!doctype html><h1>Hello world</h1>")
                                     (h2o/close emitter))
                                   {:body emitter})
                                 :entrypoints {:plain {:bind (str "127.0.0.1:" (plain-port))
                                                       :http3? false
                                                       :tls false}
                                               :tls {:bind (str "127.0.0.1:" (tls-port))
                                                     :http3? false
                                                     :tls {:tls-compatibility-mode
                                                           :modern}}}
                                 :tls util/static-tls)]

      (let [result (util/curl :https :h2 (tls-port) "/" :args ["-v"])]
        (is (= 0 (:exit result)))
        (is (re-find #"HTTP/2 103" (:err result)))
        (is (re-find #"link: </style.css>; rel=preload; as=style" (:err result)))
        (is (re-find #"HTTP/2 200" (:err result)))
        (is (re-find #"<!doctype html><h1>Hello world</h1>" (:out result))))))

  (testing "sse"
    (with-server [_ (test-server (fn [{emitter :ol.busker.request/emitter}]
                                   (future
                                     (h2o/emit! emitter {:status 200 :headers {"content-type" "text/event-stream" "connection" "keep-alive"}})
                                     (h2o/emit! emitter "event: hello\ndata: first\n\n")
                                     (h2o/flush emitter)
                                     (h2o/emit! emitter "event: close\ndata:\n\n")
                                     (h2o/close emitter))
                                   {:body emitter}))]
      (let [{:keys [headers status body]} (req :get "/")]
        (is (= "text/event-stream" (get headers "content-type")))
        (is (= 200 status))
        (is (= "event: hello\ndata: first\n\nevent: close\ndata:\n\n" body))))))

(deftest tcp-connection-limit-test
  (testing "TCP connections respect global max-connections limit"
    (let [port (util/free-port)
          stream-duration-ms 3000
          chunk-interval-ms 200
          handler (fn [{:keys [uri]
                        emitter :ol.busker.request/emitter}]
                    (if (= "/ready" uri)
                      {:status 200
                       :body "ready"}
                      (do
                        (future
                          (h2o/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                          (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                            (doseq [idx (range num-chunks)]
                              (h2o/emit! emitter (str "chunk-" idx "-"))
                              (h2o/flush emitter)
                              (Thread/sleep chunk-interval-ms)))
                          (h2o/close emitter))
                        {:body emitter})))
          server (busker/start!
                  (util/with-handler
                    handler
                    {:max-connections 2
                     :entrypoints {:test {:bind (str "127.0.0.1:" port)
                                          :http3? false
                                          :tls false}}}))]
      (try
        (let [ready (util/wait-for-curl-ready! :http :h1 port "/ready"
                                               :max-time 2)]
          (is (= 0 (:exit ready))
              (str "HTTP/1 readiness check should succeed. stderr: "
                   (:err ready))))
        ;; Start 2 long-lived HTTP/1.1 connections
        (let [conn1 (future (util/curl :http :h1 port "/" :max-time 10))
              conn2 (future (util/curl :http :h1 port "/" :max-time 10))]
          ;; Wait a bit for connections to establish
          (Thread/sleep 500)

          ;; Third connection should fail (connection refused or timeout)
          (let [conn3 (util/curl :http :h1 port "/" :max-time 1)]
            (is (not= 0 (:exit conn3))
                "Third TCP connection should fail when at limit"))

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
          (let [conn4 (util/wait-for-curl-ready! :http :h1 port "/"
                                                 :max-time 5)]
            (is (zero? (:exit conn4))
                (str "New TCP connection should succeed after others close. stderr: " (:err conn4)))))
        (finally
          (busker/stop! server))))))

(deftest tcp-and-tls-share-limit-test
  (testing "TCP and TLS connections share the global limit"
    (let [http-port (util/free-port)
          https-port (loop [port (util/free-port)]
                       (if (= port http-port)
                         (recur (util/free-port))
                         port))
          stream-duration-ms 3000
          chunk-interval-ms 200
          handler (fn [{emitter :ol.busker.request/emitter}]
                    (future
                      (h2o/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                      (let [num-chunks (/ stream-duration-ms chunk-interval-ms)]
                        (doseq [idx (range num-chunks)]
                          (h2o/emit! emitter (str "chunk-" idx "-"))
                          (h2o/flush emitter)
                          (Thread/sleep chunk-interval-ms)))
                      (h2o/close emitter))
                    {:body emitter})
          server (busker/start!
                  (util/with-static-tls
                    (util/with-handler
                      handler
                      {:max-connections 3
                       :entrypoints {:http {:bind (str "127.0.0.1:" http-port)
                                            :http3? false
                                            :tls false}
                                     :https {:bind (str "127.0.0.1:" https-port)
                                             :http3? false
                                             :tls {:tls-compatibility-mode
                                                   :modern}}}})))]
      (try
        (Thread/sleep 200)
        ;; Start 2 HTTP connections and 1 HTTPS connection
        (let [http-conn1 (future (util/curl :http :h1 http-port "/" :max-time 10))
              http-conn2 (future (util/curl :http :h1 http-port "/" :max-time 10))
              https-conn (future (util/curl :https :h2 https-port "/" :max-time 10))]
          ;; Wait a bit for connections to establish
          (Thread/sleep 500)

          ;; Fourth connection should fail
          (let [conn4 (util/curl :http :h1 http-port "/" :max-time 1)]
            (is (not= 0 (:exit conn4))
                "Fourth connection should fail when at limit"))

          ;; Wait for all connections to complete
          (let [result1 (deref http-conn1 15000 nil)
                result2 (deref http-conn2 15000 nil)
                result3 (deref https-conn 15000 nil)]
            (is (some? result1) "First HTTP connection should complete")
            (is (some? result2) "Second HTTP connection should complete")
            (is (some? result3) "HTTPS connection should complete")
            (when result1
              (is (zero? (:exit result1))
                  (str "First HTTP connection should succeed. stderr: " (:err result1))))
            (when result3
              (is (zero? (:exit result3))
                  (str "HTTPS connection should succeed. stderr: " (:err result3))))))
        (finally
          (busker/stop! server))))))

(deftest managed-clave-lifecycle-test
  (testing "server starts/stops managed clave runtime with server lifecycle"
    (let [calls (atom [])
          plan {:subject-names ["example.com"]
                :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}}
          runtime {:system {:id ::managed-system}
                   :subject-names ["example.com"]
                   :http-solver {:registry (atom {})}
                   :lookup-fn (fn [_] nil)}]
      (with-redefs [clave-adapter/build-managed-plan (fn [config]
                                                       (swap! calls conj
                                                              [:build (get-in config
                                                                              [:tls
                                                                               :certificates
                                                                               :manage])])
                                                       plan)
                    clave-adapter/start! (fn [managed-plan]
                                           (swap! calls conj [:start managed-plan])
                                           runtime)
                    clave-adapter/wrap-handler (fn [handler managed-runtime]
                                                 (swap! calls conj [:wrap managed-runtime])
                                                 handler)
                    clave-adapter/stop! (fn [managed-runtime]
                                          (swap! calls conj [:stop managed-runtime])
                                          nil)]
        (with-server [_server (test-server (fn [_] {:status 200 :body "ok"})
                                           :entrypoints {:test-plain
                                                         {:bind (str "127.0.0.1:" (plain-port))
                                                          :http3? false
                                                          :tls false}}
                                           :tls {:certificates {:manage ["example.com"]}
                                                 :issuers [{:directory-url
                                                            "https://acme.example/directory"}]})]
          (is (= 200 (:status (req :get "/")))))
        (is (= [[:build ["example.com"]]
                [:start plan]
                [:wrap runtime]
                [:stop runtime]]
               @calls))))))

(deftest tls-lookup-prefers-static-certificate-test
  (testing "lookup function returns static cert/key material before clave lookup"
    (let [clave-calls (atom 0)
          config {:tls util/static-tls
                  :entrypoints {:https {:bind "127.0.0.1:8443"
                                        :http3? false
                                        :tls {:tls-compatibility-mode
                                              :modern}}}}
          lookup-fn (with-redefs [clave-adapter/lookup-certificate (fn [_runtime _hostname]
                                                                     (swap! clave-calls inc)
                                                                     nil)]
                      (#'generation/build-tls-lookup-fn config {:system {:id ::runtime}}))
          result (lookup-fn "localhost.examp1e.net")]
      (is (= 0 @clave-calls) "Static cert should short-circuit clave lookup")
      (is (string? (:cert-chain-pem result)))
      (is (string? (:private-key-pem result)))
      (is (str/includes? (:cert-chain-pem result) "BEGIN CERTIFICATE"))
      (is (or (str/includes? (:private-key-pem result) "BEGIN PRIVATE KEY")
              (str/includes? (:private-key-pem result) "BEGIN RSA PRIVATE KEY"))))))

(deftest tls-lookup-falls-back-to-clave-test
  (testing "lookup function uses clave bundle when no static cert material exists"
    (let [runtime {:system {:id ::runtime}}
          cert (load-fixture-certificate)
          keypair (clave-certificate/keypair :p256)
          config {:entrypoints {:https {:bind "127.0.0.1:8443"
                                        :http3? false
                                        :tls {:tls-compatibility-mode
                                              :modern}}}}
          lookup-fn (with-redefs [clave-adapter/lookup-certificate
                                  (fn [_ hostname]
                                    (when (= hostname "hit.example")
                                      {:certificate [cert]
                                       :private-key (.getPrivate keypair)}))]
                      (#'generation/build-tls-lookup-fn config runtime))]
      (is (nil? (lookup-fn "miss.example")))
      (let [result (lookup-fn "hit.example")]
        (is (string? (:cert-chain-pem result)))
        (is (string? (:private-key-pem result)))
        (is (str/includes? (:cert-chain-pem result) "BEGIN CERTIFICATE"))
        (is (str/includes? (:private-key-pem result) "BEGIN PRIVATE KEY"))))))

(deftest tls-lookup-no-sni-uses-first-static-certificate-test
  (testing "lookup function uses the first static certificate when SNI is absent"
    (let [config {:tls util/static-tls
                  :entrypoints {:https {:bind "127.0.0.1:8443"
                                        :http3? false
                                        :tls {:tls-compatibility-mode
                                              :modern}}}}
          lookup-fn (#'generation/build-tls-lookup-fn config nil)
          result (lookup-fn nil)]
      (is (= {:cert-chain-pem (util/fixture-cert-pem)
              :private-key-pem (util/fixture-key-pem)}
             result)))))

(deftest tls-lookup-no-sni-uses-first-managed-subject-test
  (testing "lookup function routes missing SNI to the first managed subject name"
    (let [calls (atom [])
          expected-material {:cert-chain-pem (util/fixture-cert-pem)
                             :private-key-pem (util/fixture-key-pem)}
          config {:entrypoints {:https {:bind "127.0.0.1:8443"
                                        :http3? false
                                        :tls {:tls-compatibility-mode
                                              :modern}}}}
          lookup-fn (#'generation/build-tls-lookup-fn
                     config
                     {:subject-names ["fallback.example"]
                      :lookup-fn (fn [hostname]
                                   (swap! calls conj hostname)
                                   (when (= hostname "fallback.example")
                                     (util/fixture-tls-bundle)))})
          result (lookup-fn nil)]
      (is (= ["fallback.example"] @calls))
      (is (= expected-material result)))))

(deftest tls-lookup-no-sni-miss-without-fallback-test
  (testing "lookup function returns miss when SNI is missing and no fallback material exists"
    (let [calls (atom [])
          config {:entrypoints {:https {:bind "127.0.0.1:8443"
                                        :http3? false
                                        :tls {:tls-compatibility-mode
                                              :modern}}}}
          lookup-fn (#'generation/build-tls-lookup-fn
                     config
                     {:lookup-fn (fn [hostname]
                                   (swap! calls conj hostname)
                                   (util/fixture-tls-bundle))})]
      (is (nil? (lookup-fn nil)))
      (is (empty? @calls)))))

(deftest tls-lookup-callback-lifecycle-test
  (testing "server builds lookup callback at startup"
    (let [calls (atom [])]
      (with-redefs [native/build-tls-lookup-callback
                    (fn [lookup-fn]
                      (swap! calls conj [:register (ifn? lookup-fn)])
                      {:lookup-fn lookup-fn
                       :callback-ptr (mem/as-segment 1)
                       :callback (fn [& _] nil)})
                    clave-adapter/build-managed-plan (fn [_] nil)
                    clave-adapter/start! (fn [_] nil)
                    clave-adapter/wrap-handler (fn [handler _] handler)
                    clave-adapter/stop! (fn [_] nil)]
        (with-server [_server (test-server (fn [_] {:status 200 :body "ok"})
                                           :entrypoints {:plain {:bind (str "127.0.0.1:" (plain-port))
                                                                 :http3? false
                                                                 :tls false}
                                                         :tls {:bind (str "127.0.0.1:" (tls-port))
                                                               :http3? false
                                                               :tls {:tls-compatibility-mode
                                                                     :modern}}}
                                           :tls util/static-tls)]
          (is (= 200 (:status (req :get "/")))))
        (is (= 1 (count (filter #(= :register (first %)) @calls))))))))

(deftest tcp-tls-no-sni-falls-back-to-managed-subject-test
  (testing "TCP TLS handshake without SNI succeeds using the first managed subject name"
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
        (with-server [_server (busker/start!
                               {:tls {:certificates {:manage ["fallback.example"]}
                                      :issuers [{:directory-url
                                                 "https://acme.example/directory"}]}
                                :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                    :http3? false
                                                    :tls {:tls-compatibility-mode
                                                          :modern}}}
                                :dispatch [{:handler (fn [_]
                                                       {:status 200
                                                        :body "fallback-ok"})}]})]
          (Thread/sleep 200)
          (let [result (openssl-no-sni-request port "/")]
            (is (= {:exit 0
                    :status-line? true
                    :body? true}
                   {:exit (:exit result)
                    :status-line? (str/includes? (:out result) "HTTP/1.1 200")
                    :body? (str/includes? (:out result) "fallback-ok")})
                (str "No-SNI handshake should succeed. stderr: "
                     (:err result)
                     " output: "
                     (:out result)))))))))

(deftest tcp-tls-no-sni-miss-without-fallback-test
  (testing "TCP TLS handshake without SNI fails when no fallback material exists"
    (let [port (util/free-port)
          runtime {:system {:id ::runtime}
                   :subject-names ["fallback.example"]
                   :lookup-fn (fn [_hostname] nil)}]
      (with-redefs [clave-adapter/build-managed-plan
                    (fn [_]
                      {:subject-names ["fallback.example"]
                       :clave-config {:issuers [{:directory-url "https://acme.example/directory"}]}})
                    clave-adapter/start! (fn [_] runtime)
                    clave-adapter/wrap-handler (fn [handler _] handler)
                    clave-adapter/stop! (fn [_] nil)]
        (with-server [_server (busker/start!
                               {:tls {:certificates {:manage ["fallback.example"]}
                                      :issuers [{:directory-url
                                                 "https://acme.example/directory"}]}
                                :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                                    :http3? false
                                                    :tls {:tls-compatibility-mode
                                                          :modern}}}
                                :dispatch [{:handler (fn [_]
                                                       {:status 200
                                                        :body "unexpected"})}]})]
          (Thread/sleep 200)
          (let [result (openssl-no-sni-request port "/")]
            (is (not= 0 (:exit result))
                (str "No-SNI handshake should fail without fallback material. output: "
                     (:out result)))))))))
