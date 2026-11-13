(ns ol.busker.native-test
  (:require
   [babashka.ffi :as ffi]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker.native :as native]
   [ol.busker.native.loader :as loader])
  (:import
   [java.io File]
   [java.lang.foreign MemorySegment]
   [java.util.concurrent TimeUnit]
   [java.nio.file Files Path]))

(defn- run-loader-process [form & jvm-options]
  (let [output (File/createTempFile "busker-loader-" ".log")
        process (-> (ProcessBuilder.
                     ^"[Ljava.lang.String;"
                     (into-array String
                                 (concat [(str (System/getProperty "java.home") "/bin/java")
                                          "--enable-native-access=ALL-UNNAMED"]
                                         jvm-options
                                         ["-cp" (System/getProperty "java.class.path")
                                          "clojure.main" "-e" (pr-str form)])))
                    (.redirectErrorStream true)
                    (.redirectOutput output)
                    (.start))]
    (try
      (let [finished? (.waitFor process 60 TimeUnit/SECONDS)]
        (is finished? (slurp output))
        (when finished?
          (is (zero? (.exitValue process)) (slurp output))))
      (finally
        (.destroyForcibly process)
        (.delete output)))))

(deftest namespace-reload-retains-loaded-library
  (run-loader-process
   '(do
      (require '[babashka.ffi :as ffi])
      (let [load-library ffi/load-library
            loaded (atom [])]
        (with-redefs [ffi/load-library (fn [path]
                                         (let [library (load-library path)]
                                           (swap! loaded conj library)
                                           library))]
          (require '[ol.busker.native :as native]
                   '[ol.busker :as busker]
                   '[ol.busker.test-utils :as util]
                   '[babashka.http-client :as http])
          (eval
           '(let [size (native/req-ctx-size)
                  old-call (ffi/cfn "clj_h2o_req_ctx_size" [] :long)
                  port (util/free-port)
                  server (busker/start!
                          (util/with-handler
                            (fn [_] {:status 200 :body "still running"})
                            {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                                  :tls false :http3? false}}}))
                  url (str "http://127.0.0.1:" port)]
              (try
                (assert (= "still running" (:body (http/get url))))
                (dotimes [_ 2]
                  (require 'ol.busker.native.loader :reload)
                  (require 'ol.busker.native :reload)
                  (assert (= size (old-call) (native/req-ctx-size)
                             ((ffi/cfn "clj_h2o_req_ctx_size" [] :long))))
                  (assert (= "still running" (:body (http/get url)))))
                (finally
                  (busker/stop! server)))))
          (assert (= 1 (count @loaded)) (str "Loaded libraries: " (mapv :path @loaded)))
          (assert (not (.exists (java.io.File. (:path (first @loaded))))))))
      (shutdown-agents))))

(deftest bundled-library-remains-callable-after-extraction
  (let [{:keys [path] :as library} (loader/load-bundled-library)
        request-context-size (ffi/cfn library "clj_h2o_req_ctx_size" [] :long)]
    (is (not (Files/exists (Path/of path (make-array String 0))
                           (make-array java.nio.file.LinkOption 0))))
    (is (= (ffi/sizeof native/ffi-clj-req-ctx-t)
           (request-context-size)))
    (is (thrown? clojure.lang.ExceptionInfo
                 ((ffi/cfn library "clj_h2o_absent_symbol" [] :long))))))

(deftest native-library-override-and-failures
  (let [library-name (if (str/includes? (loader/get-os-arch) "macos")
                       "libh2oclj.dylib" "libh2oclj.so")
        path (Files/createTempFile "h2oclj-override-" library-name
                                   (make-array java.nio.file.attribute.FileAttribute 0))
        missing (str path "-missing")]
    (try
      (loader/copy-resource (str (loader/get-os-arch) "/" library-name) (str path))
      (run-loader-process
       '(do
          (require '[ol.busker.native :as native]
                   '[ol.busker.native.loader :as loader])
          (assert (= (System/getProperty "ol.libh2oclj.path") (:path loader/native-library)))
          (assert (pos? (native/req-ctx-size)))
          (System/setProperty "ol.libh2oclj.path" "no-such-library-after-initial-load")
          (require 'ol.busker.native.loader :reload)
          (require 'ol.busker.native :reload)
          (assert (pos? (native/req-ctx-size))))
       (str "-Dol.libh2oclj.path=" path))
      (run-loader-process
       '(try
          (require 'ol.busker.native.loader)
          (throw (AssertionError. "Invalid initial library path was accepted"))
          (catch clojure.lang.Compiler$CompilerException e
            (assert (= (System/getProperty "ol.libh2oclj.path")
                       (:library (ex-data (ex-cause e)))))))
       (str "-Dol.libh2oclj.path=" missing))
      (finally
        (Files/deleteIfExists path)))))

(deftest tls-callback-copies-hostname-and-returned-bytes
  (with-open [arena (ffi/shared-arena)]
    (let [seen (atom nil)
          {:keys [callback-ptr]}
          (native/build-tls-lookup-callback
           (fn [hostname]
             (reset! seen hostname)
             (when hostname
               {:cert-chain-pem "CERT" :private-key-pem "KEY"})))
          invoke (ffi/cfn callback-ptr
                          [:pointer :long :pointer :pointer :pointer :pointer :pointer] :int)
          [cert-out cert-len-out key-out key-len-out]
          (repeatedly 4 #(ffi/alloc arena 8))
          hostname (ffi/string->ptr arena "example.test")]
      (is (= 1 (invoke hostname 12 cert-out cert-len-out key-out key-len-out ffi/null)))
      (is (= "example.test" @seen))
      (let [cert (ffi/read cert-out :pointer)
            key (ffi/read key-out :pointer)]
        (try
          (is (= "CERT" (String. ^bytes (ffi/read-array
                                         (ffi/reinterpret cert (ffi/read cert-len-out :long))
                                         :byte (ffi/read cert-len-out :long)) "UTF-8")))
          (is (= "KEY" (String. ^bytes (ffi/read-array
                                        (ffi/reinterpret key (ffi/read key-len-out :long))
                                        :byte (ffi/read key-len-out :long)) "UTF-8")))
          (finally
            ((ffi/cfn "free" [:pointer] :void) cert)
            ((ffi/cfn "free" [:pointer] :void) key))))
      (is (= 0 (invoke ffi/null 0 cert-out cert-len-out key-out key-len-out ffi/null)))
      (is (nil? @seen))
      (is (= [0 0 0 0] (mapv #(ffi/read % :long)
                             [cert-out cert-len-out key-out key-len-out]))))))

(deftest tls-callback-leaves-copies-for-the-shim-to-free-when-key-copy-throws
  (with-open [arena (ffi/shared-arena)]
    (let [copy-bytes native/tls-bytes-dup
          copies (atom [])
          {:keys [callback-ptr]}
          (native/build-tls-lookup-callback
           (constantly {:cert-chain-pem "CERT" :private-key-pem "KEY"}))
          invoke (ffi/cfn callback-ptr
                          [:pointer :long :pointer :pointer :pointer :pointer :pointer] :int)
          [cert-out cert-len-out key-out key-len-out]
          (repeatedly 4 #(ffi/alloc arena 8))]
      (try
        (with-redefs [native/tls-bytes-dup
                      (fn [value len]
                        (when (seq @copies)
                          (throw (OutOfMemoryError. "key copy")))
                        (let [copy (copy-bytes value len)]
                          (swap! copies conj copy)
                          copy))]
          (is (= -1 (invoke ffi/null 0 cert-out cert-len-out key-out key-len-out ffi/null))))
        (is (= 1 (count @copies)))
        (is (= (.address ^MemorySegment (first @copies))
               (.address ^MemorySegment (ffi/read cert-out :pointer)))
            "the certificate copy stays in its output, where the shim frees it")
        (is (= 4 (ffi/read cert-len-out :long)))
        (is (ffi/null? (ffi/read key-out :pointer)))
        (finally
          (run! (ffi/cfn "free" [:pointer] :void) @copies))))))

(deftest native-struct-sizes-match-shim
  (is (= {:header          32
          :iovec          16
          :fixed-slot      (native/mt-response-slot-data-size)
          :accept-context  (native/accept-ctx-size)
          :sendvec         32
          :h2o-header      40
          :generator       16
          :request-meta    120
          :entrypoint-hosts (native/entrypoint-hosts-size)
          :request-context (native/req-ctx-size)
          :globalconf      336
          :ticket          128}
         {:header          (ffi/sizeof native/ffi-clj-header-t)
          :iovec          (ffi/sizeof native/ffi-h2o-iovec-t)
          :fixed-slot      (ffi/sizeof native/ffi-clj-fixed-response-slot-data-t)
          :accept-context  (ffi/sizeof native/ffi-h2o-accept-ctx-t)
          :sendvec         (ffi/sizeof native/ffi-h2o-sendvec-t)
          :h2o-header      (ffi/sizeof native/ffi-h2o-header-t)
          :generator       (ffi/sizeof native/ffi-h2o-generator-t)
          :request-meta    (ffi/sizeof native/ffi-clj-req-meta-t)
          :entrypoint-hosts 32
          :request-context (ffi/sizeof native/ffi-clj-req-ctx-t)
          :globalconf      (ffi/sizeof native/ffi-clj-h2o-flat-globalconf-t)
          :ticket          (ffi/sizeof native/ffi-clj-session-ticket-t)})))

(defn- ffi-byte-offset [layout path value]
  (with-open [arena (ffi/confined-arena)]
    (let [pointer (ffi/alloc arena layout)]
      (ffi/write pointer (ffi/place layout path) value)
      (first (keep-indexed (fn [i byte] (when-not (zero? byte) i))
                           (ffi/read-array pointer :byte (ffi/sizeof layout)))))))

(deftest native-field-positions-match-shim
  (let [pointer (ffi/segment 1)
        positions [[native/ffi-clj-header-t :value pointer 16]
                   [native/ffi-h2o-iovec-t :len 1 8]
                   [native/ffi-clj-fixed-response-slot-data-t :headers-len 1 24]
                   [native/ffi-clj-fixed-response-slot-data-t :status 1 64]
                   [native/ffi-h2o-accept-ctx-t :ssl_ctx pointer 16]
                   [native/ffi-h2o-sendvec-t :raw pointer 16]
                   [native/ffi-h2o-sendvec-t :cb_arg_padding 1 24]
                   [native/ffi-h2o-header-t :flags 1 32]
                   [native/ffi-h2o-generator-t :stop pointer 8]
                   [native/ffi-clj-req-meta-t :is_early_data 1 102]
                   [native/ffi-clj-req-meta-t :entrypoint 1 104]
                   [native/ffi-clj-req-meta-t :connection_id 1 112]
                   [native/ffi-clj-req-ctx-t [:meta :headers_len] 1 96]
                   [native/ffi-clj-req-ctx-t [:meta :entrypoint] 1 112]
                   [native/ffi-clj-req-ctx-t [:meta :connection_id] 1 120]
                   [native/ffi-clj-req-ctx-t :fixed-slot pointer 160]
                   [native/ffi-clj-req-ctx-t :fixed-claim 1 168]
                   [native/ffi-clj-req-ctx-t :on-response-generator-proceed pointer 192]
                   [native/ffi-clj-req-ctx-t :on-response-generator-stop pointer 200]
                   [native/ffi-clj-h2o-flat-globalconf-t :compress_args_zstd_quality 1 328]
                   [native/ffi-clj-session-ticket-t :not_after 1 120]]]
    (doseq [[layout path value expected] positions]
      (is (= expected (ffi-byte-offset layout path value)) (str path)))
    (is (= 32 (- (ffi-byte-offset native/ffi-clj-fixed-response-slot-data-t
                                  [:headers 1 :name] pointer)
                 (ffi-byte-offset native/ffi-clj-fixed-response-slot-data-t
                                  [:headers 0 :name] pointer))))
    (is (= 128 (ffi-byte-offset [:array native/ffi-clj-session-ticket-t 2]
                                [1 :name 0] 1)))
    (doseq [layout [native/ffi-clj-header-t native/ffi-h2o-iovec-t
                    native/ffi-clj-fixed-response-slot-data-t
                    native/ffi-h2o-accept-ctx-t native/ffi-h2o-sendvec-t
                    native/ffi-h2o-header-t native/ffi-h2o-generator-t
                    native/ffi-clj-req-meta-t native/ffi-clj-req-ctx-t
                    native/ffi-clj-h2o-flat-globalconf-t
                    native/ffi-clj-session-ticket-t]]
      (is (= 8 (ffi/alignof layout)) (str layout)))))

(defn request-context-value [arena]
  (let [pointer #(ffi/alloc arena 1)]
    {:req                           (pointer)
     :meta                          {:authority       (pointer)
                                     :method          (pointer)
                                     :path            (pointer)
                                     :remote_addr     (pointer)
                                     :scheme          (pointer)
                                     :headers         (pointer)
                                     :authority_len   11
                                     :method_len      3
                                     :path_len        10
                                     :remote_addr_len 9
                                     :scheme_len      5
                                     :headers_len     2
                                     :http_version    0x101
                                     :has_body        1
                                     :is_early_data   0
                                     :entrypoint      3
                                     :connection_id   12345}
     :on-cleanup                    (pointer)
     :on-request-body-chunk         (pointer)
     :response-receiver             (pointer)
     :response-hash-next            (pointer)
     :fixed-slot                    (pointer)
     :fixed-claim                   65537
     :generator                     {:proceed (pointer)
                                     :stop    (pointer)}
     :on-response-generator-proceed (pointer)
     :on-response-generator-stop    (pointer)
     :preferred-chunk-size          16384
     :dispatch-module-id            42
     :dispatch-request-seq          7
     :cleanup                       0
     :closing                       0
     :response_started              0}))

(def ^:private request-context-reader-offsets
  {:req                          0
   :meta                         8
   :on-cleanup                   128
   :on-request-body-chunk        136
   :response-receiver             144
   :response-hash-next            152
   :fixed-slot                    160
   :fixed-claim                   168
   :generator                     176
   :on-response-generator-proceed 192
   :on-response-generator-stop    200
   :preferred-chunk-size          208
   :dispatch-module-id            216
   :dispatch-request-seq          224
   :cleanup                       232
   :closing                       236
   :response_started              240})

(def ^:private request-metadata-reader-offsets
  {:authority       0
   :method          8
   :path            16
   :remote_addr     24
   :scheme          32
   :headers         40
   :authority_len   48
   :method_len      56
   :path_len        64
   :remote_addr_len 72
   :scheme_len      80
   :headers_len     88
   :http_version    96
   :has_body        100
   :is_early_data   102
   :entrypoint      104
   :connection_id   112})

(def ^:private generator-reader-offsets
  {:proceed 0
   :stop    8})

(deftest request-copy-offsets-match-the-layout-test
  (let [pointer (ffi/segment 1)
        meta native/req-ctx-meta-offset]
    (is (= {:req native/req-ctx-req-offset
            :meta meta
            [:meta :authority] (+ meta native/req-meta-text-offset)
            [:meta :scheme] (+ meta native/req-meta-text-offset 32)
            [:meta :headers] (+ meta native/req-meta-headers-offset)
            [:meta :authority_len] (+ meta native/req-meta-text-length-offset)
            [:meta :scheme_len] (+ meta native/req-meta-text-length-offset 32)
            [:meta :headers_len] (+ meta native/req-meta-headers-length-offset)
            [:meta :http_version] (+ meta native/req-meta-http-version-offset)
            [:meta :has_body] (+ meta native/req-meta-has-body-offset)
            [:meta :is_early_data] (+ meta native/req-meta-early-data-offset)
            [:meta :entrypoint] (+ meta native/req-meta-entrypoint-offset)
            [:meta :connection_id] (+ meta native/req-meta-connection-id-offset)}
           {:req (ffi-byte-offset native/ffi-clj-req-ctx-t :req pointer)
            :meta (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :authority] pointer)
            [:meta :authority] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :authority] pointer)
            [:meta :scheme] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :scheme] pointer)
            [:meta :headers] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :headers] pointer)
            [:meta :authority_len] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :authority_len] 1)
            [:meta :scheme_len] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :scheme_len] 1)
            [:meta :headers_len] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :headers_len] 1)
            [:meta :http_version] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :http_version] 1)
            [:meta :has_body] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :has_body] 1)
            [:meta :is_early_data] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :is_early_data] 1)
            [:meta :entrypoint] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :entrypoint] 1)
            [:meta :connection_id] (ffi-byte-offset native/ffi-clj-req-ctx-t [:meta :connection_id] 1)}))
    (is (= {:size native/header-size
            :name-length native/header-length-offset
            :value native/header-value-offset
            :value-length (+ native/header-value-offset native/header-length-offset)}
           {:size (ffi/sizeof native/ffi-clj-header-t)
            :name-length (ffi-byte-offset native/ffi-clj-header-t :name_len 1)
            :value (ffi-byte-offset native/ffi-clj-header-t :value pointer)
            :value-length (ffi-byte-offset native/ffi-clj-header-t :value_len 1)}))))

(deftest request-context-layout-test
  (is (= 248 (ffi/sizeof native/ffi-clj-req-ctx-t)))
  (doseq [[field expected-offset] request-context-reader-offsets]
    (let [path (case field :meta [:meta :authority] :generator [:generator :proceed] field)
          value (if (contains? #{:preferred-chunk-size :dispatch-module-id :dispatch-request-seq
                                 :cleanup :closing :response_started} field) 1 (ffi/segment 1))]
      (is (= expected-offset (ffi-byte-offset native/ffi-clj-req-ctx-t path value)))))
  (doseq [[field expected-offset] request-metadata-reader-offsets]
    (let [value (if (contains? #{:authority :method :path :remote_addr :scheme :headers} field)
                  (ffi/segment 1) 1)]
      (is (= expected-offset (ffi-byte-offset native/ffi-clj-req-meta-t field value)))))
  (doseq [[field expected-offset] generator-reader-offsets]
    (is (= expected-offset
           (ffi-byte-offset native/ffi-h2o-generator-t field (ffi/segment 1))))))

(deftest overlay-lazy-ring-request-defers-base-map
  (let [realizations_ (atom 0)
        request (native/overlay-lazy-ring-request
                 #(do (swap! realizations_ inc) {:a 1 :remove-me 2})
                 #{:a :remove-me}
                 {:emitter :emitter})
        overlaid (-> request
                     (assoc :ol.busker/entrypoint :http)
                     (dissoc :remove-me)
                     (with-meta {:source :test}))]
    (is (= {:before 0
            :overlay :emitter
            :entrypoint :http
            :after-overlay 0
            :base 1
            :after-base 1
            :map {:a 1 :emitter :emitter :ol.busker/entrypoint :http}
            :metadata {:source :test}}
           {:before 0
            :overlay (:emitter overlaid)
            :entrypoint (:ol.busker/entrypoint overlaid)
            :after-overlay @realizations_
            :base (:a overlaid)
            :after-base @realizations_
            :map (into {} overlaid)
            :metadata (meta overlaid)}))))

(def ^:private wrk-request-snapshot
  {:method "GET"
   :path "/"
   :authority "localhost:8080"
   :scheme "http"
   :remote-addr "127.0.0.1"
   :headers {"host" "localhost:8080"}
   :http-version 0x0101
   :has-body 0
   :early-data 0})

(defn- copied-snapshot
  "Writes a native request context for `snapshot` and returns its copy. The
  native memory is freed before this returns. `:headers` is a map or a sequence
  of name and value pairs."
  [{:keys [authority method path remote-addr scheme headers http-version has-body early-data
           entrypoint]
    :or {entrypoint 0}}]
  (with-open [arena (ffi/confined-arena)]
    (let [text (fn [s]
                 (if s
                   [(ffi/string->ptr arena s) (alength (.getBytes ^String s "UTF-8"))]
                   [ffi/null 0]))
          [authority-ptr authority-len] (text authority)
          [method-ptr method-len] (text method)
          [path-ptr path-len] (text path)
          [remote-ptr remote-len] (text remote-addr)
          [scheme-ptr scheme-len] (text scheme)
          header-count (count headers)
          header-ptr (if (pos? header-count) (ffi/alloc arena (* 32 header-count)) ffi/null)
          pointer (ffi/alloc arena native/ffi-clj-req-ctx-t)]
      (doseq [[i [name value]] (map-indexed vector headers)]
        (let [[name-ptr name-len] (text name)
              [value-ptr value-len] (text value)]
          (ffi/write (ffi/slice header-ptr (* i 32) 32) native/ffi-clj-header-t
                     {:name name-ptr :name_len name-len :value value-ptr :value_len value-len})))
      (ffi/write pointer native/ffi-clj-req-ctx-t
                 (assoc (request-context-value arena)
                        :meta {:authority authority-ptr :authority_len authority-len
                               :method method-ptr :method_len method-len
                               :path path-ptr :path_len path-len
                               :remote_addr remote-ptr :remote_addr_len remote-len
                               :scheme scheme-ptr :scheme_len scheme-len
                               :headers header-ptr :headers_len header-count
                               :http_version http-version :has_body has-body
                               :is_early_data early-data :entrypoint entrypoint
                               :connection_id 1}))
      (native/copy-request-context pointer))))

(deftest copy-request-context-reads-the-entrypoint-index-test
  (is (= 3 (:entrypoint (copied-snapshot (assoc wrk-request-snapshot :entrypoint 3)))))
  (is (= native/CLJ_ENTRYPOINT_UNKNOWN
         (:entrypoint (copied-snapshot (assoc wrk-request-snapshot
                                              :entrypoint (unchecked-int 0xFFFFFFFF))))))
  (is (not (contains? native/assembled-request-keys :ol.busker/entrypoint))
      "the entrypoint comes from the overlay, so reading it does not decode the request"))

(deftest request-connection-id-reads-the-native-context-test
  (with-open [arena (ffi/confined-arena)]
    (let [pointer (ffi/alloc arena native/ffi-clj-req-ctx-t)]
      (ffi/write pointer native/ffi-clj-req-ctx-t
                 (assoc-in (request-context-value arena) [:meta :connection_id] Long/MAX_VALUE))
      (is (= Long/MAX_VALUE (native/request-connection-id pointer))))))

(defn- assembled-snapshot
  [snapshot input-stream]
  (native/assemble-ring-request (copied-snapshot snapshot) input-stream))

(defn- assembled-request
  ([] (assembled-request {}))
  ([snapshot-changes]
   (assembled-snapshot (merge wrk-request-snapshot snapshot-changes) nil)))

(deftest assemble-ring-request-builds-the-ring-request-test
  (let [body (java.io.ByteArrayInputStream. (byte-array 0))]
    (is (= {:server-port 8080
            :server-name "localhost"
            :remote-addr "127.0.0.1"
            :uri "/things"
            :query-string "q=1&r=2"
            :scheme :https
            :request-method :post
            :protocol "HTTP/2.0"
            :headers {"host" "localhost:8080"}
            :body body
            :ol.busker/early-data? true}
           (assembled-snapshot
            (assoc wrk-request-snapshot
                   :method "POST" :path "/things?q=1&r=2" :scheme "https"
                   :http-version 0x0200 :has-body 1 :early-data 1)
            body))))
  (is (= {:server-port 443
          :server-name "localhost"
          :remote-addr ""
          :uri nil
          :query-string nil
          :scheme :https
          :request-method :get
          :protocol "HTTP/1.1"
          :headers nil
          :body nil
          :ol.busker/early-data? false}
         (assembled-snapshot {:scheme "https" :http-version 0 :has-body 0 :early-data 0}
                             nil)))
  (is (= [:protocol :remote-addr :headers :server-port :ol.busker/early-data? :uri
          :server-name :query-string :body :scheme :request-method]
         (vec (keys (assembled-request)))))
  (is (= ["" ""] ((juxt :uri :query-string) (assembled-request {:path "?"}))))
  (is (= ["/a" "b?c"] ((juxt :uri :query-string) (assembled-request {:path "/a?b?c"}))))
  (is (= ["HTTP/1.1" "HTTP/3.0"]
         [(:protocol (assembled-request {:http-version 0x0100}))
          (:protocol (assembled-request {:http-version 0x0300}))]))
  (is (= native/assembled-request-keys (set (keys (assembled-request)))))
  (is (= clojure.lang.PersistentHashMap (type (assoc (assembled-request) :x 1))))
  (is (= (assoc (into {} (assembled-request)) :x 1) (assoc (assembled-request) :x 1))))

(deftest assemble-ring-request-from-a-copied-header-set-test
  (let [many (for [i (range 50)] [(str "X-Header-" i) (str "value-" i)])
        headers (concat [["Host" "example.test:8443"]
                         ["Accept-Language" "日本語, fr-CH"]
                         ["X-Name" "café ☕"]
                         ["X-Empty" ""]
                         ["X-Dup" "one"]
                         ["Cookie" "a=1"]]
                        many
                        [["x-dup" "two"]
                         ["COOKIE" "b=2"]
                         ["X-Dup" "über"]])]
    (is (= {:server-port 8443
            :server-name "example.test"
            :remote-addr "192.0.2.10"
            :uri "/données"
            :query-string "q=ü"
            :scheme :https
            :request-method :put
            :protocol "HTTP/2.0"
            :headers (into {"host" "example.test:8443"
                            "accept-language" "日本語, fr-CH"
                            "x-name" "café ☕"
                            "x-empty" ""
                            "x-dup" "one,two,über"
                            "cookie" "a=1;b=2"}
                           (for [i (range 50)] [(str "x-header-" i) (str "value-" i)]))
            :body nil
            :ol.busker/early-data? false}
           (assembled-snapshot {:authority "example.test:8443" :method "PUT"
                                :path "/données?q=ü" :remote-addr "192.0.2.10"
                                :scheme "https" :headers headers :http-version 0x0200
                                :has-body 0 :early-data 0}
                               nil)))))

(deftest assemble-ring-request-keeps-empty-header-values-test
  (doseq [[headers expected]
          [[[["X-Empty" ""]] {"x-empty" ""}]
           [[["X-Empty" ""] ["X-Empty" "b"]] {"x-empty" ",b"}]
           [[["X-Empty" "a"] ["X-Empty" ""]] {"x-empty" "a,"}]
           [[["X-Empty" "a"] ["X-Empty" ""] ["X-Empty" "c"]] {"x-empty" "a,,c"}]
           [[["X-Empty" ""] ["x-empty" ""]] {"x-empty" ","}]
           [[["Cookie" ""] ["Cookie" "b=2"]] {"cookie" ";b=2"}]
           [[["X-Other" "1"]] {"x-other" "1"}]]]
    (is (= expected (:headers (assembled-request {:headers headers}))) (pr-str headers)))
  (is (= {:headers nil :query-string nil}
         (select-keys (assembled-request {:headers nil :path "/"}) [:headers :query-string]))))

(deftest assemble-ring-request-treats-an-overlong-authority-port-as-invalid-test
  (doseq [authority ["localhost:99999" "localhost:9999999999999999999"
                     "[::1]:99999" "[::1]:9999999999999999999"]]
    (is (= {:server-name authority :server-port 80}
           (select-keys (assembled-request {:authority authority})
                        [:server-name :server-port]))
        authority)))

(deftest assemble-ring-request-method-and-scheme-keywords-test
  (doseq [[method expected] [["GET" :get] ["get" :get] ["Get" :get] ["POST" :post]
                             ["post" :post] ["PUT" :put] ["put" :put] ["DELETE" :delete]
                             ["delete" :delete] ["HEAD" :head] ["head" :head]
                             ["OPTIONS" :options] ["options" :options] ["PATCH" :patch]
                             ["patch" :patch] ["PROPFIND" :propfind] ["M-SEARCH" :m-search]
                             [nil :get]]]
    (is (identical? expected (:request-method (assembled-request {:method method}))) method))
  (doseq [[scheme expected] [["http" :http] ["https" :https] [nil :http] ["HTTP" :HTTP]]]
    (is (identical? expected (:scheme (assembled-request {:scheme scheme}))) scheme)))

(deftest overlay-lazy-ring-request-behaves-like-the-merged-map-test
  (let [overlay {::emitter :emitter}
        request (native/overlay-lazy-ring-request #(assembled-request)
                                                  native/assembled-request-keys overlay)
        expected (merge (assembled-request) overlay)
        destructure (fn [{:keys [uri query-string missing] :or {missing :default query-string :q}}]
                      [uri query-string missing])
        observe (fn [m]
                  {:get [(get m :uri) (get m :missing) (get m :missing ::none)
                         (get m :query-string ::none) (get m ::emitter)]
                   :invoke [(m :uri) (m :missing ::none) (:missing m ::none) (:headers m)]
                   :contains [(contains? m :query-string) (contains? m :missing)
                              (contains? m ::emitter)]
                   :find [(find m :uri) (find m :missing) (find m :query-string)]
                   :java [(.get ^java.util.Map m :uri) (.containsKey ^java.util.Map m :missing)
                          (.size ^java.util.Map m) (.isEmpty ^java.util.Map m)]
                   :count (count m)
                   :seq (vec (seq m))
                   :keys (vec (keys m))
                   :vals (vec (vals m))
                   :hash (hash m)
                   :hash-code (.hashCode ^Object m)
                   :string (str m)
                   :merge (merge m {:a 1 :uri "/b"})
                   :into (into {} m)
                   :conj (conj m [:a 1])
                   :select-keys (select-keys m [:uri :missing :headers :query-string])
                   :destructure (destructure m)
                   :assoc (assoc m :a 1)
                   :assoc-existing (assoc m :uri "/b")
                   :dissoc (dissoc m :uri)
                   :dissoc-missing (dissoc m :missing)
                   :dissoc-lookup (let [d (dissoc m :uri)]
                                    [(get d :uri ::none) (contains? d :uri) (find d :uri)
                                     (count d)])
                   :reassoc (let [d (assoc (dissoc m :uri) :uri "/c")]
                              [(get d :uri) (count d)])
                   :empty (empty m)
                   :meta (meta m)
                   :with-meta (meta (with-meta m {:a 1}))})]
    (is (= request expected))
    (is (= expected request))
    (is (.equals ^Object request expected))
    (is (= (observe expected) (observe request)))
    (is (= clojure.lang.PersistentHashMap
           (type (into {} request))
           (type (merge request {:a 1}))
           (type (conj request [:a 1]))))))

(deftest overlay-lazy-ring-request-answers-unknown-keys-without-assembly-test
  (let [realizations_ (atom 0)
        request (native/overlay-lazy-ring-request
                 #(do (swap! realizations_ inc) (assembled-request))
                 native/assembled-request-keys
                 {::emitter :emitter})
        dispatched (assoc request :ol.busker/entrypoint :http)]
    (is (= {:unknown [nil ::none false nil nil ::none]
            :overlay [:emitter true :http]
            :removed [nil false]
            :realizations-before-known-key 0
            :known [8080 :http "/"]
            :realizations 1}
           {:unknown [(:ol.busker/entrypoint request) (get request :missing ::none)
                      (contains? request :missing) (find request :missing)
                      (request :missing) (request :missing ::none)]
            :overlay [(::emitter request) (contains? request ::emitter)
                      (:ol.busker/entrypoint dispatched)]
            :removed [(:uri (dissoc request :uri)) (contains? (dissoc request :uri) :uri)]
            :realizations-before-known-key @realizations_
            :known [(:server-port dispatched) (:scheme dispatched) (:uri dispatched)]
            :realizations @realizations_}))))

(deftest response-slot-layout-matches-native
  (is (= 2120
         (ffi/sizeof native/ffi-clj-fixed-response-slot-data-t)
         (native/mt-response-slot-data-size)))
  (doseq [[field expected] {:claim-token 0
                            :module-id 8
                            :request-seq 16
                            :headers-len 24
                            :content-length 32
                            :body-offset 40
                            :body-len 48
                            :payload-len 56
                            :status 64
                            :compress-hint 68
                            :headers 72}]
    (is (= expected
           (ffi-byte-offset native/ffi-clj-fixed-response-slot-data-t
                            (if (= :headers field) [:headers 0 :name] field)
                            (if (= :headers field) (ffi/segment 1) 1)))))
  (is (= 32 (ffi/sizeof native/ffi-clj-header-t)))
  (doseq [[field expected] {:name 0
                            :name_len 8
                            :value 16
                            :value_len 24}]
    (is (= expected
           (ffi-byte-offset native/ffi-clj-header-t field
                            (if (#{:name :value} field) (ffi/segment 1) 1))))))
(defn- copied-request-context [arena]
  (let [request (ffi/alloc arena 1)
        string-pointer #(ffi/string->ptr arena %)
        context (-> (request-context-value arena)
                    (assoc :req request)
                    (assoc :meta
                           {:authority (string-pointer "example.test:8443")
                            :method (string-pointer "POST")
                            :path (string-pointer "/things?q=1")
                            :remote_addr (string-pointer "192.0.2.10")
                            :scheme (string-pointer "https")
                            :headers ffi/null
                            :authority_len 17
                            :method_len 4
                            :path_len 11
                            :remote_addr_len 10
                            :scheme_len 5
                            :headers_len 0
                            :http_version 0x0200
                            :has_body 1
                            :is_early_data 1
                            :entrypoint 0
                            :connection_id 1}))
        pointer (ffi/alloc arena native/ffi-clj-req-ctx-t)]
    (ffi/write pointer native/ffi-clj-req-ctx-t context)
    {:pointer pointer :request request}))

(defn- copied-request-values [{:keys [req has-body request-method] :as copied}]
  {:request-address (.address ^java.lang.foreign.MemorySegment req)
   :has-body has-body
   :request-method request-method
   :ring-request (native/assemble-ring-request copied nil)})

(def ^:private expected-copied-request-values
  {:has-body 1
   :request-method :post
   :ring-request {:server-port 8443
                  :server-name "example.test"
                  :remote-addr "192.0.2.10"
                  :uri "/things"
                  :query-string "q=1"
                  :scheme :https
                  :request-method :post
                  :protocol "HTTP/2.0"
                  :headers nil
                  :body nil
                  :ol.busker/early-data? true}})

(deftest request-snapshot-realizes-after-native-cleanup
  (let [snapshot
        (with-open [arena (ffi/confined-arena)]
          (let [{:keys [pointer]} (copied-request-context arena)
                pairs [["Cookie" "a=1"] ["cookie" "b=2"]
                       ["X-Test" "café"] ["x-test" ""]]
                headers (ffi/alloc arena (* 32 (count pairs)))]
            (doseq [[i [name value]] (map-indexed vector pairs)]
              (ffi/write (ffi/slice headers (* i 32) 32) native/ffi-clj-header-t
                         {:name (ffi/string->ptr arena name)
                          :name_len (alength (.getBytes ^String name "UTF-8"))
                          :value (ffi/string->ptr arena value)
                          :value_len (alength (.getBytes ^String value "UTF-8"))}))
            (ffi/write pointer (ffi/place native/ffi-clj-req-ctx-t [:meta :headers]) headers)
            (ffi/write pointer (ffi/place native/ffi-clj-req-ctx-t [:meta :headers_len]) (count pairs))
            (let [copied (native/copy-request-context pointer)]
              (is (= {:has-body 1 :request-method :post}
                     (select-keys copied [:has-body :request-method])))
              copied)))
        request (native/overlay-lazy-ring-request
                 #(native/assemble-ring-request snapshot nil)
                 native/assembled-request-keys
                 {:extra true})]
    (is (:extra request))
    (is (= {"cookie" "a=1;b=2" "x-test" "café,"} (:headers request)))
    (is (= :post (:request-method request)))
    (is (= "/things" (:uri request)))
    (is (= "q=1" (:query-string request)))
    (is (= "HTTP/2.0" (:protocol request)))
    (is (:ol.busker/early-data? request))))

(deftest request-copy-method-keyword-test
  (with-open [arena (ffi/shared-arena)]
    (let [{:keys [pointer]} (copied-request-context arena)]
      (doseq [[method expected] [["GET" :get] ["get" :get] ["Get" :get] ["POST" :post]
                                 ["HEAD" :head] ["head" :head] ["PATCH" :patch]
                                 ["OPTIONS" :options] ["PROPFIND" :propfind]
                                 ["M-SEARCH" :m-search] ["" nil]]]
        (ffi/write pointer (ffi/place native/ffi-clj-req-ctx-t [:meta :method])
                   (ffi/string->ptr arena method))
        (ffi/write pointer (ffi/place native/ffi-clj-req-ctx-t [:meta :method_len])
                   (count method))
        (is (identical? expected (:request-method (native/copy-request-context pointer)))
            method)))))

(deftest request-copy-keeps-only-the-request-pointer-test
  (with-open [source-arena (ffi/shared-arena)]
    (let [{:keys [pointer request]} (copied-request-context source-arena)
          result (native/copy-request-context pointer)]
      (is (= {:source-scope? false
              :request-values (assoc expected-copied-request-values
                                     :request-address (.address ^java.lang.foreign.MemorySegment request))}
             {:source-scope? (identical? (.scope source-arena)
                                         (.scope ^java.lang.foreign.MemorySegment (:req result)))
              :request-values (copied-request-values result)})))))
