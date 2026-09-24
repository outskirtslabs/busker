(ns ol.busker.native-test
  (:require
   [babashka.ffi :as ffi]
   [clojure.string :as str]
   [clojure.test :refer [deftest is]]
   [ol.busker.native :as native]
   [ol.busker.native.loader :as loader])
  (:import
   [java.nio.file Files Path]))

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
        key "ol.libh2oclj.path"
        previous (System/getProperty key)
        missing (str path "-missing")]
    (try
      (loader/copy-resource (str (loader/get-os-arch) "/" library-name) (str path))
      (System/setProperty key (str path))
      (require 'ol.busker.native.loader :reload)
      (is (= (native/req-ctx-size)
             ((ffi/cfn "clj_h2o_req_ctx_size" [] :long))))
      (is (thrown? Exception (ffi/load-library missing)))
      (System/setProperty key missing)
      (is (thrown? Exception (require 'ol.busker.native.loader :reload)))
      (finally
        (if previous (System/setProperty key previous) (System/clearProperty key))
        (require 'ol.busker.native.loader :reload)
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

(deftest native-struct-sizes-match-shim
  (is (= {:header          32
          :iovec          16
          :fixed-slot      (native/mt-response-slot-data-size)
          :accept-context  (native/accept-ctx-size)
          :sendvec         32
          :h2o-header      40
          :generator       16
          :request-meta    104
          :request-context (native/req-ctx-size)
          :globalconf      328
          :ticket          128}
         {:header          (ffi/sizeof native/ffi-clj-header-t)
          :iovec          (ffi/sizeof native/ffi-h2o-iovec-t)
          :fixed-slot      (ffi/sizeof native/ffi-clj-fixed-response-slot-data-t)
          :accept-context  (ffi/sizeof native/ffi-h2o-accept-ctx-t)
          :sendvec         (ffi/sizeof native/ffi-h2o-sendvec-t)
          :h2o-header      (ffi/sizeof native/ffi-h2o-header-t)
          :generator       (ffi/sizeof native/ffi-h2o-generator-t)
          :request-meta    (ffi/sizeof native/ffi-clj-req-meta-t)
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
                   [native/ffi-clj-req-ctx-t [:meta :headers_len] 1 96]
                   [native/ffi-clj-req-ctx-t :on-response-generator-stop pointer 168]
                   [native/ffi-clj-h2o-flat-globalconf-t :compress_args_zstd_quality 1 320]
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
                                     :is_early_data   0}
     :on-cleanup                    (pointer)
     :on-request-body-chunk         (pointer)
     :response-receiver             (pointer)
     :response-hash-next            (pointer)
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
   :on-cleanup                   112
   :on-request-body-chunk        120
   :response-receiver             128
   :response-hash-next            136
   :generator                     144
   :on-response-generator-proceed 160
   :on-response-generator-stop    168
   :preferred-chunk-size          176
   :dispatch-module-id            184
   :dispatch-request-seq          192
   :cleanup                       200
   :closing                       204
   :response_started              208})

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
   :is_early_data   102})

(def ^:private generator-reader-offsets
  {:proceed 0
   :stop    8})

(deftest request-context-layout-test
  (is (= 216 (ffi/sizeof native/ffi-clj-req-ctx-t)))
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
                            :is_early_data 1}))
        pointer (ffi/alloc arena native/ffi-clj-req-ctx-t)]
    (ffi/write pointer native/ffi-clj-req-ctx-t context)
    {:pointer pointer :request request}))

(defn- copied-request-values [{:keys [req has-body ring-data]}]
  {:request-address (.address ^java.lang.foreign.MemorySegment req)
   :has-body has-body
   :ring-data ring-data})

(def ^:private expected-copied-request-values
  {:has-body 1
   :ring-data {:method "POST"
               :path "/things?q=1"
               :authority "example.test:8443"
               :scheme "https"
               :remote-addr "192.0.2.10"
               :headers nil
               :http-version 0x0200
               :has-body 1
               :early-data 1}})
(defn- recording-arena [^java.lang.foreign.Arena arena scope-calls_ allocation-calls_]
  (reify java.lang.foreign.Arena
    (scope [_]
      (swap! scope-calls_ inc)
      (.scope arena))
    (^java.lang.foreign.MemorySegment allocate [_ ^long bytes ^long alignment]
      (swap! allocation-calls_ inc)
      (.allocate arena bytes alignment))
    (close [_])))

(deftest explicit-arena-request-copy-uses-supplied-arena-test
  (with-open [arena (ffi/shared-arena)]
    (let [{:keys [pointer request]} (copied-request-context arena)
          scope-calls_ (atom 0)
          allocation-calls_ (atom 0)
          supplied-arena (recording-arena arena scope-calls_ allocation-calls_)
          result (native/copy-request-context pointer supplied-arena)]
      (is (= {:arena-scope-calls 6
              :arena-allocation-calls 0
              :request-values (assoc expected-copied-request-values
                                     :request-address (.address ^java.lang.foreign.MemorySegment request))}
             {:arena-scope-calls @scope-calls_
              :arena-allocation-calls @allocation-calls_
              :request-values (copied-request-values result)})))))

(deftest explicit-arena-header-copy-uses-supplied-arena-test
  (with-open [arena (ffi/shared-arena)]
    (let [name (ffi/string->ptr arena "X-Test")
          value (ffi/string->ptr arena "value")
          header (ffi/alloc arena native/ffi-clj-header-t)
          _ (ffi/write header native/ffi-clj-header-t
                       {:name name :name_len 6 :value value :value_len 5})
          scope-calls_ (atom 0)
          allocation-calls_ (atom 0)
          supplied-arena (recording-arena arena scope-calls_ allocation-calls_)]
      (is (= {:headers {"x-test" "value"}
              :arena-scope-calls 3
              :arena-allocation-calls 0}
             {:headers (native/build-ring-headers-map header 1 supplied-arena)
              :arena-scope-calls @scope-calls_
              :arena-allocation-calls @allocation-calls_})))))

(deftest default-request-copy-retains-an-implicit-scope-test
  (with-open [source-arena (ffi/shared-arena)]
    (let [{:keys [pointer request]} (copied-request-context source-arena)
          result (native/copy-request-context pointer)]
      (is (= {:source-scope? false
              :request-values (assoc expected-copied-request-values
                                     :request-address (.address ^java.lang.foreign.MemorySegment request))}
             {:source-scope? (identical? (.scope source-arena)
                                         (.scope ^java.lang.foreign.MemorySegment (:req result)))
              :request-values (copied-request-values result)})))))
