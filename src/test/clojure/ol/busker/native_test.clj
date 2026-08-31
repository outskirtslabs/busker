(ns ol.busker.native-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.native :as native]))

(defn request-context-value [arena]
  (let [pointer #(mem/alloc 1 arena)]
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

(deftest request-context-reader-preserves-consumed-values-test
  (with-open [arena (mem/confined-arena)]
    (let [ctx-ptr (mem/serialize (request-context-value arena)
                                 :ol.busker.native/clj-req-ctx-t
                                 arena)
          generic (mem/deserialize
                   (mem/reinterpret ctx-ptr
                                    (mem/size-of :ol.busker.native/clj-req-ctx-t))
                   :ol.busker.native/clj-req-ctx-t)]
      (is (= (select-keys generic [:req :meta])
             (native/read-request-context ctx-ptr))))))

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

(deftest request-context-reader-layout-test
  (is (= 216 (mem/size-of :ol.busker.native/clj-req-ctx-t)))
  (doseq [[field expected-offset] request-context-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/clj-req-ctx-t field))))
  (doseq [[field expected-offset] request-metadata-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/clj-req-meta-t field))))
  (doseq [[field expected-offset] generator-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/h2o-generator-t field)))))

(defn- copied-request-context [arena]
  (let [request (mem/alloc 1 arena)
        string-pointer #(mem/serialize % ::mem/c-string arena)
        context (-> (request-context-value arena)
                    (assoc :req request)
                    (assoc :meta
                           {:authority (string-pointer "example.test:8443")
                            :method (string-pointer "POST")
                            :path (string-pointer "/things?q=1")
                            :remote_addr (string-pointer "192.0.2.10")
                            :scheme (string-pointer "https")
                            :headers mem/null
                            :authority_len 17
                            :method_len 4
                            :path_len 11
                            :remote_addr_len 10
                            :scheme_len 5
                            :headers_len 0
                            :http_version 0x0200
                            :has_body 1
                            :is_early_data 1}))]
    {:pointer (mem/serialize context :ol.busker.native/clj-req-ctx-t arena)
     :request request}))

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
  (with-open [arena (mem/shared-arena)]
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
  (with-open [arena (mem/shared-arena)]
    (let [name (mem/serialize "X-Test" ::mem/c-string arena)
          value (mem/serialize "value" ::mem/c-string arena)
          header (mem/serialize {:name name
                                 :name_len 6
                                 :value value
                                 :value_len 5}
                                :ol.busker.native/clj-header-t
                                arena)
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
  (with-open [source-arena (mem/shared-arena)]
    (let [{:keys [pointer request]} (copied-request-context source-arena)
          result (native/copy-request-context pointer)]
      (is (= {:source-scope? false
              :request-values (assoc expected-copied-request-values
                                     :request-address (.address ^java.lang.foreign.MemorySegment request))}
             {:source-scope? (identical? (.scope source-arena)
                                         (.scope ^java.lang.foreign.MemorySegment (:req result)))
              :request-values (copied-request-values result)})))))
