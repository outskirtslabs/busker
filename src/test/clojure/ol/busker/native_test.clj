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
     :generator                     {:proceed (pointer)
                                     :stop    (pointer)}
     :on-response-generator-proceed (pointer)
     :on-response-generator-stop    (pointer)
     :preferred-chunk-size          16384
     :req-id                        (vec (concat (map int "probe-request")
                                                 (repeat 51 0)))
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
      (is (= (select-keys generic [:req :meta :req-id])
             (#'native/read-request-context ctx-ptr))))))

(def ^:private request-context-reader-offsets
  {:req                          0
   :meta                         8
   :on-cleanup                   112
   :on-request-body-chunk        120
   :generator                    128
   :on-response-generator-proceed 144
   :on-response-generator-stop   152
   :preferred-chunk-size         160
   :req-id                       168
   :dispatch-module-id           232
   :dispatch-request-seq         240
   :cleanup                      248
   :closing                      252
   :response_started             256})

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
  (is (= 264 (mem/size-of :ol.busker.native/clj-req-ctx-t)))
  (doseq [[field expected-offset] request-context-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/clj-req-ctx-t field))))
  (doseq [[field expected-offset] request-metadata-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/clj-req-meta-t field))))
  (doseq [[field expected-offset] generator-reader-offsets]
    (is (= expected-offset
           (native/offset-of :ol.busker.native/h2o-generator-t field)))))
