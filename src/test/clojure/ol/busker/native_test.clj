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

(deftest request-context-reader-preserves-generic-decode
  (with-open [arena (mem/confined-arena)]
    (let [ctx-ptr (mem/serialize (request-context-value arena)
                                 :ol.busker.native/clj-req-ctx-t
                                 arena)
          generic (mem/deserialize
                   (mem/reinterpret ctx-ptr
                                    (mem/size-of :ol.busker.native/clj-req-ctx-t))
                   :ol.busker.native/clj-req-ctx-t)]
      (is (= generic (#'native/read-request-context ctx-ptr))))))
