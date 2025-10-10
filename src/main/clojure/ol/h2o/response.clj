(ns ol.h2o.response
  (:require
   [ol.h2o.native :as h2o]
   [clojure.string :as str]
   [coffi.mem :as mem]
   [ring.core.protocols :as ring-protocols]))

(set! *warn-on-reflection* true)

(defn send-ring-response!
  "Send Ring response via h2o FFI.
   MUST be called from the worker thread that owns the request.

   Parameters:
   - req-ptr: Native pointer to h2o_req_t
   - ring-resp: Ring response map {:status :headers :body}"
  [req-ptr ring-resp]
  (try
    (let [status (:status ring-resp 200)
          headers (:headers ring-resp {})
          body (:body ring-resp)

          content-length-str (get headers "content-length")
          content-length (when content-length-str
                           (try
                             (Long/parseLong (str content-length-str))
                             (catch Exception _ nil)))

          headers-to-send (dissoc headers "content-length")]

      (h2o/req-set-status req-ptr status)
      (when content-length
        (h2o/req-set-content-length req-ptr content-length))

      (let [pool-ptr (h2o/req-get-pool req-ptr)
            res-headers-ptr (h2o/req-get-res-headers req-ptr)]

        (doseq [[name value] headers-to-send]
          (let [name-lower (str/lower-case (str name))
                name-orig (str name)
                value-str (str value)
                name-lower-bytes (.getBytes name-lower "UTF-8")
                name-orig-bytes (.getBytes name-orig "UTF-8")
                value-bytes (.getBytes value-str "UTF-8")
                name-lower-len (alength name-lower-bytes)
                name-orig-len (alength name-orig-bytes)
                value-len (alength value-bytes)
                name-lower-ptr-raw (h2o/mem-alloc-shared pool-ptr name-lower-len
                                                         java.lang.foreign.MemorySegment/NULL)
                name-orig-ptr-raw (h2o/mem-alloc-shared pool-ptr name-orig-len
                                                        java.lang.foreign.MemorySegment/NULL)
                value-ptr-raw (h2o/mem-alloc-shared pool-ptr value-len
                                                    java.lang.foreign.MemorySegment/NULL)]
            (let [name-lower-ptr (mem/reinterpret name-lower-ptr-raw name-lower-len)
                  name-orig-ptr (mem/reinterpret name-orig-ptr-raw name-orig-len)
                  value-ptr (mem/reinterpret value-ptr-raw value-len)]
              (java.lang.foreign.MemorySegment/copy
               (java.lang.foreign.MemorySegment/ofArray name-lower-bytes) 0
               name-lower-ptr 0 name-lower-len)
              (java.lang.foreign.MemorySegment/copy
               (java.lang.foreign.MemorySegment/ofArray name-orig-bytes) 0
               name-orig-ptr 0 name-orig-len)
              (java.lang.foreign.MemorySegment/copy
               (java.lang.foreign.MemorySegment/ofArray value-bytes) 0
               value-ptr 0 value-len))
            (h2o/add-header-by-str pool-ptr res-headers-ptr
                                   name-lower-ptr-raw name-lower-len
                                   0
                                   name-orig-ptr-raw
                                   value-ptr-raw value-len)))

        (let [buffer (java.io.ByteArrayOutputStream.)
              output-stream (proxy [java.io.OutputStream] []
                              (write
                                ([b-or-arr]
                                 (if (int? b-or-arr)
                                   (.write buffer (int b-or-arr))
                                   (.write buffer ^bytes b-or-arr)))
                                ([bytes off len]
                                 (.write buffer ^bytes bytes (int off) (int len))))
                              (close [])
                              (flush []))]

          (when body
            (ring-protocols/write-body-to-stream body ring-resp output-stream))

          (let [body-bytes (.toByteArray buffer)
                body-len (alength body-bytes)
                body-ptr-raw (h2o/mem-alloc-shared pool-ptr body-len
                                                   java.lang.foreign.MemorySegment/NULL)
                body-ptr (mem/reinterpret body-ptr-raw body-len)]
            (when (pos? body-len)
              (java.lang.foreign.MemorySegment/copy
               (java.lang.foreign.MemorySegment/ofArray body-bytes) 0
               body-ptr 0 body-len))
            (let [sendvec-size (mem/size-of ::h2o/h2o-sendvec-t)
                  sendvec-ptr-raw (h2o/mem-alloc-shared pool-ptr sendvec-size
                                                        java.lang.foreign.MemorySegment/NULL)
                  sendvec-seg (mem/reinterpret sendvec-ptr-raw sendvec-size)]
              (h2o/sendvec-init-raw sendvec-seg body-ptr-raw body-len)
              (let [generator-ptr (h2o/get-static-generator)]
                (h2o/start-response req-ptr generator-ptr))
              (h2o/sendvec req-ptr sendvec-seg 1 h2o/H2O_SEND_STATE_FINAL))))))

    (catch Exception e
      (println "Error sending response:" (.getMessage e))
      (.printStackTrace e))))


