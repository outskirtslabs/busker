(ns ol.h2o.response
  "Response handling for h2o HTTP server."
  (:require
   [coffi.mem :as mem]
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols :as p]
   [ol.h2o.protocols.content-length :as content-length]
   [ol.h2o.response-channel :as response-channel]
   [ol.h2o.response-queue :as response-queue]
   [ol.h2o.util :as util]
   [ring.core.protocols :as ring-protocols])
  (:import
   [java.io OutputStream]
   [java.lang.foreign MemorySegment]
   [ol.h2o.protocols Request]))

(set! *warn-on-reflection* true)

(defn dissoc-header
  "Remove all case variations of a header by name (case-insensitive)."
  [headers ^String header-name]
  (let [target-lower (.toLowerCase header-name java.util.Locale/ROOT)]
    (into {}
          (remove (fn [[k _]]
                    (= (.toLowerCase (str k) java.util.Locale/ROOT) target-lower))
                  headers))))

(defn- coerce-content-length
  [val]
  (if (number? val) (long val)
      (Long/parseLong val)))

;; TODO intern common header names?

(defn build-headers
  "Return a tuple [headers headers-len content-length]
  - headers is a clj_header_t *headers MemorySegment
  - headers-len is size_t (number of headers, aka length of headers array)
  - content-length is the content length header if any was found or -1 (SIZE_MAX)

  headers memorysegment should NOT contain content-length if it was found"
  [resp]
  (let [headers (:headers resp)
        content-length-val (second (util/find-header resp "content-length"))
        content-length (if content-length-val
                         (coerce-content-length content-length-val)
                         -1)
        filtered-headers (if content-length-val
                           (dissoc-header headers "content-length")
                           headers)
        header-pairs (vec filtered-headers)
        headers-count (count header-pairs)]

    (if (zero? headers-count)
      [(mem/as-segment 0) 0 content-length]

      (let [header-size (mem/size-of ::h2o/clj-header-t)
            total-size (* headers-count header-size)
            headers-seg (mem/alloc total-size)]

        (doseq [[idx [name-key value-val]] (map-indexed vector header-pairs)]
          (let [offset (* idx header-size)
                name-str (if (string? name-key) name-key (str name-key))
                value-str (if (string? value-val) value-val (str value-val))
                name-ptr ^MemorySegment (mem/serialize name-str ::mem/c-string)
                value-ptr ^MemorySegment (mem/serialize value-str ::mem/c-string)
                header-data {:name name-ptr
                             :name_len (dec (.byteSize name-ptr))
                             :value value-ptr
                             :value_len (dec (.byteSize value-ptr))}
                header-seg (mem/serialize header-data ::h2o/clj-header-t)
                dest-seg (mem/slice headers-seg offset header-size)]
            (mem/copy-segment dest-seg header-seg)))
        [headers-seg headers-count content-length]))))

(defn with-cl-or-te [response]
  (if (util/get-header response "content-length")
    response
    (if-let [size (content-length/body-size-in-bytes (:body response) response)]
      (util/header response "content-length" (str size))
      response)))

(def use-new-queue? true)

(defn with-response-writer [req]
  (assoc req :write-resp
         (if use-new-queue?
           (response-queue/create-response-queue req)
           (response-channel/create-write-res-channel req {}))))

(defn send-ring-response!
  "Send a Ring response map using StreamableResponseBody protocol."
  [^Request req ring-resp]
  (try
    (let [{:keys     [status body]
           :h2o/keys [compress-hint]
           :as       ring-resp}                      (with-cl-or-te ring-resp)
          [headers headers-len content-length] (build-headers ring-resp)
          ^OutputStream out-stream             (-> req :write-resp :out-stream)
          compress-hint                        (get h2o/->compress-hint compress-hint h2o/H2O_COMPRESS_HINT_ENABLE)]
      (p/send-msg (:worker req)
                  [:h2o/start-response
                   (fn []
                     (try
                       (h2o/start-response (:req-ctx-ptr req) status
                                           headers headers-len
                                           content-length compress-hint
                                           (-> req :write-resp :on-proceed-cb-ptr)
                                           (-> req :write-resp :on-stop-cb-ptr))
                       (catch Throwable t
                         (println "THROWABLE in response write:" t))))])
      (if body
        (ring-protocols/write-body-to-stream body ring-resp out-stream)
        (.close ^OutputStream out-stream)))

    (catch Exception e
      (println "EXCEPTION in response:" e)
      (.printStackTrace e)))
  nil)
