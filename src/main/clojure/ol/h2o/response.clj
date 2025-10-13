(ns ol.h2o.response
  "Response handling for h2o HTTP server."
  (:require
   [ol.h2o.native :as h2o]
   [ol.h2o.native.raw :as raw]
   [clojure.string :as str]
   [coffi.mem :as mem]
   [ring.core.protocols :as ring-protocols])
  (:import
   [java.lang.foreign Arena MemorySegment]))

(set! *warn-on-reflection* true)

(defn- find-header
  "Find a header value by name (case-insensitive)."
  [headers header-name]
  (let [target-lower (str/lower-case header-name)]
    (some (fn [[k v]]
            (when (= (str/lower-case (str k)) target-lower)
              v))
          headers)))

(defn- dissoc-header
  "Remove all case variations of a header by name (case-insensitive)."
  [headers header-name]
  (let [target-lower (str/lower-case header-name)]
    (into {}
          (remove (fn [[k _]]
                    (= (str/lower-case (str k)) target-lower))
                  headers))))

(defn- parse-content-length [headers]
  (when-let [cl (find-header headers "content-length")]
    (try
      (Long/parseLong (str cl))
      (catch Exception _ nil))))

(defn- copy-string-to-segment [s pool-ptr]
  (let [bytes (.getBytes ^String s "UTF-8")
        len (alength bytes)
        ptr-raw (h2o/mem-alloc-shared pool-ptr len MemorySegment/NULL)
        ptr (mem/reinterpret ptr-raw len)]
    (MemorySegment/copy (MemorySegment/ofArray bytes) 0 ptr 0 len)
    {:ptr ptr-raw :len len}))

(defn- add-header! [pool-ptr res-headers-ptr name value]
  (let [name-lower (str/lower-case (str name))
        name-orig (str name)
        value-str (str value)
        {:keys [ptr len]} (copy-string-to-segment name-lower pool-ptr)
        name-lower-ptr ptr
        name-lower-len len
        {:keys [ptr len]} (copy-string-to-segment name-orig pool-ptr)
        name-orig-ptr ptr
        name-orig-len len
        {:keys [ptr len]} (copy-string-to-segment value-str pool-ptr)
        value-ptr ptr
        value-len len]
    (h2o/add-header-by-str pool-ptr res-headers-ptr
                           name-lower-ptr name-lower-len
                           0
                           name-orig-ptr
                           value-ptr value-len)))

(defn- set-headers! [req-ptr headers]
  (let [req-seg (mem/reinterpret req-ptr 2048)
        pool-ptr (raw/get-req-pool req-seg)
        res-headers-ptr (h2o/req-get-res-headers req-ptr)
        content-length (parse-content-length headers)
        headers-to-send (dissoc-header headers "content-length")]

    (when content-length
      (raw/set-res-content-length! req-seg content-length))

    (doseq [[name value] headers-to-send]
      (add-header! pool-ptr res-headers-ptr name value))))

(defn- body->bytes [body ring-resp]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (ring-protocols/write-body-to-stream body ring-resp baos)
    (.toByteArray baos)))

(defn- send-body! [req-ptr body-bytes arena]
  (let [req-seg (mem/reinterpret req-ptr 2048)
        pool-ptr (raw/get-req-pool req-seg)
        body-len (alength ^bytes body-bytes)]
    (when (pos? body-len)
      (let [body-seg-raw (h2o/mem-alloc-shared pool-ptr body-len MemorySegment/NULL)
            body-seg (mem/reinterpret body-seg-raw body-len)]
        (MemorySegment/copy (MemorySegment/ofArray ^bytes body-bytes) 0
                            body-seg 0 body-len)
        (let [vec-seg (mem/alloc (mem/size-of ::h2o/h2o-sendvec-t) arena)]
          (h2o/sendvec-init-raw vec-seg body-seg-raw body-len)
          (h2o/sendvec req-ptr vec-seg 1 h2o/H2O_SEND_STATE_FINAL))))))

(defn- send-empty-final! [req-ptr arena]
  (let [vec-seg (mem/alloc (mem/size-of ::h2o/h2o-sendvec-t) arena)]
    (h2o/sendvec-init-raw vec-seg MemorySegment/NULL 0)
    (h2o/sendvec req-ptr vec-seg 1 h2o/H2O_SEND_STATE_FINAL)))

(defn send-ring-response!
  "Send Ring response via h2o FFI using simple synchronous model.
   MUST be called from the worker thread that owns the request.

   Parameters:
   - req-ptr: Native pointer to h2o_req_t
   - ring-resp: Ring response map {:status :headers :body}"
  [req-ptr ring-resp]
  (let [arena (Arena/ofConfined)]
    (try
      (println "SEND RING RESP")
      #_(let [{:keys [status headers body]
               :or {status 500 headers {}}} ring-resp
              req-seg (mem/reinterpret req-ptr 2048)
              pool-ptr (raw/get-req-pool req-seg)
              {:keys [ptr len]} (copy-string-to-segment "OK" pool-ptr)]

          (raw/set-res-status! req-seg status)
          (raw/set-res-reason! req-seg ptr)
          (set-headers! req-ptr headers)
          (h2o/start-response req-ptr (h2o/get-static-generator))

          (if body
            (send-body! req-ptr (body->bytes body ring-resp) arena)
            (send-empty-final! req-ptr arena)))

      (catch Exception e
        (println "Error sending response:" (.getMessage e))
        (.printStackTrace e))
      (finally
        (.close arena)))))
