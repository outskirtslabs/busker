(ns ol.h2o.native.raw
  "Fast h2o struct accessors using jextract-generated Java classes.
   
   These functions provide near-C performance by using jextract's optimized
   static methods that compile down to simple pointer arithmetic.
   
   Performance: ~2ns per field access (vs ~500ns+ with coffi deserialize)"
  (:require
   [coffi.mem :as mem])
  (:import
   [net.example.h2o st_h2o_req_t st_h2o_res_t
    st_h2o_iovec_t h2o_headers_t st_h2o_header_t]
   [java.lang.foreign MemorySegment]))

;;
;; NOTE: Request metadata extraction (build-ring-request) has been moved to
;; ol.h2o.native using C shim functions for better performance and reliability.
;; This namespace now only contains response-related helpers.
;;

(defn get-req-pool
  "Get memory pool from h2o_req_t.
   Returns MemorySegment of h2o_mem_pool_t."
  [req-ptr]
  (st_h2o_req_t/pool req-ptr))

;;
;; Response field accessors
;;

(defn get-res
  "Get response struct from h2o_req_t.
   Returns MemorySegment of h2o_res_t."
  [req-ptr]
  (st_h2o_req_t/res req-ptr))

(defn get-res-status
  "Get response status from h2o_req_t."
  [req-ptr]
  (let [res-seg (st_h2o_req_t/res req-ptr)]
    (st_h2o_res_t/status res-seg)))

(defn get-res-headers
  "Get response headers from h2o_req_t.
   Returns MemorySegment of h2o_headers_t."
  [req-ptr]
  (let [res-seg (st_h2o_req_t/res req-ptr)]
    (st_h2o_res_t/headers res-seg)))

(defn set-res-status!
  "Set response status in h2o_req_t."
  [req-ptr status]
  (let [res-seg (st_h2o_req_t/res req-ptr)]
    (st_h2o_res_t/status res-seg status)))

(defn set-res-reason!
  "Set response reason phrase in h2o_req_t."
  [req-ptr reason-ptr]
  (let [res-seg (st_h2o_req_t/res req-ptr)]
    (st_h2o_res_t/reason res-seg reason-ptr)))

(defn set-res-content-length!
  "Set response content-length in h2o_req_t."
  [req-ptr content-length]
  (let [res-seg (st_h2o_req_t/res req-ptr)]
    (st_h2o_res_t/content_length res-seg content-length)))

;;
;; IOVec accessors
;;

(defn iovec-base
  "Get base pointer from h2o_iovec_t MemorySegment."
  [iovec-seg]
  (st_h2o_iovec_t/base iovec-seg))

(defn iovec-len
  "Get length from h2o_iovec_t MemorySegment."
  [iovec-seg]
  (st_h2o_iovec_t/len iovec-seg))

(defn iovec->map
  "Convert h2o_iovec_t MemorySegment to map {:base :len}."
  [iovec-seg]
  {:base (st_h2o_iovec_t/base iovec-seg)
   :len (st_h2o_iovec_t/len iovec-seg)})

;;
;; Headers accessors
;;

(defn headers-entries
  "Get entries pointer from h2o_headers_t MemorySegment."
  [headers-seg]
  (h2o_headers_t/entries headers-seg))

(defn headers-size
  "Get size from h2o_headers_t MemorySegment."
  [headers-seg]
  (h2o_headers_t/size headers-seg))

(defn headers-capacity
  "Get capacity from h2o_headers_t MemorySegment."
  [headers-seg]
  (h2o_headers_t/capacity headers-seg))

(defn headers->map
  "Convert h2o_headers_t MemorySegment to map {:entries :size :capacity}."
  [headers-seg]
  {:entries (h2o_headers_t/entries headers-seg)
   :size (h2o_headers_t/size headers-seg)
   :capacity (h2o_headers_t/capacity headers-seg)})

;;
;; Header entry accessors
;;

(defn header-name
  "Get name iovec from h2o_header_t MemorySegment."
  [header-seg]
  (st_h2o_header_t/name header-seg))

(defn header-value
  "Get value iovec from h2o_header_t MemorySegment."
  [header-seg]
  (st_h2o_header_t/value header-seg))
