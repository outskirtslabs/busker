(ns ol.h2o.buffer-pool
  "Buffer mechanics layered atop ol.util.pool: pool fixed-size direct ByteBuffers.
   This keeps the pool generic and isolates ByteBuffer concerns here."
  (:require [ol.h2o.pool :as pool])
  (:import [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn- alloc-direct ^ByteBuffer [^long size]
  (ByteBuffer/allocateDirect (int size)))

(defn make-bytebuffer-pool
  "Create a fixed-size pool of direct ByteBuffers of canonical `buf-size` bytes.

   Options:
   - :buf-size         buffer capacity in bytes (required)
   - :pool-size        number of buffers in pool (default: 2 * cores)
   - :regenerate-interval seconds between retries when allocation fails (default 5)
   - :block-start?     prefill pool before returning (default true)

   Returns a Pool; each borrowed buffer is cleared (position=0, limit=capacity)."
  [{:keys [pool-size buf-size regenerate-interval block-start?]
    :or {pool-size (* 2 (.availableProcessors (Runtime/getRuntime)))
         regenerate-interval 5
         block-start? true}}]
  (assert (pos? (long buf-size)) ":buf-size must be positive")
  (let [open (fn ^ByteBuffer [] (alloc-direct buf-size))
        close (fn [^ByteBuffer _buf] nil)]
    (pool/fixed-pool open close {:size pool-size
                                 :regenerate-interval regenerate-interval
                                 :block-start? block-start?})))
