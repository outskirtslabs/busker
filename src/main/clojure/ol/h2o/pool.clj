(ns ol.h2o.pool
  "A generic, thread-safe resource pool with fixed capacity and regeneration."
  (:require [taoensso.trove :as trove])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(set! *warn-on-reflection* true)

(defprotocol Pool
  (grow [pool]
    "Attempt to create and enqueue a new resource. Retries on failure according
     to regenerate-interval set at construction time.")
  (borrow [pool] [pool timeout-seconds]
    "Obtain a resource from the pool.
     - (borrow pool) is non-blocking; returns the resource or nil if none.
     - (borrow pool timeout-seconds) waits up to timeout-seconds; throws on timeout.
     Interrupted waits return nil.")
  (release [pool resource]
    "Return a resource to the pool (no-op for nil).")
  (invalidate [pool resource]
    "Mark a resource as invalid and schedule regeneration (no-op for nil).")
  (stats [pool]
    "Return a map describing current pool stats."))

(defrecord FixedPool
    [^LinkedBlockingQueue q
     open-fn                   ;; () -> resource (or throws / returns nil)
     close-fn                  ;; (resource) -> nil
     ^long regenerate-interval ;; seconds between regen attempts when open fails
     ^long capacity            ;; max resources in pool
     block-start?]             ;; if true, prefill before returning from constructor
  Pool
  (grow [_]
    (loop []
      (if-let [r (try (open-fn) (catch Exception e nil))]
        (.put ^LinkedBlockingQueue q r)
        (do
          (Thread/sleep (* 1000 regenerate-interval))
          (recur)))))

  (borrow [this] (borrow this nil))
  (borrow [_ timeout-seconds]
    (let [ms (long (* 1000 (or timeout-seconds 0)))]
      (or (try (.poll q ms TimeUnit/MILLISECONDS)
               (catch InterruptedException _
                 ;; Treat interrupt as 'no resource' to let caller decide (e.g. retry).
                 nil))
          (throw (ex-info (str "Pool borrow timed out after " ms " ms")
                          {:type       ::timeout
                           :timeout-ms ms})))))
  (release [_ resource] (when resource (.put q resource)))
  (invalidate [this resource]
    (when resource
      (try
        (when close-fn (close-fn resource))
        (catch Throwable t (trove/log! {:level :error
                                        :id    ::close-failed-on-invalidate
                                        :msg   "Pool close-fn threw on invalidate"
                                        :error t
                                        :data  {:resource resource}})))
      ;; Replace the invalidated item asynchronously to avoid blocking caller.
      (future (grow this))))

  (stats [_this]
    {:capacity  capacity
     :available (.size q)}))

(defn fixed-pool
  "Construct a fixed-size pool of resources.

   Options:
   - :size (int, default: 2 * availableProcessors)
   - :regenerate-interval (seconds, default 5)
   - :block-start? (boolean, default true) prefill before returning

   open-fn: () -> resource (may throw or return nil to indicate failure)
   close-fn: (resource) -> nil (may throw; will be logged)

   Returns a FixedPool that implements Pool."
  ([open-fn]
   (fixed-pool open-fn {}))
  ([open-fn opts-or-close-fn]
   (if (map? opts-or-close-fn)
     (fixed-pool open-fn (constantly nil) opts-or-close-fn)
     (fixed-pool open-fn opts-or-close-fn {})))
  ([open-fn close-fn {:keys [size regenerate-interval block-start?]
                      :or   {size                (* 2 (.availableProcessors (Runtime/getRuntime)))
                             regenerate-interval 5
                             block-start?        true}}]
   (let [q        (LinkedBlockingQueue. (int size))
         pool     (->FixedPool q open-fn close-fn regenerate-interval size block-start?)
         starters (doall
                   (map (fn [_] (future (grow pool)))
                        (range size)))]
     (when block-start?
       (doseq [f starters] @f))
     pool)))

(defmacro with-pool
  "Borrows a resource from `pool` with optional timeout (seconds), executes body, and
   guarantees the resource is either released (on success) or invalidated (on error).
   Usage:
     (with-pool [res pool 5] (do-something res))"
  [[sym pool timeout-seconds] & body]
  `(let [~sym (borrow ~pool ~timeout-seconds)]
     (try
       (let [result# (do ~@body)]
         (release ~pool ~sym)
         result#)
       (catch Throwable t#
         (invalidate ~pool ~sym)
         (throw t#)))))
