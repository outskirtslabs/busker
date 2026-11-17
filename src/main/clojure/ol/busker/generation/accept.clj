(ns ^:no-doc ol.busker.generation.accept
  "Accepts connections on a worker's listener sockets and spreads them over
  the workers by their connection counts. The callbacks run on the worker
  thread.

  ## Related Namespaces

  - [[ol.busker.generation]] runs the workers."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicLongArray AtomicReferenceArray]))

(set! *warn-on-reflection* true)

;; Connection spread. `counts` holds each worker's TCP and Unix socket
;; connections; only that worker writes its slot, from its accept and close
;; callbacks. A worker reads its listeners only while [[at-minimum?]] holds.
;; It decides in every loop turn before it blocks, and an accept that raises the
;; minimum wakes the others, so a worker that became eligible decides again.
;; A worker stopping for the global limit sets `limit-waiting`, which a release
;; clears with a wake. A failed worker's slot is Long/MAX_VALUE, and during
;; shutdown no worker starts reading.

(defn at-minimum?
  "Returns true when `c` is not above any count in `counts`."
  [^long c ^AtomicLongArray counts]
  (let [n (.length counts)]
    (loop [i 0]
      (cond
        (== i n) true
        (> c (.get counts (int i))) false
        :else (recur (inc i))))))

(defn wake-other-workers!
  "Wakes every started worker in `workers` except the one at `worker-idx`."
  [^AtomicReferenceArray workers ^long worker-idx]
  (let [n (.length workers)]
    (loop [i 0]
      (when (< i n)
        (when-not (== i worker-idx)
          (when-let [worker (.get workers i)]
            (p/wake worker)))
        (recur (inc i))))))

(defn log-callback-failure!
  "Logs a failure of a native callback. Never throws, since the callback
  returns to native code."
  [id ^Throwable t]
  (try
    (when (instance? InterruptedException t)
      (.interrupt (Thread/currentThread)))
    (trove/log! {:level :error :id id :error t})
    (catch Throwable _ nil)))

(defn create-connection-close-callback
  [active-connection-count_ ^AtomicLongArray counts worker-idx]
  (let [worker-idx (long worker-idx)
        cb (fn [_]
             (try
               (.decrementAndGet ^AtomicLong active-connection-count_)
               (.decrementAndGet counts (int worker-idx))
               (h2o/conn-limit-release)
               (catch Throwable t
                 (log-callback-failure! ::connection-close-callback-failed t))))]
    {::connection-close-cb cb
     ::connection-close-cb-ptr (mem/callback (mem/auto-arena) cb [:pointer] :void)
     ::accept-failure-run {:started (AtomicLong. Long/MIN_VALUE)
                           :failed-worker? (AtomicBoolean.)}}))

(def ^:const accept-failure-run-limit-nanos
  "How long a run of failed accepts, from its first failure to the next
  completed accept, may last before the worker is failed."
  1000000000)

(defn- now-nanos
  []
  (System/nanoTime))

(defn- attempt!
  [f]
  (try (f) (catch Throwable _ nil)))

(defn- undo-failed-accept!
  "Gives back what an accept took before it failed at `stage`. From `:closable`
  on, closing the socket runs its close callback, which releases the slot and
  the counts; at `:handed` H2O has the socket and nothing is given back."
  [stage sock-ptr ^AtomicLong active-connection-count_ ^AtomicLongArray counts worker-idx]
  (when (= :counted stage)
    (attempt! #(.decrementAndGet active-connection-count_))
    (attempt! #(.decrementAndGet counts (int worker-idx))))
  (when (#{:limited :counted} stage)
    (attempt! h2o/conn-limit-release))
  (when (#{:accepted :limited :counted :closable} stage)
    (attempt! #(h2o/socket-close sock-ptr))))

(defn- end-accept-failure-run!
  [{:keys [^AtomicLong started ^AtomicBoolean failed-worker?]}]
  (.set started Long/MIN_VALUE)
  (.set failed-worker? false))

(defn- accept-failed!
  "Stops `listener-ptr` for this event-loop turn after a failed accept of the
  worker at `worker-idx`. The first failure of a run starts it and is logged;
  when the run has lasted [[accept-failure-run-limit-nanos]], the worker is
  failed once."
  [^AtomicReferenceArray workers worker-idx
   {:keys [^AtomicLong started ^AtomicBoolean failed-worker?]} listener-ptr ^Throwable t]
  (attempt! #(h2o/socket-read-stop listener-ptr))
  (attempt!
   #(let [now (now-nanos)]
      (if (= Long/MIN_VALUE (.get started))
        (do (.set started now)
            (log-callback-failure! ::accept-callback-failed t))
        (when (and (>= (- now (.get started)) accept-failure-run-limit-nanos)
                   (.compareAndSet failed-worker? false true))
          (let [worker (.get workers (int worker-idx))]
            (log-callback-failure! ::accept-failures-failed-worker t)
            (when-let [fail (:on-native-failure! worker)]
              (fail worker t))))))))

(defn create-accept-callback
  [accept-ctx-ptr {::keys [connection-close-cb-ptr accept-failure-run]}
   active-connection-count_ ^AtomicLongArray counts workers worker-idx]
  (let [worker-idx (long worker-idx)
        accept-cb (fn [listener-ptr _err-ptr]
                    (let [stage_ (volatile! nil)
                          sock_ (volatile! nil)]
                      (try
                        (if-not (at-minimum? (.get counts (int worker-idx)) counts)
                          (h2o/socket-read-stop listener-ptr)
                          (let [sock-ptr (h2o/evloop-socket-accept listener-ptr)]
                            (when-not (mem/null? sock-ptr)
                              (vreset! sock_ sock-ptr)
                              (vreset! stage_ :accepted)
                              (if (pos? (h2o/conn-limit-try-acquire))
                                (do
                                  (vreset! stage_ :limited)
                                  (.incrementAndGet ^AtomicLong active-connection-count_)
                                  (let [own (.incrementAndGet counts (int worker-idx))]
                                    (vreset! stage_ :counted)
                                    (h2o/socket-set-on-close sock-ptr connection-close-cb-ptr mem/null)
                                    (vreset! stage_ :closable)
                                    (h2o/h2o-accept accept-ctx-ptr sock-ptr)
                                    (vreset! stage_ :handed)
                                    (end-accept-failure-run! accept-failure-run)
                                    (when (at-minimum? own counts)
                                      (wake-other-workers! workers worker-idx))))
                                (do
                                  (vreset! stage_ nil)
                                  (h2o/socket-close sock-ptr))))))
                        (catch Throwable t
                          (undo-failed-accept! @stage_ @sock_ active-connection-count_
                                               counts worker-idx)
                          (accept-failed! workers worker-idx accept-failure-run listener-ptr t)))))]
    {::accept-cb accept-cb
     ::accept-cb-ptr (mem/callback (mem/auto-arena) accept-cb [:pointer :pointer] :void)}))

(defn update-listener-state!
  "Starts or stops reading this worker's listener sockets by the global
  connection limit and the connection spread rule."
  [{:keys [listener-socks accept-callbacks max-connections worker-connection-counts
           worker-idx accepting-workers limit-waiting]}]
  (let [^AtomicLongArray counts worker-connection-counts
        ^AtomicBoolean limit-waiting limit-waiting
        worker-idx (long worker-idx)
        limit-allows? (or (zero? max-connections)
                          (< (h2o/conn-limit-current) max-connections)
                          (do (.set limit-waiting true)
                              (< (h2o/conn-limit-current) max-connections)))
        _ (when (and limit-allows? (.get limit-waiting) (.compareAndSet limit-waiting true false))
            (wake-other-workers! accepting-workers worker-idx))
        should-accept? (and limit-allows?
                            (at-minimum? (.get counts (int worker-idx)) counts))]
    (doseq [listener-idx (range (count listener-socks))]
      (let [sock-ptr (nth listener-socks listener-idx)
            accept-callback (nth accept-callbacks listener-idx)]
        (when-not (mem/null? sock-ptr)
          (if should-accept?
            (when (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-start sock-ptr accept-callback))
            (when-not (zero? (h2o/socket-reading? sock-ptr))
              (h2o/socket-read-stop sock-ptr))))))))
