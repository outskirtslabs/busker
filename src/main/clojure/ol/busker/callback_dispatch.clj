(ns ^:no-doc ol.busker.callback-dispatch
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [ol.busker.byte-bounded-queue :as bbq]
   [ol.busker.response :as response]
   [ol.busker.response-queue :as response-queue]
   [taoensso.trove :as trove])
  (:import
   [java.io InputStream]
   [java.lang Thread]
   [java.util HashMap]
   [java.util.concurrent.atomic AtomicLong AtomicReference]
   [ol.busker.response H2OResponseEmitter]))

(set! *warn-on-reflection* true)

(defonce ^:private next-module-id_ (AtomicLong. 0))

(defrecord CallbackDispatch
           [^long module-id
            ^AtomicLong next-request-seq_
            ^AtomicReference phase_
            ^AtomicReference thread_
            ^HashMap entries
            counters
            body-callback
            proceed-callback
            stop-callback
            body-callback-ptr
            proceed-callback-ptr
            stop-callback-ptr])

(defn- increment!
  [dispatch counter]
  (.incrementAndGet ^AtomicLong (get (:counters dispatch) counter)))

(defn- next-positive!
  [^AtomicLong counter label]
  (loop []
    (let [current (.get counter)]
      (when (= Long/MAX_VALUE current)
        (throw (ex-info "Callback dispatch identity space exhausted"
                        {:identity label})))
      (let [next (inc current)]
        (if (.compareAndSet counter current next)
          next
          (recur))))))

(defn- callback-phase?
  [dispatch]
  (contains? #{:accepting :quiescing} (.get ^AtomicReference (:phase_ dispatch))))

(defn- worker-thread?
  [dispatch]
  (identical? (Thread/currentThread) (.get ^AtomicReference (:thread_ dispatch))))

(defn- live-entry
  [dispatch module-id request-seq]
  (cond
    (not (callback-phase? dispatch))
    (do
      (increment! dispatch :late)
      nil)

    (not (worker-thread? dispatch))
    (do
      (increment! dispatch :wrong-thread)
      nil)

    (or (not= (:module-id dispatch) module-id)
        (not (pos? request-seq)))
    (do
      (increment! dispatch :malformed)
      nil)

    :else
    (or (.get ^HashMap (:entries dispatch) request-seq)
        (do
          (increment! dispatch :late)
          nil))))

(defn entry
  "Returns the live request entry for `module-id` and `request-seq`, if any."
  [dispatch module-id request-seq]
  (live-entry dispatch module-id request-seq))

(defn- close-entry!
  [[req emitter]]
  (try
    (when-some [^InputStream input-stream (-> req :write-req :input-stream)]
      (.close input-stream))
    (when emitter
      (response/stop-emitter emitter))
    (catch Throwable t
      (trove/log! {:level :error
                   :id    ::entry-close-failed
                   :ex    t}))))

(defn- body-callback!
  [dispatch module-id request-seq chunk-seg chunk-len is-last]
  (try
    (when-let [[req _] (live-entry dispatch module-id request-seq)]
      (if-let [write-chunk (-> req :write-req :write-chunk)]
        (write-chunk
         (when-not (mem/null? chunk-seg)
           (mem/read-bytes (mem/reinterpret chunk-seg chunk-len) chunk-len))
         (= 1 is-last))
        (increment! dispatch :malformed)))
    (catch Throwable t
      (increment! dispatch :callback-fault)
      (trove/log! {:level :error
                   :id    ::body-callback-failed
                   :ex    t}))))

(defn- proceed-callback!
  [dispatch module-id request-seq]
  (try
    (when-let [[_ ^H2OResponseEmitter emitter] (live-entry dispatch module-id request-seq)]
      (response-queue/on-proceed (.write-resp emitter)))
    (catch Throwable t
      (increment! dispatch :callback-fault)
      (trove/log! {:level :error
                   :id    ::proceed-callback-failed
                   :ex    t}))))

(defn- stop-callback!
  [dispatch module-id request-seq reason]
  (try
    (when-let [[_ ^H2OResponseEmitter emitter] (live-entry dispatch module-id request-seq)]
      (response-queue/on-stop (.write-resp emitter) reason))
    (catch Throwable t
      (increment! dispatch :callback-fault)
      (trove/log! {:level :error
                   :id    ::stop-callback-failed
                   :ex    t}))))

(defn create
  "Creates a worker-affine callback dispatcher in `arena`."
  [arena]
  (let [module-id (next-positive! next-module-id_ :module)
        phase_ (AtomicReference. :initializing)
        thread_ (AtomicReference.)
        entries (HashMap. 100)
        counters {:malformed     (AtomicLong.)
                  :late          (AtomicLong.)
                  :wrong-thread  (AtomicLong.)
                  :callback-fault (AtomicLong.)
                  :duplicate      (AtomicLong.)
                  :forced         (AtomicLong.)}
        dispatch (->CallbackDispatch module-id
                                     (AtomicLong. 0)
                                     phase_
                                     thread_
                                     entries
                                     counters
                                     nil nil nil nil nil nil)
        body-callback (partial body-callback! dispatch)
        proceed-callback (partial proceed-callback! dispatch)
        stop-callback (partial stop-callback! dispatch)
        body-callback-ptr
        (mem/serialize body-callback
                       [::ffi/fn [::mem/long ::mem/long ::mem/pointer ::mem/long ::mem/int]
                        ::mem/void]
                       arena)
        proceed-callback-ptr
        (mem/serialize proceed-callback
                       [::ffi/fn [::mem/long ::mem/long] ::mem/void]
                       arena)
        stop-callback-ptr
        (mem/serialize stop-callback
                       [::ffi/fn [::mem/long ::mem/long ::mem/int] ::mem/void]
                       arena)]
    (assoc dispatch
           :body-callback body-callback
           :proceed-callback proceed-callback
           :stop-callback stop-callback
           :body-callback-ptr body-callback-ptr
           :proceed-callback-ptr proceed-callback-ptr
           :stop-callback-ptr stop-callback-ptr)))

(defn bind-thread!
  "Binds `dispatch` to its event-loop `thread`."
  [dispatch ^Thread thread]
  (when-not (.compareAndSet ^AtomicReference (:phase_ dispatch) :initializing :accepting)
    (throw (ex-info "Callback dispatch module cannot bind its worker thread"
                    {:phase (.get ^AtomicReference (:phase_ dispatch))})))
  (.set ^AtomicReference (:thread_ dispatch) thread)
  dispatch)

(defn begin-drain!
  "Stops new dispatch registration while admitted requests continue to run."
  [dispatch]
  (.compareAndSet ^AtomicReference (:phase_ dispatch) :accepting :quiescing)
  dispatch)

(defn callback-pointers
  "Returns the stable callback pointers for `dispatch`."
  [dispatch]
  {:body    (:body-callback-ptr dispatch)
   :proceed (:proceed-callback-ptr dispatch)
   :stop    (:stop-callback-ptr dispatch)})

(defn allocate-identity!
  "Allocates a nonrepeating dispatch identity for the current worker."
  [dispatch]
  (when-not (and (= :accepting (.get ^AtomicReference (:phase_ dispatch)))
                 (worker-thread? dispatch))
    (throw (ex-info "Callback dispatch module cannot allocate this request identity"
                    {:phase (.get ^AtomicReference (:phase_ dispatch))})))
  [(:module-id dispatch)
   (next-positive! ^AtomicLong (:next-request-seq_ dispatch) :request)])

(defn register!
  "Registers `req` and `emitter` under `module-id` and `request-seq`."
  [dispatch module-id request-seq req emitter]
  (when-not (and (= :accepting (.get ^AtomicReference (:phase_ dispatch)))
                 (worker-thread? dispatch)
                 (= (:module-id dispatch) module-id)
                 (pos? request-seq))
    (throw (ex-info "Callback dispatch module cannot register this request"
                    {:module-id module-id
                     :request-seq request-seq
                     :phase (.get ^AtomicReference (:phase_ dispatch))})))
  (let [previous (.put ^HashMap (:entries dispatch) request-seq [req emitter])]
    (when previous
      (.put ^HashMap (:entries dispatch) request-seq previous)
      (throw (ex-info "Callback dispatch request identity collision"
                      {:module-id module-id
                       :request-seq request-seq})))
    [module-id request-seq]))

(defn retire!
  "Removes and closes the entry identified by `module-id` and `request-seq`."
  [dispatch module-id request-seq]
  (when-not (worker-thread? dispatch)
    (increment! dispatch :wrong-thread)
    (throw (ex-info "Callback dispatch retirement ran on the wrong thread"
                    {:module-id module-id
                     :request-seq request-seq})))
  (if (or (not= (:module-id dispatch) module-id)
          (not (pos? request-seq)))
    (increment! dispatch :malformed)
    (if-let [entry (.remove ^HashMap (:entries dispatch) request-seq)]
      (close-entry! entry)
      (increment! dispatch :duplicate))))

(defn pending-response-work?
  "Returns true when this worker has queued or in-flight response data."
  [dispatch]
  (boolean
   (some
    (fn [[_ ^H2OResponseEmitter emitter]]
      (let [st (.write-resp emitter)]
        (or (.get ^java.util.concurrent.atomic.AtomicBoolean (:scheduled?_ st))
            (some? (.get ^java.util.concurrent.atomic.AtomicReference (:in-flight_ st)))
            (pos? (bbq/queued-bytes (:bbq st))))))
    (.values ^HashMap (:entries dispatch)))))

(defn finish!
  "Closes any unexpected entries after the worker has stopped."
  [dispatch]
  (let [entries ^HashMap (:entries dispatch)]
    (when-not (.isEmpty entries)
      (increment! dispatch :forced)
      (doseq [entry (vec (.values entries))]
        (close-entry! entry))
      (.clear entries))
    (.set ^AtomicReference (:phase_ dispatch) :closed)
    dispatch))

(defn no-live-entries?
  "Returns true when `dispatch` has no live request entries."
  [dispatch]
  (.isEmpty ^HashMap (:entries dispatch)))

(defn diagnostics
  "Returns callback-dispatch diagnostic counters."
  [dispatch]
  (into {}
        (map (fn [[k ^AtomicLong counter]] [k (.get counter)]))
        (:counters dispatch)))
