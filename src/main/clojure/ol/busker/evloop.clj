(ns ^:no-doc ol.busker.evloop
  (:require
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.internal.protocols :as p]
   [ol.busker.wake-notifier :as notifier]
   [taoensso.trove :as trove])
  (:import
   [java.util HashMap]
   [java.util.concurrent ArrayBlockingQueue]
   [java.util.concurrent.locks Condition ReentrantLock]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

(declare admit-message! request-stop! required-message! stop-msg)

;; ------------------------------
;; Worker control-plane primitives
;; ------------------------------

(defrecord Worker
           [id
            thread
            ^AtomicBoolean running?_
            ^AtomicBoolean accepting?_
            ^AtomicBoolean stop-requested?_
            ^AtomicInteger admissions_
            ^AtomicInteger mailbox-waiters_
            ^ReentrantLock mailbox-lock
            ^Condition mailbox-space
            ^AtomicLong mailbox-signal_
            ^AtomicLong wake-generation_
            ^AtomicBoolean sleep-armed?_
            wake-notifier
            wake-endpoint
            mailbox
            evloop
            loop-fn
            message-handler
            ^AtomicReference wakeup-receiver_
            ^AtomicReference response-receiver_
            ^AtomicReference fixed-final-scratch_
            callback-dispatch
            ^HashMap requests
            args]
  p/WorkerThread
  (running? [_]
    (and (.get running?_) (.get accepting?_)))
  (wake [_]
    (when (.get running?_)
      (.incrementAndGet wake-generation_)
      (when (.get sleep-armed?_)
        (notifier/request! wake-notifier wake-endpoint))))
  (send-msg [this msg]
    (if (= stop-msg msg)
      (request-stop! this)
      (admit-message! this msg)))
  (send-required-msg [this msg]
    (if (= stop-msg msg)
      (request-stop! this)
      (required-message! this msg)))
  (count-msgs [_] (.size ^ArrayBlockingQueue mailbox))
  (add-req [_ req-id r]
    (.put requests req-id r))
  (reap-req [_ req-id]
    (.remove requests req-id)))

(defonce ^:private next-id_ (atom 0))

;; thread local to store current worker on each platform thread
(def ^ThreadLocal worker-context (ThreadLocal.))

(defn get-current-worker
  "Get the Worker record for the current thread.
   Only valid when called from a worker thread."
  []
  (.get worker-context))

;; ------------------------------
;; Control messages
;; ------------------------------

(def stop-msg [::stop])

;; ------------------------------
;; Worker loop
;; ------------------------------

(defn- offer-mailbox!
  [^Worker worker msg]
  (.offer ^ArrayBlockingQueue (:mailbox worker) msg))

(defn- log-mailbox-wakeup-failure!
  [worker msg first-error second-error reset?]
  (trove/log! {:level :error
               :id    ::mailbox-wakeup-failed
               :ex    second-error
               :data  {:worker-id (:id worker)
                       :message msg
                       :first-error first-error
                       :reset? reset?}}))

(defn- signal-mailbox!
  [^Worker worker msg]
  (loop []
    (let [^AtomicLong signal_ (:mailbox-signal_ worker)
          token (.get signal_)]
      (cond
        (odd? token)
        nil

        (not (.compareAndSet signal_ token (inc token)))
        (recur)

        :else
        (try
          (p/wake worker)
          (catch Throwable first-error
            (when (.compareAndSet signal_ (inc token) (+ token 2))
              (when (.compareAndSet signal_ (+ token 2) (+ token 3))
                (try
                  (p/wake worker)
                  (catch Throwable second-error
                    (let [reset? (.compareAndSet signal_ (+ token 3) (+ token 4))]
                      (log-mailbox-wakeup-failure! worker msg first-error
                                                   second-error reset?))))))))))))

(defn- signal-mailbox-space!
  [^Worker worker]
  (when (pos? (.get ^AtomicInteger (:mailbox-waiters_ worker)))
    (let [^ReentrantLock lock (:mailbox-lock worker)]
      (.lock lock)
      (try
        (.signalAll ^Condition (:mailbox-space worker))
        (finally
          (.unlock lock))))))

(defn- await-mailbox-space!
  [^Worker worker]
  (let [^AtomicInteger waiters_ (:mailbox-waiters_ worker)
        ^ReentrantLock lock (:mailbox-lock worker)
        ^Condition space (:mailbox-space worker)]
    (.incrementAndGet waiters_)
    (.lock lock)
    (try
      (while (and (.get ^AtomicBoolean (:running?_ worker))
                  (.get ^AtomicBoolean (:accepting?_ worker))
                  (zero? (.remainingCapacity ^ArrayBlockingQueue (:mailbox worker))))
        (.await space))
      (finally
        (.unlock lock)
        (.decrementAndGet waiters_)))))

(defn- admit-message!
  [^Worker worker msg]
  (if (and (.get ^AtomicBoolean (:running?_ worker))
           (.get ^AtomicBoolean (:accepting?_ worker)))
    (do
      (.incrementAndGet ^AtomicInteger (:admissions_ worker))
      (let [accepted?
            (try
              (and (.get ^AtomicBoolean (:accepting?_ worker))
                   (offer-mailbox! worker msg))
              (finally
                (.decrementAndGet ^AtomicInteger (:admissions_ worker))))]
        (if accepted?
          (do
            (signal-mailbox! worker msg)
            :accepted)
          (if (and (.get ^AtomicBoolean (:running?_ worker))
                   (.get ^AtomicBoolean (:accepting?_ worker)))
            :overloaded
            :closed))))
    :closed))

(defn- required-message!
  [^Worker worker msg]
  (loop []
    (case (admit-message! worker msg)
      :accepted :accepted
      :closed :closed
      :overloaded (do
                    (await-mailbox-space! worker)
                    (recur)))))

(defn- request-stop!
  [^Worker worker]
  (if (.compareAndSet ^AtomicBoolean (:accepting?_ worker) true false)
    (do
      (signal-mailbox-space! worker)
      (while (pos? (.get ^AtomicInteger (:admissions_ worker)))
        (Thread/onSpinWait))
      (.set ^AtomicBoolean (:stop-requested?_ worker) true)
      (p/wake worker)
      :accepted)
    :closed))

(defn- clear-mailbox-signal!
  [^Worker worker]
  (loop []
    (let [^AtomicLong signal_ (:mailbox-signal_ worker)
          token (.get signal_)]
      (if (even? token)
        token
        (if (.compareAndSet signal_ token (inc token))
          (inc token)
          (recur))))))

(defn- mark-mailbox-signalled!
  [^Worker worker]
  (loop []
    (let [^AtomicLong signal_ (:mailbox-signal_ worker)
          token (.get signal_)]
      (if (odd? token)
        token
        (if (.compareAndSet signal_ token (inc token))
          (inc token)
          (recur))))))

(defn- handle-message!
  [^Worker worker [op & args]]
  (if (= ::stop op)
    (request-stop! worker)
    (when-let [handler (:message-handler worker)]
      (handler op args))))

(defn- drain-mailbox!
  [^Worker worker]
  (loop [handled? false]
    (if-let [message (.poll ^ArrayBlockingQueue (:mailbox worker))]
      (do
        (handle-message! worker message)
        (recur true))
      (do
        (clear-mailbox-signal! worker)
        (if-let [message (.poll ^ArrayBlockingQueue (:mailbox worker))]
          (do
            (mark-mailbox-signalled! worker)
            (handle-message! worker message)
            (recur true))
          (do
            (when (.get ^AtomicBoolean (:stop-requested?_ worker))
              (.set ^AtomicBoolean (:running?_ worker) false))
            (when handled?
              (signal-mailbox-space! worker))
            handled?))))))

(defn- mailbox-empty?
  [^Worker worker]
  (.isEmpty ^ArrayBlockingQueue (:mailbox worker)))

(defn- run-evloop-on-thread!
  [^Worker worker]
  (.set worker-context worker)
  (try
    (let [^AtomicBoolean running?_ (:running?_ worker)
          ^AtomicLong wake-generation_ (:wake-generation_ worker)
          ^AtomicBoolean sleep-armed?_ (:sleep-armed?_ worker)
          loop-fn (:loop-fn worker)]
      (loop [state {}
             wake-seen (long 0)]
        (when (.get running?_)
          (let [mailbox-work? (drain-mailbox! worker)
                wake-requested (.get wake-generation_)]
            (when (.get running?_)
              (if (or mailbox-work? (not= wake-requested wake-seen))
                (recur (loop-fn worker (assoc state ::mailbox-work? true))
                       wake-requested)
                (do
                  (.set sleep-armed?_ true)
                  (if (or (not (mailbox-empty? worker))
                          (not= wake-requested (.get wake-generation_)))
                    (do
                      (.set sleep-armed?_ false)
                      (recur state wake-seen))
                    (let [next-state (try
                                       (loop-fn worker (assoc state ::mailbox-work? false))
                                       (finally
                                         (.set sleep-armed?_ false)))]
                      (recur next-state wake-requested))))))))))
    (catch InterruptedException _
      (.set ^AtomicBoolean (:running?_ worker) false))
    (catch Throwable error
      (println "[evloop] worker crashed:" (.getMessage error))
      (println error))
    (finally
      (try
        (.set ^AtomicBoolean (:accepting?_ worker) false)
        (signal-mailbox-space! worker)
        (finally
          (try
            (fixed-final/close-worker-scratch! worker)
            (finally
              (.remove worker-context))))))))

;; ------------------------------
;; Public API
;; ------------------------------

(defn start-worker!
  "Start a worker on a platform thread (daemon).

   Parameters:
   - loop-fn: function called for each loop iteration: (worker, state) -> state
   - message-handler
   - wakeup-receiver

   Options:
   - :thread-name-prefix - prefix for thread name (default 'h2o-evloop')
   - :callback-dispatch - worker-local native callback dispatcher
   - :wake-notifier - shared runtime notifier for native event-loop wakes
   - :response-receiver - native complete-response receiver

   Returns: worker"

  [loop-fn message-handler wakeup-receiver
   & {:keys [callback-dispatch wake-notifier response-receiver thread-name-prefix]
      :or {thread-name-prefix "h2o-evloop"}}]
  (when-not callback-dispatch
    (throw (ex-info "Worker requires a callback dispatcher" {})))
  (when-not wake-notifier
    (throw (ex-info "Worker requires a wake notifier" {})))
  (let [id (swap! next-id_ inc)
        receiver_ (AtomicReference. wakeup-receiver)
        response-receiver_ (AtomicReference. response-receiver)
        wake-endpoint (notifier/endpoint receiver_)
        mailbox-lock (ReentrantLock.)
        worker (map->Worker {:id id
                             :thread nil
                             :callback-dispatch callback-dispatch
                             :requests (:entries callback-dispatch)
                             :running?_ (AtomicBoolean. true)
                             :accepting?_ (AtomicBoolean. true)
                             :stop-requested?_ (AtomicBoolean. false)
                             :admissions_ (AtomicInteger.)
                             :mailbox-waiters_ (AtomicInteger.)
                             :mailbox-lock mailbox-lock
                             :mailbox-space (.newCondition mailbox-lock)
                             :mailbox-signal_ (AtomicLong.)
                             :wake-generation_ (AtomicLong.)
                             :sleep-armed?_ (AtomicBoolean. false)
                             :wake-notifier wake-notifier
                             :wake-endpoint wake-endpoint
                             :mailbox (ArrayBlockingQueue. 256)
                             :evloop nil
                             :loop-fn loop-fn
                             :message-handler message-handler
                             :wakeup-receiver_ receiver_
                             :response-receiver_ response-receiver_
                             :fixed-final-scratch_ (AtomicReference.)})
        thread (Thread. #(run-evloop-on-thread!
                          (assoc worker :thread (Thread/currentThread)))
                        (format "%s-%d" thread-name-prefix id))
        worker (assoc worker :thread thread)]
    (callback-dispatch/bind-thread! callback-dispatch thread)
    (.start thread)
    worker))

(defn join-worker!
  "Join a worker thread without sending stop messages. Assumes the worker will
   exit on its own (e.g. after draining)."
  [worker]
  (when-let [^Thread t (:thread worker)]
    (.join t)
    (.set ^AtomicReference (:wakeup-receiver_ worker) nil)))

(defn join-all!
  "Join all workers without signalling stop."
  [workers]
  (doseq [w workers]
    (join-worker! w)))

(defn broadcast!
  "Send a control message to all workers.

   Parameters:
   - workers: a seq of workers
   - msg: message to broadcast"
  [workers msg]
  (mapv (fn [^Worker worker]
          (p/send-required-msg worker msg))
        workers))

(defn broadcast-wake!
  "Wake all workers.

   Parameters:
   - workers: a seq of workers"
  [workers]
  (doseq [^Worker w workers]
    (p/wake w)))