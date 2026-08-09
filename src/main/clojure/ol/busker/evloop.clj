(ns ^:no-doc ol.busker.evloop
  (:require
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.util HashMap]
   [java.util.concurrent ArrayBlockingQueue]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

(declare admit-message! request-stop! stop-msg)

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
            ^AtomicLong mailbox-signal_
            mailbox
            evloop
            loop-fn
            message-handler
            ^AtomicReference wakeup-receiver_
            callback-dispatch
            ^HashMap requests
            args]
  p/WorkerThread
  (running? [_]
    (and (.get running?_) (.get accepting?_)))
  (wake [_]
    (when (.get running?_)
      (when-let [receiver (.get wakeup-receiver_)]
        (h2o/mt-wakeup receiver))))
  (send-msg [this msg]
    (if (= stop-msg msg)
      (request-stop! this)
      (admit-message! this msg)))
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
        (when accepted?
          (signal-mailbox! worker msg))
        accepted?))
    false))

(defn- request-stop!
  [^Worker worker]
  (if (.compareAndSet ^AtomicBoolean (:accepting?_ worker) true false)
    (do
      (while (pos? (.get ^AtomicInteger (:admissions_ worker)))
        (Thread/onSpinWait))
      (.set ^AtomicBoolean (:stop-requested?_ worker) true)
      (p/wake worker)
      true)
    false))

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
  (loop []
    (if-let [message (.poll ^ArrayBlockingQueue (:mailbox worker))]
      (do
        (handle-message! worker message)
        (recur))
      (do
        (clear-mailbox-signal! worker)
        (if-let [message (.poll ^ArrayBlockingQueue (:mailbox worker))]
          (do
            (mark-mailbox-signalled! worker)
            (handle-message! worker message)
            (recur))
          (when (.get ^AtomicBoolean (:stop-requested?_ worker))
            (.set ^AtomicBoolean (:running?_ worker) false)))))))

(defn- run-evloop-on-thread!
  [^Worker worker]
  (.set worker-context worker)
  (try
    (let [running?_ ^AtomicBoolean (:running?_ worker)
          loop-fn (:loop-fn worker)]
      (loop [state {}]
        (when (.get running?_)
          (drain-mailbox! worker)
          (when (.get running?_)
            (recur (loop-fn worker state))))))
    (catch InterruptedException _
      (.set ^AtomicBoolean (:running?_ worker) false))
    (catch Throwable error
      (println "[evloop] worker crashed:" (.getMessage error))
      (println error))
    (finally
      (.remove worker-context))))

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

   Returns: worker"

  [loop-fn message-handler wakeup-receiver & {:keys [callback-dispatch thread-name-prefix]
                                              :or {thread-name-prefix "h2o-evloop"}}]
  (when-not callback-dispatch
    (throw (ex-info "Worker requires a callback dispatcher" {})))
  (let [id (swap! next-id_ inc)
        w (map->Worker {:id id
                        :thread nil
                        :callback-dispatch callback-dispatch
                        :requests (:entries callback-dispatch)
                        :running?_ (AtomicBoolean. true)
                        :accepting?_ (AtomicBoolean. true)
                        :stop-requested?_ (AtomicBoolean. false)
                        :admissions_ (AtomicInteger.)
                        :mailbox-signal_ (AtomicLong.)
                        :mailbox (ArrayBlockingQueue. 256)
                        :evloop nil
                        :loop-fn loop-fn
                        :message-handler message-handler
                        :wakeup-receiver_ (AtomicReference. wakeup-receiver)})
        t (Thread. #(run-evloop-on-thread! (assoc w :thread (Thread/currentThread)))
                   (format "%s-%d" thread-name-prefix id))
        w (assoc w :thread t)]
    (callback-dispatch/bind-thread! callback-dispatch t)
    (.start t)
    w))

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
  (doseq [^Worker w workers]
    (p/send-msg w msg)))

(defn broadcast-wake!
  "Wake all workers.

   Parameters:
   - workers: a seq of workers"
  [workers]
  (doseq [^Worker w workers]
    (p/wake w)))