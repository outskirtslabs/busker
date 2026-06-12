(ns ol.busker.evloop
  (:require
   [ol.busker.internal.protocols :as p]
   [ol.busker.native :as h2o])
  (:import
   [java.util HashMap]
   [java.util.concurrent ArrayBlockingQueue]
   [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(set! *warn-on-reflection* true)

;; ------------------------------
;; Worker control-plane primitives
;; ------------------------------

(defrecord Worker
           [id                     ;; int
            thread                 ;; java.lang.Thread (platform)
            ^AtomicBoolean running?_;; AtomicBoolean
            mailbox                ;; ArrayBlockingQueue of control messages
            evloop                 ;; opaque: native pointer/handle when interop lands
            loop-fn                ;;  the loop iteration body
            message-handler        ;; fn: (op, args) -> void, handles custom messages
            ^AtomicReference wakeup-receiver_
            ^HashMap requests
            args]
  p/WorkerThread
  (running? [_]
    (.get running?_))
  (wake [_]
    (when (.get running?_)
      (when-let [receiver (.get wakeup-receiver_)]
        (h2o/mt-wakeup receiver))))
  (send-msg [this msg]
    (when (.get running?_)
      (.offer ^ArrayBlockingQueue mailbox msg)
      (p/wake this)))
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

(defn- drain-mailbox!
  "Non-blocking drain. Returns a possibly updated worker state map."
  [^Worker w]
  (loop [stop-requested? false]
    (let [msg (.poll ^ArrayBlockingQueue (:mailbox w))]
      (if (nil? msg)
        (do
          (when stop-requested?
            (.set ^AtomicBoolean (:running?_ w) false))
          w)
        (let [[op & args] msg]
          (case op
            ::stop
            (recur true)
            (do
              (when-let [handler (:message-handler w)]
                (handler op args))
              (recur stop-requested?))))))))

(defn- run-evloop-on-thread!
  "Owns the OS thread and drives the event loop until stopped.
   Calls the worker's loop-fn for each iteration."
  [^Worker w]
  (.set worker-context w)
  (try
    (let [running? ^AtomicBoolean (:running?_ w)
          loop-fn (:loop-fn w)]
      (loop [s {}]
        (when (.get running?)
          (let [w (drain-mailbox! w)]
            (recur (loop-fn w s))))))
    (catch InterruptedException _
      (.set ^AtomicBoolean (:running?_ w) false))
    (catch Throwable t
      (println "[evloop] worker crashed:" (.getMessage t))
      (println t))
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

   Returns: worker"

  [loop-fn message-handler wakeup-receiver & {:keys [thread-name-prefix]
                                              :or {thread-name-prefix "h2o-evloop"}}]
  (let [id (swap! next-id_ inc)
        w (map->Worker {:id id
                        :thread nil
                        :requests (HashMap. 100)
                        :running?_ (AtomicBoolean. true)
                        :mailbox (ArrayBlockingQueue. 256)
                        :evloop nil
                        :loop-fn loop-fn
                        :message-handler message-handler
                        :wakeup-receiver_ (AtomicReference. wakeup-receiver)})
        t (Thread. #(run-evloop-on-thread! (assoc w :thread (Thread/currentThread))) (format "%s-%d" thread-name-prefix id))
        w (assoc w :thread t)]
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
