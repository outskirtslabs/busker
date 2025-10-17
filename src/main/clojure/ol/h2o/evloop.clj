(ns ol.h2o.evloop
  (:require
   [ol.h2o.native :as h2o]
   [ol.h2o.protocols :as p])
  (:import
   [java.util HashMap]
   [java.util.concurrent ArrayBlockingQueue]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

;; ------------------------------
;; Worker control-plane primitives
;; ------------------------------

(defrecord Worker
           [id                     ;; int
            thread                 ;; java.lang.Thread (platform)
            running?               ;; AtomicBoolean
            mailbox                ;; ArrayBlockingQueue of control messages
            evloop                 ;; opaque: native pointer/handle when interop lands
            loop-fn                ;;  the loop iteration body
            message-handler        ;; fn: (op, args) -> void, handles custom messages
            wakeup-receiver
            ^HashMap requests
            args]
  p/WorkerThread
  (wake [_]
    (h2o/mt-wakeup wakeup-receiver))
  (send-msg [this msg]
    (.offer ^ArrayBlockingQueue mailbox msg)
    (p/wake this))
  (count-msgs [_] (.size ^ArrayBlockingQueue mailbox))
  (add-req [_ req]
    (.put requests (:req-id req) req))
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
            (.set ^AtomicBoolean (:running? w) false))
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
    (let [running? ^AtomicBoolean (:running? w)
          loop-fn (:loop-fn w)]
      (loop [s {}]
        (when (.get running?)
          (let [w (drain-mailbox! w)]
            (recur (loop-fn w s))))))
    (catch InterruptedException _
      (.set ^AtomicBoolean (:running? w) false))
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
                        :running? (AtomicBoolean. true)
                        :mailbox (ArrayBlockingQueue. 256)
                        :evloop nil
                        :loop-fn loop-fn
                        :message-handler message-handler
                        :wakeup-receiver wakeup-receiver})
        t (Thread. #(run-evloop-on-thread! (assoc w :thread (Thread/currentThread))) (format "%s-%d" thread-name-prefix id))
        w (assoc w :thread t)]
    (.start t)
    w))

(defn stop-worker!
  "Stop a specific worker by id. Blocks until worker thread terminates.
   
   Parameters:
   - worker: the workder to stop "
  [worker]
  (p/send-msg worker stop-msg)
  (when-let [^Thread t (:thread worker)]
    (.interrupt t)
    (.join t)))

(defn stop-all!
  "Stop all workers in the system.
   
   Parameters:
   - workers: a seq of workers "
  [workers]
  (doseq [w workers]
    (stop-worker! w)))

(defn broadcast!
  "Send a control message to all workers.

   Parameters:
   - workers: a seq of workers
   - msg: message to broadcast "
  [workers msg]
  (doseq [^Worker w workers]
    (p/send-msg w msg)))

(defn broadcast-wake!
  "Wake all workers.
   
   Parameters:
   - workers: a seq of workers "
  [workers]
  (doseq [^Worker w workers]
    (p/wake w)))
