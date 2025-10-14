(ns ol.h2o.evloop
  (:import
   [java.util.concurrent.atomic AtomicBoolean]
   [java.util.concurrent
    ArrayBlockingQueue ConcurrentHashMap]))

(set! *warn-on-reflection* true)

;; ------------------------------
;; Worker control-plane primitives
;; ------------------------------
(defrecord Worker
           [id ;; int
            thread ;; java.lang.Thread (platform)
            running? ;; AtomicBoolean
            mailbox ;; ArrayBlockingQueue of control messages
            evloop ;; opaque: native pointer/handle when interop lands
            max-wait-ms ;; int, passed to h2o_evloop_run(loop, max_wait)
            loop-fn ;; fn: (worker, max-wait-ms) -> void, the loop iteration body
            message-handler ;; fn: (op, args) -> void, handles custom messages
            args])

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

(defn set-max-wait [millis] [::set-max-wait (int millis)])
(def stop-msg [::stop])

;; ------------------------------
;; Worker loop
;; ------------------------------

(defn- drain-mailbox!
  "Non-blocking drain. Returns a possibly updated worker state map."
  [^Worker w]
  (loop [max-wait (:max-wait-ms w)
         stop-requested? false]
    (let [msg (.poll ^ArrayBlockingQueue (:mailbox w))]
      (if (nil? msg)
        (do
          (when stop-requested?
            (.set ^AtomicBoolean (:running? w) false))
          (assoc w :max-wait-ms max-wait))
        (let [[op & args] msg]
          (case op
            ::set-max-wait
            (recur (max 0 (int (first args))) stop-requested?)
            ::stop
            (recur max-wait true)
            (do
              (when-let [handler (:message-handler w)]
                (handler op args))
              (recur max-wait stop-requested?))))))))

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

(defn create-system
  []
  {:workers (ConcurrentHashMap.)})

(defn start-worker!
  "Create and start one event-loop worker on a platform thread (daemon).
   
   Parameters:
   - system: the system context returned by create-system
   
   Options:
   - :max-wait-ms - maximum time to wait in each loop iteration (default 10ms)
   - :thread-name-prefix - prefix for thread name (default 'h2o-evloop')
   - :loop-fn - function called for each loop iteration: (worker, max-wait-ms) -> void
   - :message-handler - function to handle custom messages: (op, args) -> void

   Returns: worker id"
  ([system loop-fn args]
   (start-worker! system loop-fn args nil))
  ([system loop-fn args
    {:keys [max-wait-ms thread-name-prefix message-handler]
     :or {max-wait-ms 10
          thread-name-prefix "h2o-evloop"}}]
   (let [id (swap! next-id_ inc)
         running? (AtomicBoolean. true)
         mailbox (ArrayBlockingQueue. 256)
         ;; TODO evloop: will be a native handle after FFM init (nil for now)
         evloop nil
         w (->Worker id nil running? mailbox evloop (int max-wait-ms) loop-fn message-handler args)
         t (Thread. #(run-evloop-on-thread! w) (format "%s-%d" thread-name-prefix id))
         w (assoc w :thread t)]
     (.put ^ConcurrentHashMap (:workers system) id w)
     (.start t)
     id)))

(defn stop-worker!
  "Stop a specific worker by id. Blocks until worker thread terminates.
   
   Parameters:
   - system: the system context
   - id: worker id to stop
   
   Returns: true if worker was found and stopped"
  [system id]
  (when-let [^Worker w (.get ^ConcurrentHashMap (:workers system) id)]
    (.offer ^ArrayBlockingQueue (:mailbox w) stop-msg)
    ;; Wait for thread to fully terminate before returning
    (when-let [^Thread t (:thread w)]
      (.interrupt t)
      (.join t))
    (.remove ^ConcurrentHashMap (:workers system) id)
    true))

(defn stop-all!
  "Stop all workers in the system.
   
   Parameters:
   - system: the system context
   
   Returns: true"
  [system]
  (doseq [id (vec (.keySet ^ConcurrentHashMap (:workers system)))]
    (stop-worker! system id))
  true)

(defn set-worker-wait!
  "Adjust the max_wait for a specific worker.
   
   Parameters:
   - system: the system context
   - id: worker id
   - millis: new max wait time in milliseconds
   
   Returns: true if worker was found"
  [system id millis]
  (when-let [^Worker w (.get ^ConcurrentHashMap (:workers system) id)]
    (.offer ^ArrayBlockingQueue (:mailbox w) (set-max-wait millis))
    true))

(defn send-msg!
  "Send a control message to a specific worker.
   
   Parameters:
   - system: the system context
   - id: worker id
   - msg: message to send
   
   Returns: true if worker was found and message was queued"
  ([system msg]
   (send-msg! system (:worker-id system) msg))
  ([system id msg]
   (when-let [^Worker w (.get ^ConcurrentHashMap (:workers system) id)]
     (.offer ^ArrayBlockingQueue (:mailbox w) msg)
     #_(println (first msg)))))

(defn broadcast!
  "Send a control message to all workers (bounded mailboxes).
   
   Parameters:
   - system: the system context
   - msg: message to broadcast
   
   Returns: true"
  [system msg]
  (doseq [^Worker w (.values ^ConcurrentHashMap (:workers system))]
    (.offer ^ArrayBlockingQueue (:mailbox w) msg))
  true)

(defn get-worker-ids
  "Get all worker ids in the system.
   
   Parameters:
   - system: the system context
   
   Returns: vector of worker ids"
  [system]
  (vec (.keySet ^ConcurrentHashMap (:workers system))))

(defn get-worker-count
  "Get the number of active workers.
   
   Parameters:
   - system: the system context
   
   Returns: count of workers"
  [system]
  (.size ^ConcurrentHashMap (:workers system)))
