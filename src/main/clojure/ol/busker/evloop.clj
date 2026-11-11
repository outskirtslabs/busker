(ns ^:no-doc ol.busker.evloop
  "Runs worker event loops and their bounded mailbox control operations.

  [[Worker]] directly implements [[ol.busker.internal.protocols/WorkerThread]]. Key functions
  admit messages, request worker shutdown, drain mailboxes, and bind worker context for
  worker-affine response operations. Each platform worker processes at most 256
  mailbox commands per turn before native polling. The 256-command capacity
  includes unpublished permits; reservations do not occupy FIFO positions.
  Application virtual threads can wait for capacity through Java synchronization.
  Stop rejects new reservations and drains admitted work, including outstanding
  publication or discard obligations, before a normal exit.

  ## Related Namespaces

  - [[ol.busker.generation]] creates and stops workers.
  - [[ol.busker.wake-endpoint]] sends native event-loop wakes."
  (:require
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.internal.protocols :as p]
   [ol.busker.wake-endpoint :as wake-endpoint]
   [ol.busker.worker-context :as worker-context]
   [ol.busker.worker-mailbox :as mailbox]
   [taoensso.trove :as trove])
  (:import
   [java.util HashMap]
   [java.util.concurrent.locks ReentrantReadWriteLock]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong AtomicReference]))

(set! *warn-on-reflection* true)

(def stop-msg [::stop])

(defonce ^:private next-id_ (atom 0))

(declare signal-mailbox!)

(defn reserve-command! [worker wait? cancelled?]
  (mailbox/reserve! (:mailbox worker) wait? cancelled?))

(defn release-command! [worker permit]
  (mailbox/release! (:mailbox worker) permit)
  (try (p/wake worker) (catch Throwable _ nil)))

(defn wake-command-waiters! [worker]
  (mailbox/wake-waiters! (:mailbox worker)))

(defn publish-command! [worker permit command]
  (let [m (:mailbox worker)
        published? (locking (:lock m)
                     (if (.get ^AtomicBoolean (:running?_ worker))
                       (do (mailbox/publish! m permit command) true)
                       (do (mailbox/release! m permit) false)))]
    (when-not published?
      (when-let [discard (:discard-command! worker)] (discard command))))
  (try (signal-mailbox! worker command) (catch Throwable _ nil)))

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
  [worker msg]
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

(defn- submit-message! [worker msg wait?]
  (let [permit (reserve-command! worker wait? (constantly false))]
    (case permit
      :full :overloaded
      :closed :closed
      (do (publish-command! worker permit msg) :accepted))))

(defn- admit-message! [worker msg]
  (submit-message! worker msg false))

(defn- required-message! [worker msg]
  (submit-message! worker msg true))

(defn- request-stop!
  [worker]
  (if (.compareAndSet ^AtomicBoolean (:accepting?_ worker) true false)
    (do
      (mailbox/close! (:mailbox worker))
      (.set ^AtomicBoolean (:stop-requested?_ worker) true)
      (p/wake worker)
      :accepted)
    :closed))

(defrecord Worker
           [id
            thread
            ^AtomicBoolean running?_
            ^AtomicBoolean accepting?_
            ^AtomicBoolean stop-requested?_
            ^AtomicLong mailbox-signal_
            ^AtomicLong wake-generation_
            ^AtomicBoolean sleep-armed?_
            ^AtomicBoolean wake-sent?_
            wake-endpoint
            mailbox
            evloop
            loop-fn
            message-handler
            ^AtomicReference wakeup-receiver_
            ^AtomicReference response-receiver_
            ^AtomicBoolean response-receiver-open?_
            ^ReentrantReadWriteLock response-receiver-lock
            response-slots
            ^long response-slot-payload-capacity
            callback-dispatch
            ^HashMap requests
            args]
  p/WorkerThread
  (running? [_]
    (and (.get running?_) (.get accepting?_)))
  (wake [_]
    (when (.get running?_)
      (.incrementAndGet wake-generation_)
      (when (and (.get sleep-armed?_)
                 (.compareAndSet wake-sent?_ false true))
        (wake-endpoint/wake-now! wake-endpoint))))
  (send-msg [this msg]
    (if (= stop-msg msg)
      (request-stop! this)
      (admit-message! this msg)))
  (send-required-msg [this msg]
    (if (= stop-msg msg)
      (request-stop! this)
      (required-message! this msg)))
  (count-msgs [_] (mailbox/queued mailbox))
  (add-req [_ req-id request]
    (.put requests req-id request))
  (reap-req [_ req-id]
    (.remove requests req-id)))

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
  (loop [handled 0]
    (if (= handled 256)
      (do (mark-mailbox-signalled! worker) true)
      (if-let [message (mailbox/poll! (:mailbox worker))]
        (do (handle-message! worker message) (recur (inc handled)))
        (do
          (clear-mailbox-signal! worker)
          (if-let [message (mailbox/poll! (:mailbox worker))]
            (do
              (mark-mailbox-signalled! worker)
              (handle-message! worker message)
              (recur (inc handled)))
            (do
              (when (and (.get ^AtomicBoolean (:stop-requested?_ worker))
                         (not (mailbox/pending? (:mailbox worker))))
                (.set ^AtomicBoolean (:running?_ worker) false))
              (pos? handled))))))))

(defn- mailbox-empty?
  [^Worker worker]
  (zero? (mailbox/queued (:mailbox worker))))

(defn- run-evloop-on-thread!
  [^Worker worker]
  (.set worker-context/worker-context worker)
  (try
    (let [^AtomicBoolean running?_ (:running?_ worker)
          ^AtomicLong wake-generation_ (:wake-generation_ worker)
          ^AtomicBoolean sleep-armed?_ (:sleep-armed?_ worker)
          ^AtomicBoolean wake-sent?_ (:wake-sent?_ worker)
          loop-fn (:loop-fn worker)]
      (loop [state {}
             wake-seen (long 0)]
        (when (.get running?_)
          (when-let [cause (.get ^AtomicReference (:termination-cause_ worker))]
            (throw cause))
          (let [mailbox-work? (drain-mailbox! worker)
                wake-requested (.get wake-generation_)]
            (when (.get running?_)
              (if (or mailbox-work? (not= wake-requested wake-seen))
                (recur (loop-fn worker (assoc state ::mailbox-work? true))
                       wake-requested)
                (do
                  ;; Reset wake-sent?_ before arming sleep. A publisher's compare-and-set
                  ;; fails only when an earlier publisher won it after this reset, and that
                  ;; publisher sent the native wake after the reset. That wake is either
                  ;; consumed by a turn before the worker sleeps, which comes back here and
                  ;; resets again, or it ends the sleep.
                  (.set wake-sent?_ false)
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
    (catch InterruptedException error
      (when-let [fail (:on-native-failure! worker)] (fail worker error)))
    (catch Throwable error
      (when-let [fail (:on-native-failure! worker)] (fail worker error)))
    (finally
      (try
        (.set ^AtomicBoolean (:accepting?_ worker) false)
        (locking (:lock (:mailbox worker))
          (.set ^AtomicBoolean (:running?_ worker) false)
          (mailbox/close! (:mailbox worker)))
        (when-let [discard (:discard-command! worker)]
          (loop []
            (when-let [command (mailbox/poll! (:mailbox worker))]
              (discard command)
              (recur))))
        (finally
          (.remove worker-context/worker-context))))))

(defn start-worker!
  "Start a worker on a platform thread (daemon).

   Parameters:
   - loop-fn: function called for each loop iteration: (worker, state) -> state
   - message-handler
   - wakeup-receiver

   Options:
   - :thread-name-prefix - prefix for thread name (default 'h2o-evloop')
   - :callback-dispatch - worker-local native callback dispatcher
   - :response-receiver - native complete-response receiver
   - :response-slots - generation-scoped native slot views
   - :response-slot-payload-capacity - writable bytes in one slot

   Returns: worker"

  [loop-fn message-handler wakeup-receiver
   & {:keys [callback-dispatch response-receiver response-slots
             response-slot-payload-capacity thread-name-prefix stream-pool
             on-native-failure! discard-command!]
      :or {thread-name-prefix "h2o-evloop"}}]
  (when-not callback-dispatch
    (throw (ex-info "Worker requires a callback dispatcher" {})))
  (let [id (swap! next-id_ inc)
        receiver_ (AtomicReference. wakeup-receiver)
        response-receiver_ (AtomicReference. response-receiver)
        wake-endpoint (wake-endpoint/endpoint receiver_)
        worker (map->Worker {:id id
                             :thread nil
                             :callback-dispatch callback-dispatch
                             :requests (:entries callback-dispatch)
                             :running?_ (AtomicBoolean. true)
                             :accepting?_ (AtomicBoolean. true)
                             :stop-requested?_ (AtomicBoolean. false)
                             :mailbox-signal_ (AtomicLong.)
                             :wake-generation_ (AtomicLong.)
                             :sleep-armed?_ (AtomicBoolean. false)
                             :wake-sent?_ (AtomicBoolean. false)
                             :wake-endpoint wake-endpoint
                             :mailbox (mailbox/create)
                             :stream-pool stream-pool
                             :reserve-command! reserve-command!
                             :publish-command! publish-command!
                             :release-command! release-command!
                             :wake-command-waiters! wake-command-waiters!
                             :on-native-failure! on-native-failure!
                             :discard-command! discard-command!
                             :response-slot-monitor (Object.)
                             :native-failure_ (AtomicReference.)
                             :termination-cause_ (AtomicReference.)
                             :evloop nil
                             :loop-fn loop-fn
                             :message-handler message-handler
                             :wakeup-receiver_ receiver_
                             :response-receiver_ response-receiver_
                             :response-receiver-open?_ (AtomicBoolean. true)
                             :response-receiver-lock (ReentrantReadWriteLock.)
                             :response-slots response-slots
                             :response-slot-payload-capacity response-slot-payload-capacity})
        thread (Thread. #(run-evloop-on-thread!
                          (assoc worker :thread (Thread/currentThread)))
                        (format "%s-%d" thread-name-prefix id))
        worker (assoc worker :thread thread)]
    (callback-dispatch/bind-thread! callback-dispatch thread)
    (.start thread)
    worker))

(defn join-worker!
  "Join a worker thread without sending stop messages. Assumes the worker will
   exit after draining."
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