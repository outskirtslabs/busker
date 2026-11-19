(ns ^:no-doc ol.busker.evloop
  "Runs worker event loops and their bounded mailbox control operations.

  [[Worker]] directly implements [[ol.busker.internal.protocols/WorkerThread]]. Key functions
  admit messages, request worker shutdown, drain mailboxes, and bind worker context for
  worker-affine response operations. Each platform worker processes at most 256
  mailbox commands per turn before native polling. A worker with live requests polls
  for new work for up to `hot-poll-ns` before it arms sleep. The 256-command capacity
  includes unpublished permits; reservations do not occupy FIFO positions.
  Application virtual threads can wait for capacity through Java synchronization.
  Stop rejects new reservations and drains admitted work, including outstanding
  publication or discard obligations, before a normal exit.

  ## Related Namespaces

  - [[ol.busker.generation]] creates and stops workers.
  - [[ol.busker.wake-endpoint]] sends native event-loop wakes."
  (:require
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.capacity-waiters :as capacity-waiters]
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
  (let [{:keys [lock] :as m} (:mailbox worker)
        published? (locking lock
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
               :error second-error
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
            zero-wait-turn!
            ^long hot-poll-ns
            message-handler
            ^AtomicReference wakeup-receiver_
            ^AtomicReference response-receiver_
            ^AtomicBoolean response-receiver-open?_
            ^ReentrantReadWriteLock response-receiver-lock
            response-slots
            ^long response-slot-payload-capacity
            fixed-body-limit
            large-head-warnings
            callback-dispatch
            request-dispatch
            ^HashMap requests
            args
            stream-pool
            reserve-command!
            publish-command!
            release-command!
            wake-command-waiters!
            on-native-failure!
            discard-command!
            response-slot-waiters
            ^AtomicReference native-failure_
            ^AtomicReference termination-cause_]
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

(defn- hot-poll!
  "Spins on the worker thread for at most `budget-ns` while no new work arrives.

  Returns `true` once the wake generation differs from `wake-seen` or the mailbox
  count changes, and `false` when the budget runs out. Whenever at least five
  microseconds have passed since the last native turn, runs the worker's
  `zero-wait-turn!` so sockets keep being serviced. The loop uses primitive locals
  only and allocates nothing per iteration."
  [^Worker worker ^long budget-ns ^long wake-seen]
  (let [^AtomicLong wake-generation_ (.-wake-generation_ worker)
        mailbox (.-mailbox worker)
        zero-wait-turn! (.-zero-wait-turn! worker)
        queued (long (mailbox/queued mailbox))
        start (System/nanoTime)]
    (loop [last-turn start]
      (if (or (not (== wake-seen (.get wake-generation_)))
              (not (== queued (long (mailbox/queued mailbox)))))
        true
        (let [now (System/nanoTime)]
          (cond
            (>= (- now start) budget-ns)
            false

            (>= (- now last-turn) 5000)
            (do (zero-wait-turn!)
                (recur (System/nanoTime)))

            :else
            (do (Thread/onSpinWait)
                (recur last-turn))))))))

(defn- run-evloop-on-thread!
  [^Worker worker]
  (.set worker-context/worker-context worker)
  (try
    (let [^AtomicBoolean running?_ (:running?_ worker)
          ^AtomicLong wake-generation_ (:wake-generation_ worker)
          ^AtomicBoolean sleep-armed?_ (:sleep-armed?_ worker)
          ^AtomicBoolean wake-sent?_ (:wake-sent?_ worker)
          ^HashMap requests (.-requests worker)
          hot-poll-ns (.-hot-poll-ns worker)
          loop-fn (:loop-fn worker)]
      (loop [state (:loop-state worker)
             wake-seen (long 0)]
        (when (.get running?_)
          (when-let [cause (.get ^AtomicReference (:termination-cause_ worker))]
            (throw cause))
          (let [mailbox-work? (drain-mailbox! worker)
                wake-requested (.get wake-generation_)]
            (when (.get running?_)
              (cond
                (or mailbox-work? (not= wake-requested wake-seen))
                (recur (loop-fn worker state true) wake-requested)

                (and (pos? hot-poll-ns)
                     (not (.isEmpty requests))
                     (hot-poll! worker hot-poll-ns wake-seen))
                (recur state wake-seen)

                :else
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
                                       (loop-fn worker state false)
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
        (let [{:keys [lock] :as m} (:mailbox worker)]
          (locking lock
            (.set ^AtomicBoolean (:running?_ worker) false)
            (mailbox/close! m)))
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
   - loop-fn: function called for each loop iteration:
     (worker, state, mailbox-work?) -> state
   - message-handler
   - wakeup-receiver

   Options:
   - :loop-state - the state of the first loop iteration
   - :thread-name-prefix - prefix for thread name (default 'h2o-evloop')
   - :callback-dispatch - worker-local native callback dispatcher
   - :request-dispatch - worker-local handler dispatch with
     `:request-threads :reuse-within-connection`, see [[ol.busker.request-dispatch]]
   - :response-receiver - native complete-response receiver
   - :response-slots - generation-scoped native slot views
   - :response-slot-payload-capacity - writable bytes in one slot
   - :fixed-body-limit - largest body in bytes that a slot takes
   - :large-head-warnings - generation-wide AtomicLongArray of the next time a
     large-head warning may go out and the count since the last one, or nil
   - :zero-wait-turn! - no-argument function that runs one native loop turn without
     waiting, called while the worker polls before sleep
   - :hot-poll-ns - longest poll before sleep, in nanoseconds; 0 disables the poll

   Returns: worker"

  [loop-fn message-handler wakeup-receiver
   & {:keys [callback-dispatch request-dispatch response-receiver response-slots
             response-slot-payload-capacity fixed-body-limit large-head-warnings thread-name-prefix
             stream-pool loop-state
             on-native-failure! discard-command! zero-wait-turn! hot-poll-ns]
      :or {thread-name-prefix "h2o-evloop"
           hot-poll-ns 0}}]
  (when-not callback-dispatch
    (throw (ex-info "Worker requires a callback dispatcher" {})))
  (let [id (swap! next-id_ inc)
        receiver_ (AtomicReference. wakeup-receiver)
        response-receiver_ (AtomicReference. response-receiver)
        wake-endpoint (wake-endpoint/endpoint receiver_)
        worker (map->Worker {:id id
                             :thread nil
                             :callback-dispatch callback-dispatch
                             :request-dispatch request-dispatch
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
                             :response-slot-waiters (capacity-waiters/create)
                             :native-failure_ (AtomicReference.)
                             :termination-cause_ (AtomicReference.)
                             :evloop nil
                             :loop-fn loop-fn
                             :loop-state loop-state
                             :zero-wait-turn! zero-wait-turn!
                             :hot-poll-ns (long hot-poll-ns)
                             :message-handler message-handler
                             :wakeup-receiver_ receiver_
                             :response-receiver_ response-receiver_
                             :response-receiver-open?_ (AtomicBoolean. true)
                             :response-receiver-lock (ReentrantReadWriteLock.)
                             :response-slots response-slots
                             :response-slot-payload-capacity response-slot-payload-capacity
                             :fixed-body-limit fixed-body-limit
                             :large-head-warnings large-head-warnings})
        thread (Thread. #(run-evloop-on-thread!
                          (assoc worker :thread (Thread/currentThread)))
                        (format "%s-%d" thread-name-prefix id))
        worker (assoc worker :thread thread)]
    (callback-dispatch/bind-thread! callback-dispatch thread)
    (.start thread)
    worker))

(defn broadcast-wake!
  "Wake all workers.

   Parameters:
   - workers: a seq of workers"
  [workers]
  (doseq [^Worker w workers]
    (p/wake w)))