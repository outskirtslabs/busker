(ns ^:no-doc ol.busker.wake-notifier
  "Delivers coalesced native event-loop wake notifications from a platform thread.

  Work remains in each worker mailbox or response ring. This namespace sends only wake
  signals, so it does not transfer application data. [[request!]] coalesces requests,
  [[quiesce-endpoint!]] prevents later native wake calls, and [[stop-and-join!]] ends
  notifier delivery.

  ## Related Namespaces

  - [[ol.busker.evloop]] requests worker wakes.
  - [[ol.busker.native]] invokes the native wakeup function."
  (:require
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent LinkedBlockingQueue TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]))

(set! *warn-on-reflection* true)

(defrecord WakeEndpoint
           [^AtomicBoolean pending?_
            ^AtomicReference receiver_
            ^AtomicReference state_
            ^AtomicInteger in-flight_])

(alter-meta! #'->WakeEndpoint assoc :doc
             "Creates a wake endpoint. `pending?_` coalesces queued notifications; `receiver_` holds the native receiver; `state_` moves from `:open` to `:closed`; `in-flight_` counts native wake calls in progress.")
(defrecord WakeNotifier
           [^LinkedBlockingQueue queue
            ^AtomicBoolean accepting?_
            ^AtomicInteger admissions_
            ^AtomicReference thread_])

(alter-meta! #'->WakeNotifier assoc :doc
             "Creates a wake notifier. `queue` contains pending endpoints; `accepting?_` controls new requests; `admissions_` counts requests in progress; `thread_` refers to the notifier platform thread.")
(defn endpoint
  "Creates an open endpoint for native `receiver_`. The notifier may later wake it.

  `receiver_` is an AtomicReference so lifecycle code can retire the receiver safely."
  [receiver_]
  (->WakeEndpoint (AtomicBoolean. false)
                  receiver_
                  (AtomicReference. :open)
                  (AtomicInteger.)))

(defn- report-wake-failure!
  [first-error second-error]
  (trove/log! {:level :error
               :id    ::native-wake-failed
               :ex    second-error
               :data  {:first-error first-error}}))

(defn- wake-endpoint!
  [^WakeEndpoint endpoint]
  (let [^AtomicBoolean pending?_ (.-pending?_ endpoint)
        ^AtomicReference receiver_ (.-receiver_ endpoint)
        ^AtomicReference state_ (.-state_ endpoint)
        ^AtomicInteger in-flight_ (.-in-flight_ endpoint)]
    (.set pending?_ false)
    (.incrementAndGet in-flight_)
    (try
      (when (= :open (.get state_))
        (when-let [receiver (.get receiver_)]
          (try
            (h2o/mt-wakeup receiver)
            (catch Throwable first-error
              (try
                (h2o/mt-wakeup receiver)
                (catch Throwable second-error
                  (report-wake-failure! first-error second-error)))))))
      (finally
        (.decrementAndGet in-flight_)))))

(defn- run-notifier!
  [^WakeNotifier notifier]
  (let [^LinkedBlockingQueue queue (.-queue notifier)
        ^AtomicBoolean accepting?_ (.-accepting?_ notifier)]
    (loop []
      (when (or (.get accepting?_) (not (.isEmpty queue)))
        (try
          (when-let [endpoint (.poll queue 100 TimeUnit/MILLISECONDS)]
            (wake-endpoint! endpoint))
          (catch InterruptedException _))
        (recur)))))

(defn start!
  "Starts a daemon platform thread that delivers coalesced endpoint wake notifications.

  Returns a [[WakeNotifier]]. Call [[stop-and-join!]] during shutdown."
  []
  (let [notifier (->WakeNotifier (LinkedBlockingQueue.)
                                 (AtomicBoolean. true)
                                 (AtomicInteger.)
                                 (AtomicReference.))
        ^AtomicReference thread_ (.-thread_ ^WakeNotifier notifier)
        thread (doto (Thread. #(run-notifier! notifier) "busker-wake-notifier")
                 (.setDaemon true))]
    (.set thread_ thread)
    (.start thread)
    notifier))

(defn request!
  "Requests a coalesced wake for `endpoint`.

  Returns `true` when the endpoint is already queued or is queued now, and `false`
  after notifier or endpoint shutdown. It does not carry worker work."
  [^WakeNotifier notifier ^WakeEndpoint endpoint]
  (let [^AtomicBoolean accepting?_ (.-accepting?_ notifier)
        ^AtomicInteger admissions_ (.-admissions_ notifier)
        ^AtomicReference state_ (.-state_ endpoint)
        ^LinkedBlockingQueue queue (.-queue notifier)]
    (if (and (.get accepting?_) (= :open (.get state_)))
      (do
        (.incrementAndGet admissions_)
        (try
          (if-not (and (.get accepting?_) (= :open (.get state_)))
            false
            (let [^AtomicBoolean pending?_ (.-pending?_ endpoint)]
              (if-not (.compareAndSet pending?_ false true)
                true
                (if (.offer queue endpoint)
                  true
                  (do
                    (.set pending?_ false)
                    (throw (IllegalStateException. "Wake notifier queue rejected an endpoint")))))))
          (finally
            (.decrementAndGet admissions_))))
      false)))

(defn quiesce-endpoint!
  "Stops future wakes for `endpoint` and waits for active native wake calls.

  Returns `true` after the endpoint reaches `:closed`. Call from lifecycle code, not
  from the notifier callback itself."
  [^WakeEndpoint endpoint]
  (let [^AtomicReference state_ (.-state_ endpoint)
        ^AtomicInteger in-flight_ (.-in-flight_ endpoint)]
    (.compareAndSet state_ :open :quiescing)
    (while (pos? (.get in-flight_))
      (Thread/onSpinWait))
    (.set state_ :closed)
    true))

(defn stop-and-join!
  "Stops notifier admission, interrupts its platform thread, and waits up to ten seconds.

  Returns `true` when the thread has stopped. Pending queue entries are processed before
  the notifier loop exits unless interruption ends its wait."
  [^WakeNotifier notifier]
  (let [^AtomicBoolean accepting?_ (.-accepting?_ notifier)
        ^AtomicInteger admissions_ (.-admissions_ notifier)
        ^AtomicReference thread_ (.-thread_ notifier)
        ^Thread thread (.get thread_)]
    (.set accepting?_ false)
    (while (pos? (.get admissions_))
      (Thread/onSpinWait))
    (.interrupt thread)
    (.join thread 10000)
    (not (.isAlive thread))))
