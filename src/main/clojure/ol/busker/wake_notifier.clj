(ns ^:no-doc ol.busker.wake-notifier
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

(defrecord WakeNotifier
           [^LinkedBlockingQueue queue
            ^AtomicBoolean accepting?_
            ^AtomicInteger admissions_
            ^AtomicReference thread_])

(defn endpoint
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
  [^WakeEndpoint endpoint]
  (let [^AtomicReference state_ (.-state_ endpoint)
        ^AtomicReference receiver_ (.-receiver_ endpoint)
        ^AtomicInteger in-flight_ (.-in-flight_ endpoint)]
    (.compareAndSet state_ :open :quiescing)
    (while (pos? (.get in-flight_))
      (Thread/onSpinWait))
    (.set receiver_ nil)
    (.set state_ :closed)
    true))

(defn stop-and-join!
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
