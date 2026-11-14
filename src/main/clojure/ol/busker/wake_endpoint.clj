(ns ^:no-doc ol.busker.wake-endpoint
  "Sends native event-loop wakes for one worker from the publishing thread.

  Work remains in each worker mailbox or response ring. A wake carries no application
  data. [[wake-now!]] calls the native wakeup on the calling thread, and
  [[quiesce-endpoint!]] prevents later native wake calls before the receiver is retired.

  ## Related Namespaces

  - [[ol.busker.evloop]] decides when a worker needs a native wake.
  - [[ol.busker.native]] invokes the native wakeup function."
  (:require
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.util.concurrent.atomic AtomicInteger AtomicReference]))

(set! *warn-on-reflection* true)

(defrecord WakeEndpoint
           [^AtomicReference receiver_
            ^AtomicReference state_
            ^AtomicInteger in-flight_])

(alter-meta! #'->WakeEndpoint assoc :doc
             "Creates a wake endpoint. `receiver_` holds the native receiver; `state_` moves from `:open` through `:quiescing` to `:closed`; `in-flight_` counts native wake calls in progress.")
(defn endpoint
  "Creates an open endpoint for native `receiver_`.

  `receiver_` is an AtomicReference so lifecycle code can retire the receiver safely."
  [receiver_]
  (->WakeEndpoint receiver_
                  (AtomicReference. :open)
                  (AtomicInteger.)))

(defn- report-wake-failure!
  [first-error second-error]
  (trove/log! {:level :error
               :id    ::native-wake-failed
               :error second-error
               :data  {:first-error first-error}}))

(defn wake-now!
  "Calls the native wakeup for `endpoint` on the calling thread.

  Counts the call in `in-flight_` so [[quiesce-endpoint!]] waits for it. Does nothing
  unless the endpoint is `:open` and has a receiver. Retries a failed native call once
  and logs the second failure instead of throwing."
  [^WakeEndpoint endpoint]
  (let [^AtomicReference receiver_ (.-receiver_ endpoint)
        ^AtomicReference state_ (.-state_ endpoint)
        ^AtomicInteger in-flight_ (.-in-flight_ endpoint)]
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

(defn quiesce-endpoint!
  "Stops future wakes for `endpoint` and waits for active native wake calls.

  Returns `true` after the endpoint reaches `:closed`. Call from the event-loop worker
  before it destroys the receiver."
  [^WakeEndpoint endpoint]
  (let [^AtomicReference state_ (.-state_ endpoint)
        ^AtomicInteger in-flight_ (.-in-flight_ endpoint)]
    (.compareAndSet state_ :open :quiescing)
    (while (pos? (.get in-flight_))
      (Thread/onSpinWait))
    (.set state_ :closed)
    true))
