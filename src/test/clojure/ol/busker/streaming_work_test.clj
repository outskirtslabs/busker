(ns ol.busker.streaming-work-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.callback-dispatch :as dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.response-queue :as queue])
  (:import
   [java.util HashMap]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger]))

(defn- fixture []
  (let [dispatch {:streaming-work (ConcurrentHashMap.)
                  :entries (HashMap.)}
        writer (queue/new-response-state
                {:worker {:callback-dispatch dispatch}
                 :config {:buffer-pool ::unused :output-buffer-size 64}})]
    [dispatch writer]))

(deftest streaming-work-transitions
  (let [[d st] (fixture)]
    (with-redefs [pi/send-required-msg (fn [_ _] :accepted)
                  pi/wake (fn [_] nil)
                  queue/drain-chunks (fn [& _] nil)]
      (is (false? (dispatch/pending-response-work? d)))
      (is (= :sent-msg (queue/schedule-drain! st)))
      (is (dispatch/pending-response-work? d))
      (is (= :signalled (queue/schedule-drain! st)))
      (queue/send-vecs st)
      (is (false? (dispatch/pending-response-work? d)))
      (queue/schedule-drain! st)
      (queue/on-stop st nil)
      (is (false? (dispatch/pending-response-work? d)))
      (queue/schedule-drain! st)
      (is (false? (dispatch/pending-response-work? d))))))

(deftest streaming-work-rejection
  (let [[d st] (fixture)]
    (with-redefs [pi/send-required-msg (fn [_ _] :closed)]
      (is (thrown? java.io.IOException (queue/schedule-drain! st)))
      (is (false? (dispatch/pending-response-work? d))))))

(deftest streaming-work-in-flight
  (let [[d st] (fixture)]
    (with-redefs [pi/send-required-msg (fn [_ _] :accepted)
                  queue/drain-chunks (fn [& _] [true [] []])]
      (queue/schedule-drain! st)
      (queue/send-vecs st)
      (is (.get ^AtomicBoolean (:stopped?_ st)))
      (is (dispatch/pending-response-work? d))
      (queue/on-proceed st)
      (is (false? (dispatch/pending-response-work? d))))))

(deftest streaming-work-retirement-race
  (let [[d st] (fixture)
        pending? queue/pending-work?]
    (with-redefs [pi/send-required-msg (fn [_ _] :accepted)]
      (queue/schedule-drain! st)
      ;; Model activation after the idle CAS, before retirement removes its entry.
      (.set ^AtomicInteger (:drain-state_ st) 0)
      (queue/schedule-drain! st)
      (#'queue/retire-work! st)
      (is (dispatch/pending-response-work? d))
      ;; Model activation after removal, before the retirement recheck.
      (.set ^AtomicInteger (:drain-state_ st) 0)
      (with-redefs [queue/pending-work? (fn [writer]
                                        (queue/schedule-drain! writer)
                                        (pending? writer))]
        (#'queue/retire-work! st))
      (is (dispatch/pending-response-work? d))
      (queue/on-stop st nil)
      (#'queue/track-work! st)
      (is (false? (dispatch/pending-response-work? d))))))

(deftest fixed-final-entries-are-not-scanned
  (let [[d _] (fixture)]
    (testing "request entries do not participate in the streaming work query"
      (.put ^HashMap (:entries d) 1 [nil (delay (throw (AssertionError.)))])
      (with-redefs [queue/pending-work? (fn [_] (throw (AssertionError.)))]
        (is (false? (dispatch/pending-response-work? d)))))))
