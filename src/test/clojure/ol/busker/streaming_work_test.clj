(ns ol.busker.streaming-work-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.buffer-pool :as pool]
   [ol.busker.callback-dispatch :as dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.response-queue :as queue])
  (:import
   [java.util HashMap]
   [java.util.concurrent ConcurrentHashMap]))

(defn- with-controller [f]
  (let [p (pool/create-stream-pool 64)
        d {:streaming-work (ConcurrentHashMap.) :entries (HashMap.)}
        commands (atom [])
        st (queue/create-response-writer
            {:worker {:callback-dispatch d}
             :config {:buffer-pool p :output-buffer-size 64}
             :reserve-command! (fn [_ cancelled?] (if (cancelled?) :closed (Object.)))
             :publish-command! (fn [_ command] (swap! commands conj command) :accepted)
             :release-command! (fn [_] nil)
             :apply-head! (fn [_ _] nil)})]
    (try
      (with-redefs [pi/wake (fn [_] nil)] (f d st commands))
      (finally (queue/on-cleanup st) (pool/retire! p)))))

(deftest empty-markers-become-runnable-only-after-flush
  (with-controller
    (fn [d st commands]
      (let [result (queue/emit-bounded! st nil 0 nil {})]
        (queue/consume-record! st (peek (first @commands)))
        (is (false? (dispatch/pending-response-work? d)))
        (is (identical? (:progress result) (queue/flush! st)))
        (is (dispatch/pending-response-work? d))
        (queue/send-vecs st)
        (is (= {:status :proceeded} (deref (:progress result) 1000 :timeout)))
        (is (false? (dispatch/pending-response-work? d)))))))

(deftest flush-before-command-consumption-does-not-strand-output
  (with-controller
    (fn [d st commands]
      (let [result (queue/emit-bounded! st nil 0 nil {})]
        (queue/flush! st)
        (is (false? (dispatch/pending-response-work? d)))
        (queue/consume-record! st (peek (first @commands)))
        (is (dispatch/pending-response-work? d))
        (queue/send-vecs st)
        (is (= {:status :proceeded} (deref (:progress result) 1000 :timeout)))
        (let [next-result (queue/emit-bounded! st nil 0 nil {:flush? true})]
          (queue/consume-record! st (peek (second @commands)))
          (is (dispatch/pending-response-work? d))
          (queue/send-vecs st)
          (is (= {:status :proceeded} (deref (:progress next-result) 1000 :timeout))))))))

(deftest native-stop-removes-runnable-controls
  (with-controller
    (fn [d st _]
      (queue/close! st)
      (is (dispatch/pending-response-work? d))
      (queue/on-stop st nil)
      (is (false? (dispatch/pending-response-work? d)))
      (queue/flush! st)
      (is (false? (dispatch/pending-response-work? d))))))

(deftest fixed-final-entries-are-not-scanned
  (with-controller
    (fn [d _ _]
      (.put ^HashMap (:entries d) 1 [nil (delay (throw (AssertionError.)))])
      (with-redefs [queue/pending-work? (fn [_] (throw (AssertionError.)))]
        (is (false? (dispatch/pending-response-work? d)))))))
