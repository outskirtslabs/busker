(ns ol.busker.generation-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.config :as config]
   [ol.busker.evloop :as evloop]
   [ol.busker.generation :as generation]
   [ol.busker.test-utils :as util])
  (:import
   [java.util.concurrent.atomic AtomicReference]))

(deftest generation-starts-and-stops-without-runtime-bridge-test
  (let [port 18584
        compiled-config
        (config/load!
         {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                               :tls false}}
          :dispatch [{:handler (fn [_]
                                 {:status 200
                                  :body "generation-ok"})}]})
        instance (generation/start! compiled-config nil)]
    (try
      (let [result (util/curl :http nil port "/" :max-time 5)]
        (is (= 0 (:exit result))
            (str "The generation should serve requests directly. stderr: "
                 (:err result)))
        (is (= "generation-ok" (:out result))))
      (finally
        (generation/stop! instance)))))

(deftest failed-worker-retirement-retains-the-callback-arena-test
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          worker {:callback-dispatch dispatch
                  :wakeup-receiver_ (AtomicReference.)}
          phase (atom :running)
          generation-state {::generation/phase phase
                            ::generation/stop-lock (Object.)
                            ::generation/workers [worker]
                            ::generation/wakeup-receivers [nil]
                            ::generation/listener-runtimes []
                            ::generation/listener-claims {}
                            ::generation/config {}
                            ::generation/arena arena}]
      (try
        (with-redefs [evloop/broadcast-wake! (constantly nil)
                      evloop/join-all! (fn [_]
                                         (throw (InterruptedException. "simulated")))]
          (generation/stop! generation-state))
        (is (= :retirement-failed @phase))
        (is (= 1 (.byteSize (mem/alloc 1 arena))))
        (finally
          (swap! @#'generation/failed-retirements_
                 (fn [retirements]
                   (vec (remove #(identical? generation-state %) retirements)))))))))

(deftest successful-retirement-retry-releases-retained-generation-test
  (let [arena (mem/shared-arena)
        dispatch (callback-dispatch/create arena)
        worker {:callback-dispatch dispatch
                :wakeup-receiver_ (AtomicReference.)
                :thread (Thread.)}
        phase (atom :running)
        generation-state {::generation/phase phase
                          ::generation/stop-lock (Object.)
                          ::generation/workers [worker]
                          ::generation/wakeup-receivers [nil]
                          ::generation/listener-runtimes []
                          ::generation/listener-claims {}
                          ::generation/config {}
                          ::generation/arena arena}
        attempts (atom 0)]
    (try
      (with-redefs [evloop/broadcast-wake! (constantly nil)
                    evloop/join-all! (fn [_]
                                       (when (= 1 (swap! attempts inc))
                                         (throw (InterruptedException. "simulated"))))]
        (generation/stop! generation-state)
        (generation/stop! generation-state))
      (is (= :stopped @phase))
      (is (not (some #(identical? generation-state %)
                     @@#'generation/failed-retirements_)))
      (finally
        (swap! @#'generation/failed-retirements_
               (fn [retirements]
                 (vec (remove #(identical? generation-state %) retirements))))))))

(deftest native-callbacks-deliver-and-retire-request-state-test
  (let [port 18585
        instance
        (generation/start!
         (config/load!
          {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                :tls false}}
           :dispatch [{:handler (fn [request]
                                  {:status 200
                                   :body (slurp (:body request))})}]})
         nil)
        dispatches (mapv :callback-dispatch (::generation/workers instance))]
    (try
      (let [result (util/curl :http nil port "/"
                              :max-time 5
                              :args ["--data-binary" "callback-body"])]
        (is (= 0 (:exit result)) (:err result))
        (is (= "callback-body" (:out result)))
        (is (loop [remaining 100]
              (if (pos? (reduce + (map callback-dispatch/retired-count dispatches)))
                true
                (when (pos? remaining)
                  (Thread/sleep 10)
                  (recur (dec remaining)))))))
      (finally
        (generation/stop! instance)))))

(deftest repeated-start-stop-releases-native-callback-state-test
  (doseq [[port request-body] [[18586 "first-cycle"]
                               [18587 "second-cycle"]]]
    (let [instance
          (generation/start!
           (config/load!
            {:entrypoints {:http {:bind (str "127.0.0.1:" port)
                                  :tls false}}
             :dispatch [{:handler (fn [request]
                                    {:status 200
                                     :body (slurp (:body request))})}]})
           nil)]
      (try
        (let [result (util/curl :http nil port "/"
                                :max-time 5
                                :args ["--data-binary" request-body])]
          (is (= 0 (:exit result)) (:err result))
          (is (= request-body (:out result))))
        (finally
          (generation/stop! instance))))))