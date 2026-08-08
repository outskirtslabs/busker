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