(ns ol.busker.response-head-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.native :as h2o]
   [ol.busker.response-head :as response-head]))

(defn- invoke-on-virtual-thread
  [f]
  (let [result_ (promise)]
    (Thread/startVirtualThread
     #(try
        (deliver result_ {:value (f)})
        (catch Throwable t
          (deliver result_ {:error t}))))
    @result_))

(defn- decode-headers
  [segment header-count]
  (let [header-size (mem/size-of ::h2o/clj-header-t)]
    (mapv
     (fn [idx]
       (let [header-segment (mem/slice segment (* idx header-size) header-size)
             {:keys [name name_len value value_len]}
             (mem/deserialize header-segment ::h2o/clj-header-t)]
         [(h2o/->string name name_len)
          (h2o/->string value value_len)]))
     (range header-count))))

(deftest response-head-command-construction-does-not-allocate-ffm-memory-test
  (with-redefs [mem/alloc (fn [& _]
                            (throw (ex-info "unexpected allocation" {})))
                mem/serialize (fn [& _]
                                (throw (ex-info "unexpected serialization" {})))]
    (let [{:keys [value error]}
          (invoke-on-virtual-thread
           #(vector
             (response-head/start-command 1 2 200 [["x-a" "one"]] -1 0)
             (response-head/informational-command 1 2 103 [["link" "two"]])))]
      (is (nil? error))
      (is (= [["x-a" "one"]] (:headers (first value))))
      (is (= [["link" "two"]] (:headers (second value)))))))

(deftest response-head-staging-produces-native-layout-on-platform-thread-test
  (with-open [arena (mem/confined-arena)]
    (let [headers [["x-one" "first"] ["x-two" "second"]]
          segment (@#'response-head/headers-segment headers arena)]
      (is (false? (.isVirtual (Thread/currentThread))))
      (is (= headers (decode-headers segment (count headers)))))))

(deftest response-head-execution-rejects-the-wrong-thread-before-native-use-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"outside its event-loop worker"
       (response-head/execute-start!
        {:worker (Object.)}
        (response-head/start-command 1 2 200 [] -1 0)))))
