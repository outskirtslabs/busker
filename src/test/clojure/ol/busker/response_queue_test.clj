(ns ol.busker.response-queue-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.buffer-pool :as pool]
   [ol.busker.byte-bounded-queue :as budget]
   [ol.busker.callback-dispatch :as dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as native]
   [ol.busker.protocols :as protocols]
   [ol.busker.response :as response]
   [ol.busker.response-queue :as queue]
   [ol.busker.worker-mailbox :as mailbox])
  (:import
   [java.io OutputStream]
   [java.lang.foreign MemorySegment ValueLayout]
   [java.nio ByteBuffer]
   [java.util.concurrent ConcurrentHashMap]))

(defn- controller
  ([p commands] (controller p commands nil))
  ([p commands work]
   (queue/create-response-writer
    {:config {:output-buffer-size (:block-size p)}
     :worker {:stream-pool p
              :callback-dispatch {:streaming-work work}
              :reserve-command! (fn [_ _ cancelled?] (if (cancelled?) :closed (Object.)))
              :publish-command! (fn [_ _ command] (swap! commands conj command) :accepted)
              :release-command! (fn [_ _] nil)}
     :apply-head! (fn [_ _] nil)})))

(defn- consume! [st commands]
  (let [pending @commands]
    (reset! commands [])
    (doseq [command pending] (queue/consume-record! st (peek command)))))

(deftest close-does-not-wait-for-private-producer
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)
          entered (promise)
          resume (promise)
          result (promise)
          producer (Thread/startVirtualThread
                    (fn []
                      (deliver result
                               (queue/emit-bounded!
                                st nil 8
                                (fn [^ByteBuffer b]
                                  (.put b (byte 7))
                                  (deliver entered true)
                                  @resume
                                  (.put b (byte 8)))
                                {}))))]
      (try
        (is (= true (deref entered 1000 :timeout)))
        (is (true? (queue/close! st)))
        (is (= 1 (pool/checked-out p)))
        (is (false? (pool/retire! p)))
        (deliver resume true)
        (is (= {:status :closed} (deref result 1000 :timeout)))
        (.join producer 1000)
        (is (= [] @commands))
        (is (= [0 0] [(pool/checked-out p) (budget/queued-bytes (:budget st))]))
        (finally
          (deliver resume true)
          (.join producer 1000)
          (queue/on-cleanup st)
          (pool/retire! p))))))

(deftest empty-writes-use-receipt-credit-and-close-at-capacity
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)]
      (try
        (let [writes (mapv (fn [_] (queue/emit-bounded! st nil 0 nil {:wait? false})) (range 64))]
          (is (every? #(= :accepted (:status %)) writes))
          (is (= {:status :full} (queue/emit-bounded! st nil 0 nil {:wait? false})))
          (is (identical? (:progress (peek writes)) (queue/flush! st)))
          (is (true? (queue/close! st)))
          (is (false? (queue/close! st)))
          (is (every? #(= {:status :closed} @(:progress %)) writes))
          (is (= 0 (pool/checked-out p))))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest producer-exception-does-not-commit-or-retain-reservations
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)]
      (try
        (is (thrown? IllegalArgumentException
                     (queue/emit-bounded! st {:status 201} 8
                                          (fn [_] (throw (IllegalArgumentException. "producer"))) {})))
        (is (nil? (queue/committed-head st)))
        (is (= [] @commands))
        (is (= [0 0 0] [(pool/checked-out p) (budget/queued-bytes (:budget st))
                        (:reserved-receipts @(:state_ st))]))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest final-storage-survives-stop-until-cleanup
  (with-redefs [pi/wake (fn [_] nil)
                native/sendvec (fn [& _] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)]
      (try
        (let [result (queue/emit-bounded! st nil 8
                                          (fn [^ByteBuffer b] (.put b (byte 1))) {:end? true})]
          (is (= {:status :closed} @(:progress result)))
          (consume! st commands)
          (queue/send-vecs st)
          (is (= [1 0] [(pool/checked-out p) (budget/queued-bytes (:budget st))]))
          (queue/on-proceed st)
          (queue/on-stop st nil)
          (is (= 1 (pool/checked-out p)))
          (queue/on-cleanup st)
          (queue/on-cleanup st)
          (is (= 0 (pool/checked-out p))))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest synchronous-proceed-sees-installed-storage-and-settles-the-batch
  (let [current (atom nil)
        installed (atom false)]
    (with-redefs [pi/wake (fn [_] nil)
                  native/sendvec (fn [& _]
                                   (reset! installed (some? (:in-flight @(:state_ @current))))
                                   (queue/on-proceed @current))]
      (let [p (pool/create-stream-pool 8)
            commands (atom [])
            st (controller p commands)]
        (reset! current st)
        (try
          (let [a (queue/emit-bounded! st nil 1 (fn [^ByteBuffer b] (.put b (byte 1))) {})
                b (queue/emit-bounded! st nil 0 nil {:flush? true})]
            (consume! st commands)
            (queue/send-vecs st)
            (is (true? @installed))
            (is (= [{:status :proceeded} {:status :proceeded}]
                   [@(:progress a) @(:progress b)]))
            (is (= [nil 0] [(:in-flight @(:state_ st)) (pool/checked-out p)])))
          (finally (queue/on-cleanup st) (pool/retire! p)))))))

(deftest write-without-wait-is-rejected-when-unflushed-responses-hold-every-block
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          streams (mapv (fn [_] (controller p (atom []))) (range 257))]
      (try
        (doseq [st (take 256 streams)]
          (queue/emit-bounded! st nil 1 (fn [^ByteBuffer b] (.put b (byte 1))) {}))
        (is (= {:status :full}
               (queue/emit-bounded! (peek streams) nil 1
                                    (fn [_] (throw (AssertionError. "Producer must not run")))
                                    {:wait? false})))
        (is (= 256 (pool/checked-out p)))
        (finally
          (doseq [st streams] (queue/on-cleanup st))
          (pool/retire! p))))))

(defn- write-unflushed! [st commands]
  (queue/emit-bounded! st nil 1 (fn [^ByteBuffer b] (.put b (byte 1))) {:wait? false})
  (consume! st commands))

(defn- run-event-loop! [work]
  (dispatch/drain-responses! {:streaming-work work})
  (run! queue/on-proceed (vec (.values ^ConcurrentHashMap work))))

(defn- waiting-write-status [p work]
  (let [commands (atom [])
        waiting (controller p commands work)
        result (promise)
        producer (Thread/startVirtualThread
                  #(deliver result
                            (:status (queue/emit-bounded!
                                      waiting nil 1
                                      (fn [^ByteBuffer b] (.put b (byte 2))) {}))))
        deadline (+ (System/nanoTime) 10000000000)]
    (try
      (loop []
        (run-event-loop! work)
        (when (and (not (realized? result)) (neg? (- (System/nanoTime) deadline)))
          (Thread/sleep 1)
          (recur)))
      (consume! waiting commands)
      (queue/flush! waiting)
      (dotimes [_ 2] (run-event-loop! work))
      (deref result 100 :timeout)
      (finally
        (queue/on-cleanup waiting)
        (.join producer 1000)))))

(deftest waiting-write-gets-a-block-that-idle-unflushed-responses-hold
  (with-redefs [pi/wake (fn [_] nil)
                native/sendvec (fn [& _] nil)]
    (let [p (pool/create-stream-pool 8)
          work (ConcurrentHashMap.)
          commands (vec (repeatedly 256 #(atom [])))
          idle (mapv #(controller p % work) commands)]
      (try
        (dotimes [_ 2]
          (doseq [[st c] (map vector idle commands)] (write-unflushed! st c))
          (is (= 256 (pool/checked-out p)))
          (is (= :accepted (waiting-write-status p work))))
        (finally
          (doseq [st idle] (queue/on-cleanup st))
          (pool/retire! p))))))

(deftest mailbox-full-releases-preparation-without-calling-producer
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)
          st (assoc-in st [:req :worker :reserve-command!] (fn [_ _ _] :full))]
      (try
        (is (= {:status :full}
               (queue/emit-bounded! st {:status 201} 8
                                    (fn [_] (throw (AssertionError. "Producer must not run")))
                                    {:wait? false})))
        (is (= [nil 0 0] [(queue/committed-head st) (pool/checked-out p)
                          (budget/queued-bytes (:budget st))]))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(defn- mailbox-controller [p m]
  (queue/create-response-writer
   {:config {:output-buffer-size (:block-size p)}
    :worker {:stream-pool p
             :reserve-command! (fn [_ wait? cancelled?] (mailbox/reserve! m wait? cancelled?))
             :publish-command! (fn [_ permit command] (mailbox/publish! m permit command))
             :release-command! (fn [_ permit] (mailbox/release! m permit))
             :wake-command-waiters! (fn [_] (mailbox/wake-waiters! m))}
    :apply-head! (fn [_ _] nil)}))

(deftest streaming-uses-worker-services-with-original-worker
  (doseq [failure-site [:head :send]]
    (with-redefs [pi/wake (fn [_] nil)]
      (let [p (pool/create-stream-pool 8)
            m (mailbox/create)
            calls (atom [])
            failure (ex-info "Native failure" {})
            worker {:stream-pool p
                    :reserve-command! (fn [w wait? cancelled?]
                                        (swap! calls conj [:reserve w wait? (cancelled?)])
                                        (mailbox/reserve! m wait? cancelled?))
                    :publish-command! (fn [w permit command]
                                        (swap! calls conj [:publish w (take 3 command)])
                                        (mailbox/publish! m permit command))
                    :release-command! (fn [w permit]
                                        (swap! calls conj [:release w])
                                        (mailbox/release! m permit))
                    :wake-command-waiters! (fn [w]
                                             (swap! calls conj [:wake w])
                                             (mailbox/wake-waiters! m))
                    :on-native-failure! (fn [w t] (swap! calls conj [:failure w t]))}
            req {:worker worker :config {:output-buffer-size 8}
                 :dispatch-module-id 7 :dispatch-request-seq 11
                 :apply-head! (fn [_ _] (when (= :head failure-site) (throw failure)))}
            st (queue/create-response-writer req)]
        (try
          (is (identical? req (:req st)))
          (is (identical? p (:buffer-pool st)))
          (is (thrown? IllegalArgumentException
                       (queue/emit-bounded! st nil 1
                                            (fn [_] (throw (IllegalArgumentException.))) {})))
          (let [result (queue/emit-bounded! st nil 0 nil {:wait? false :flush? true})
                command (mailbox/poll! m)]
            (queue/consume-record! st (peek command))
            (when (= :send failure-site)
              (queue/close! st)
              (with-redefs [native/sendvec (fn [& _] (throw failure))]
                (queue/send-vecs st)))
            (is (= (if (= :head failure-site)
                     {:status :failed :cause failure} {:status :closed})
                   @(:progress result)))
            (is (every? #(identical? worker (second %)) @calls))
            (is (= [[:reserve true false] [:release] [:reserve false false]
                    [:publish [:h2o/output 7 11]] [:failure failure]]
                   (->> @calls
                        (remove #(= :wake (first %)))
                        (mapv #(into [(first %)] (drop 2 %))))))
            (is (some #(= :wake (first %)) @calls))
            (is (zero? (:used @(:state_ m)))))
          (finally (queue/on-cleanup st) (pool/retire! p)))))))

(defn- consume-mailbox! [st m]
  (loop []
    (when-let [command (mailbox/poll! m)]
      (queue/consume-record! st (peek command))
      (recur))))

(defn- credits [st m]
  {:blocks (pool/checked-out (:buffer-pool st))
   :bytes (budget/queued-bytes (:budget st))
   :receipts (count (:receipts @(:state_ st)))
   :reserved-receipts (:reserved-receipts @(:state_ st))
   :permits (:used @(:state_ m))})

(deftest accepted-finish-survives-ring-error-after-writer-lookup
  (doseq [finish [:close :end]
          waiting [:mailbox :proceed]]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)
          emitter (response/new-response-emitter (pi/map->Request (:req st)))
          entered (promise)
          resume (promise)
          done (promise)
          sends (atom [])
          paused? (atom false)
          ensure-writer @#'response/ensure-writer!]
      (reset! (.writer_ ^ol.busker.response.H2OResponseEmitter emitter) st)
      (with-redefs-fn
        {#'pi/wake (fn [_] nil)
         #'native/sendvec
         (fn [_ _ _ state]
           (swap! sends conj
                  [(vec (mapcat (fn [{:keys [block bytes]}]
                                  (when block
                                    (.toArray (.asSlice ^MemorySegment (:segment block) 0 (long bytes))
                                              ValueLayout/JAVA_BYTE)))
                                (:records (:in-flight @(:state_ st)))))
                   state]))
         #'response/ensure-writer!
         (fn [emitter]
           (let [writer (ensure-writer emitter)]
             (when (compare-and-set! paused? false true)
               (deliver entered true)
               (when (= :timeout (deref resume 5000 :timeout))
                 (throw (ex-info "Error thread was not resumed" {}))))
             writer))}
        (fn []
          (let [first-write (queue/emit-bounded! st nil 2
                                                 (fn [^ByteBuffer b] (.put b (byte-array [1 2])))
                                                 {:flush? true})
                _ (when (= waiting :proceed)
                    (consume-mailbox! st m)
                    (queue/send-vecs st))
                error-thread (Thread/startVirtualThread
                              #(do (response/send-ring-response!
                                    emitter {:status 200 :headers {"content-length" "invalid"} :body "x"})
                                   (deliver done true)))]
            (try
              (is (= true (deref entered 1000 :timeout)))
              (let [last-write (queue/emit-bounded! st nil 2
                                                    (fn [^ByteBuffer b] (.put b (byte-array [3 4])))
                                                    {:end? (= finish :end)})]
                (when (= finish :close) (is (true? (protocols/close! emitter))))
                (is (= [{:status :closed} {:status :closed}]
                       [@(:progress first-write) @(:progress last-write)]))
                (deliver resume true)
                (is (= true (deref done 1000 :timeout)))
                (consume-mailbox! st m)
                (queue/send-vecs st)
                (when (= waiting :proceed) (queue/on-proceed st))
                (is (= (if (= waiting :proceed)
                         [[[1 2] native/H2O_SEND_STATE_IN_PROGRESS]
                          [[3 4] native/H2O_SEND_STATE_FINAL]]
                         [[[1 2 3 4] native/H2O_SEND_STATE_FINAL]])
                       @sends))
                (is (= (if (= waiting :proceed) 1 2) (pool/checked-out p)))
                (queue/on-cleanup st)
                (is (= [{:status :closed} {:status :closed}]
                       [@(:progress first-write) @(:progress last-write)]))
                (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                       (credits st m))))
              (finally
                (deliver resume true)
                (.join error-thread 1000)
                (queue/on-cleanup st)
                (pool/retire! p)))))))))

(deftest incremental-error-and-close-use-one-admission-decision
  (doseq [winner [:close :error]]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)
          entered (promise)
          resume (promise)
          done (promise)
          failure (ex-info "Body failed" {})
          fail! queue/fail!
          sends (atom [])]
      (with-redefs [pi/wake (fn [_] nil)
                    native/sendvec (fn [_ _ count state] (swap! sends conj [count state]))
                    queue/fail! (fn [& args]
                                  (deliver entered true)
                                  (when (= :timeout (deref resume 5000 :timeout))
                                    (throw (ex-info "Failure was not resumed" {})))
                                  (apply fail! args))]
        (let [producer (Thread/startVirtualThread
                        #(deliver done
                                  (queue/emit-incremental!
                                   st nil
                                   (fn [^OutputStream out]
                                     (.write out (byte-array [5 6]))
                                     (throw failure)) {})))]
          (try
            (is (= true (deref entered 1000 :timeout)))
            (let [receipt (queue/flush! st)
                  expected (if (= winner :close) {:status :closed} {:status :failed :cause failure})]
              (when (= winner :close) (is (true? (queue/close! st))))
              (deliver resume true)
              (is (= {:status :accepted :progress receipt} (deref done 1000 :timeout)))
              (is (= expected (deref receipt 1000 :timeout)))
              (is (false? (queue/close! st)))
              (consume-mailbox! st m)
              (queue/send-vecs st)
              (is (= (if (= winner :close)
                       [[1 native/H2O_SEND_STATE_FINAL]]
                       [[0 native/H2O_SEND_STATE_ERROR]]) @sends))
              (queue/on-cleanup st)
              (is (= expected @receipt))
              (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                     (credits st m))))
            (finally
              (deliver resume true)
              (.join producer 1000)
              (queue/on-cleanup st)
              (pool/retire! p))))))))

(deftest transport-failure-still-discards-pending-finish
  (with-redefs [pi/wake (fn [_] nil)
                native/sendvec (fn [& _] nil)]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)]
      (try
        (let [first-write (queue/emit-bounded! st nil 1
                                               (fn [^ByteBuffer b] (.put b (byte 1))) {:flush? true})]
          (consume-mailbox! st m)
          (queue/send-vecs st)
          (let [last-write (queue/emit-bounded! st nil 1
                                                (fn [^ByteBuffer b] (.put b (byte 2))) {:end? true})]
            (queue/fail! st (ex-info "Worker failed" {}))
            (consume-mailbox! st m)
            (is (= [true 1 0] [(:aborted? @(:state_ st)) (pool/checked-out p)
                               (budget/queued-bytes (:budget st))]))
            (queue/on-stop st nil)
            (is (= [true 1] [(:native-stopped? @(:state_ st)) (pool/checked-out p)]))
            (is (false? (queue/runnable? st)))
            (queue/on-cleanup st)
            (is (= [{:status :closed} {:status :closed}]
                   [@(:progress first-write) @(:progress last-write)]))
            (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                   (credits st m)))))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest incremental-eof-settles-locally-after-prior-proceed-or-empty-source
  (doseq [payload [(byte-array [1 2]) (byte-array 0)]]
    (let [sends (atom [])]
      (with-redefs [pi/wake (fn [_] nil)
                    native/sendvec (fn [_ _ count state] (swap! sends conj [count state]))]
        (let [p (pool/create-stream-pool 8)
              m (mailbox/create)
              st (mailbox-controller p m)
              entered (promise)
              resume (promise)
              result (promise)
              producer (Thread/startVirtualThread
                        #(deliver result
                                  (queue/emit-incremental!
                                   st nil
                                   (fn [^OutputStream out]
                                     (.write out ^bytes payload)
                                     (deliver entered true)
                                     @resume) {})))]
          (try
            (is (= true (deref entered 1000 :timeout)))
            (let [receipt (queue/flush! st)]
              (is (= #{receipt} (:receipts @(:state_ st))))
              (consume-mailbox! st m)
              (queue/send-vecs st)
              (when (pos? (alength ^bytes payload)) (queue/on-proceed st))
              (is (not (realized? receipt)))
              (is (nil? (:in-flight @(:state_ st))))
              (deliver resume true)
              (is (= {:status :accepted :progress receipt} (deref result 1000 :timeout)))
              (consume-mailbox! st m)
              (queue/send-vecs st)
              (is (not (realized? receipt)))
              (is (identical? receipt (queue/flush! st)))
              (queue/send-vecs st)
              (is (= {:status :proceeded} (deref receipt 1000 :timeout)))
              (is (= (if (pos? (alength ^bytes payload))
                       [[1 native/H2O_SEND_STATE_IN_PROGRESS]] []) @sends))
              (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                     (credits st m)))
              (queue/on-cleanup st)
              (queue/on-cleanup st)
              (is (= {:status :proceeded} @receipt)))
            (finally
              (deliver resume true)
              (.join producer 1000)
              (queue/on-cleanup st)
              (pool/retire! p))))))))

(deftest incremental-preparation-failure-releases-initial-reservations
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)
          st (assoc-in st [:req :worker :reserve-command!]
                       (fn [_ _ _] (throw (IllegalStateException. "reservation failed"))))]
      (try
        (is (thrown-with-msg? IllegalStateException #"reservation failed"
                              (queue/emit-incremental!
                               st {:status 201}
                               (fn [_] (throw (AssertionError. "Source must not run"))) {})))
        (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
               (credits st m)))
        (is (= {:accepted 0 :head nil :producer nil}
               (select-keys @(:state_ st) [:accepted :head :producer])))
        (is (nil? (queue/flush! st)))
        (is (zero? (mailbox/queued m)))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest incremental-source-failure-keeps-accepted-receipt-and-native-storage
  (let [sends (atom [])]
    (with-redefs [pi/wake (fn [_] nil)
                  native/sendvec (fn [_ _ count state] (swap! sends conj [count state]))]
      (let [p (pool/create-stream-pool 8)
            m (mailbox/create)
            st (mailbox-controller p m)
            entered (promise)
            resume (promise)
            result (promise)
            failure (ex-info "Source failed" {})
            producer (Thread/startVirtualThread
                      #(deliver result
                                (queue/emit-incremental!
                                 st nil
                                 (fn [^OutputStream out]
                                   (.write out (byte-array [1 2]))
                                   (deliver entered true)
                                   @resume
                                   (.write out (byte-array [3 4]))
                                   (throw failure)) {})))]
        (try
          (is (= true (deref entered 1000 :timeout)))
          (let [receipt (queue/flush! st)]
            (is (= #{receipt} (:receipts @(:state_ st))))
            (consume-mailbox! st m)
            (queue/send-vecs st)
            (is (not (realized? receipt)))
            (deliver resume true)
            (is (= {:status :accepted :progress receipt} (deref result 1000 :timeout)))
            (is (= {:status :failed :cause failure} (deref receipt 1000 :timeout)))
            (consume-mailbox! st m)
            (is (= {:blocks 1 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                   (credits st m)))
            (is (= [[1 native/H2O_SEND_STATE_IN_PROGRESS]] @sends))
            (queue/on-proceed st)
            (is (= [[1 native/H2O_SEND_STATE_IN_PROGRESS] [0 native/H2O_SEND_STATE_ERROR]] @sends))
            (queue/on-cleanup st)
            (queue/on-cleanup st)
            (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                   (credits st m)))
            (is (= {:status :failed :cause failure} @receipt)))
          (finally
            (deliver resume true)
            (.join producer 1000)
            (queue/on-cleanup st)
            (pool/retire! p)))))))

(deftest close-captures-chunk-prefix-not-source-eof
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          commands (atom [])
          st (controller p commands)
          entered (promise)
          resume (promise)
          result (promise)
          producer (Thread/startVirtualThread
                    (fn []
                      (deliver result
                               (queue/emit-incremental!
                                st nil
                                (fn [^OutputStream out]
                                  (.write out (byte-array [1 2]))
                                  (deliver entered true)
                                  @resume
                                  (.write out (byte-array [3 4]))) {}))))]
      (try
        (is (= true (deref entered 1000 :timeout)))
        (let [receipt (queue/flush! st)]
          (is (= 2 (:flush-prefix @(:state_ st))))
          (is (true? (queue/close! st)))
          (is (= {:status :closed} @receipt))
          (deliver resume true)
          (let [returned (deref result 1000 :timeout)]
            (is (= {:status :accepted :progress receipt} returned)))
          (is (= 2 (count @commands)))
          (is (= 2 (:accepted @(:state_ st)))))
        (finally
          (deliver resume true)
          (.join producer 1000)
          (queue/on-cleanup st)
          (pool/retire! p))))))

(defn- await-condition!
  "Spins until `ready?` returns true and throws after five seconds."
  [ready?]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (while (not (ready?))
      (when (> (System/nanoTime) deadline)
        (throw (ex-info "Condition was not reached" {})))
      (Thread/onSpinWait))))

(deftest pending-byte-fullness-alone-rejects-and-flushes
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)]
      (try
        (let [first-write (queue/emit-bounded! st nil 8
                                               (fn [^ByteBuffer b] (.put b (byte-array 8 (byte 1))))
                                               {})]
          (is (= :accepted (:status first-write)))
          (is (= 0 (:flush-prefix @(:state_ st))))
          (is (= {:blocks 1 :bytes 8 :receipts 1 :reserved-receipts 0 :permits 1}
                 (credits st m)))
          (is (= {:status :full}
                 (queue/emit-bounded! st nil 1
                                      (fn [_] (throw (AssertionError. "Producer must not run")))
                                      {:wait? false})))
          (is (= 1 (:flush-prefix @(:state_ st))) "Pressure makes the older write ready.")
          (is (= {:blocks 1 :bytes 8 :receipts 1 :reserved-receipts 0 :permits 1}
                 (credits st m)))
          (is (= 1 (:accepted @(:state_ st)))))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest full-bytes-flush-older-output-before-a-blocking-writer-parks
  (with-redefs [pi/wake (fn [_] nil)
                native/sendvec (fn [& _] nil)]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)
          result (promise)]
      (try
        (queue/emit-bounded! st nil 8 (fn [^ByteBuffer b] (.put b (byte-array 8 (byte 1)))) {})
        (let [writer (Thread/startVirtualThread
                      #(deliver result
                                (queue/emit-bounded! st nil 1
                                                     (fn [^ByteBuffer b] (.put b (byte 2))) {})))]
          (try
            (await-condition! #(= 1 (:flush-prefix @(:state_ st))))
            (is (not (realized? result)) "No byte credit returns before the older write is sent.")
            (consume-mailbox! st m)
            (queue/send-vecs st)
            (is (= :accepted (:status (deref result 5000 ::timeout))))
            (is (= {:blocks 2 :bytes 1 :receipts 2 :reserved-receipts 0 :permits 1}
                   (credits st m)))
            (finally (.join writer 1000))))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest close-between-preparation-and-acceptance-publishes-nothing
  (with-redefs [pi/wake (fn [_] nil)]
    (let [p (pool/create-stream-pool 8)
          m (mailbox/create)
          st (mailbox-controller p m)
          fill pool/fill-native-range!
          called (atom 0)]
      (try
        (with-redefs [pool/fill-native-range!
                      (fn [segment capacity producer]
                        (let [written (fill segment capacity producer)]
                          (is (true? (queue/close! st)))
                          written))]
          (is (= {:status :closed}
                 (queue/emit-bounded! st {:status 201} 3
                                      (fn [^ByteBuffer b]
                                        (swap! called inc)
                                        (.put b (byte-array [1 2 3])))
                                      {}))))
        (is (= 1 @called))
        (is (nil? (queue/committed-head st)))
        (is (= 0 (:accepted @(:state_ st))))
        (is (zero? (mailbox/queued m)))
        (is (nil? (queue/flush! st)))
        (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
               (credits st m)))
        (finally (queue/on-cleanup st) (pool/retire! p))))))

(deftest blocking-writer-progresses-after-global-block-exhaustion
  (with-redefs [pi/wake (fn [_] nil)
                native/sendvec (fn [& _] nil)]
    (let [p (pool/create-stream-pool 8)
          mailboxes (vec (repeatedly 257 mailbox/create))
          streams (mapv #(mailbox-controller p %) mailboxes)
          fresh (peek streams)
          result (promise)]
      (try
        (doseq [st (pop streams)]
          (queue/emit-bounded! st nil 1 (fn [^ByteBuffer b] (.put b (byte 1))) {}))
        (is (= 256 (pool/checked-out p)))
        (let [writer (Thread/startVirtualThread
                      #(deliver result
                                (queue/emit-bounded! fresh nil 1
                                                     (fn [^ByteBuffer b] (.put b (byte 2))) {})))]
          (try
            (await-condition! #(every? (fn [st] (= 1 (:flush-prefix @(:state_ st)))) (pop streams)))
            (is (not (realized? result)) "Pressure does not reclaim queued blocks.")
            (let [st (first streams)]
              (consume-mailbox! st (first mailboxes))
              (queue/send-vecs st)
              (is (not (realized? result)) "A block in native use stays checked out.")
              (queue/on-proceed st))
            (is (= :accepted (:status (deref result 5000 ::timeout))))
            (is (= 256 (pool/checked-out p)))
            (finally (.join writer 1000))))
        (finally
          (doseq [st streams] (queue/on-cleanup st))
          (pool/retire! p))))))

(deftest wake-failure-after-streaming-acceptance-keeps-output-and-end
  (let [sends (atom [])]
    (with-redefs [pi/wake (fn [_] (throw (IllegalStateException. "wake failed")))
                  native/sendvec (fn [_ _ count state] (swap! sends conj [count state]))]
      (let [p (pool/create-stream-pool 8)
            m (mailbox/create)
            st (mailbox-controller p m)]
        (try
          (let [result (binding [*err* (java.io.StringWriter.)]
                         (queue/emit-bounded! st {:status 200} 1
                                              (fn [^ByteBuffer b] (.put b (byte 1)))
                                              {:end? true}))]
            (is (= :accepted (:status result)))
            (is (= {:status :closed} (deref (:progress result) 1000 ::timeout)))
            (is (= [false 1 1] [(queue/open? st) (:end-prefix @(:state_ st)) (mailbox/queued m)]))
            (binding [*err* (java.io.StringWriter.)]
              (consume-mailbox! st m)
              (queue/send-vecs st))
            (is (= [[1 native/H2O_SEND_STATE_FINAL]] @sends))
            (queue/on-cleanup st)
            (is (= {:blocks 0 :bytes 0 :receipts 0 :reserved-receipts 0 :permits 0}
                   (credits st m))))
          (finally (queue/on-cleanup st) (pool/retire! p)))))))
