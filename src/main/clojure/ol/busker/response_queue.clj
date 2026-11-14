(ns ^{:no-doc true :clj-kondo/config '{:linters {:locking-suspicious-lock {:level :off}}}}
 ol.busker.response-queue
  "Accepts streaming output with bounded storage and ordered transport progress.

  Application virtual threads reserve capacity and prepare one response operation
  at a time. Each response permits B pending bytes and 64 unsettled receipts,
  where B is `:output-buffer-size`. Payload blocks come from the worker's
  [[ol.busker.buffer-pool]]; [[ol.busker.worker-mailbox]] supplies command permits.
  Acceptance registers a record and its receipt atomically against closure.

  The platform worker consumes records in sequence and sends descriptors for the
  same native blocks. At most one send of up to B bytes is in flight. Installing
  that send returns pending-byte credit; non-final proceed returns its blocks
  and settles eligible receipt markers. Final-send blocks remain valid until
  native request cleanup. Close settles receipts without releasing native data.

  Flush and close capture accepted sequence prefixes without waiting for private
  producers or reserving mailbox space. Empty writes settle as ordered local
  markers. Pressure makes accepted unflushed output ready; it cannot reclaim
  private or in-flight blocks. Work registration and removal share a monitor so
  a concurrent producer cannot leave runnable output untracked.

  [[emit-bounded!]] accepts a prepared bounded operation; [[emit-incremental!]]
  gives a blocking body writer one receipt across its chunks and EOF. Applications
  use [[ol.busker.protocols/emit!]], which validates and normalizes their input."
  (:require
   [babashka.ffi :as mem]
   [ol.busker.buffer-pool :as bp]
   [ol.busker.byte-bounded-queue :as bbq]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [taoensso.trove :as trove])
  (:import
   [java.io IOException OutputStream]
   [java.lang.foreign Arena MemorySegment]
   [java.nio ByteBuffer]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean]))

(set! *warn-on-reflection* true)

(declare flush! close! fail! send-vecs on-stop emit-incremental! pending-work?)

(defn open? [st] (:open? @(:state_ st)))
(defn committed-head [st] (:head @(:state_ st)))
(defn committed? [st] (some? (committed-head st)))

(defn- work-map ^ConcurrentHashMap [st]
  (-> st :req :worker :callback-dispatch :streaming-work))

(defn- notify-capacity! [st]
  (locking (:lock st) (.notifyAll ^Object (:lock st)))
  (bp/signal! (:buffer-pool st))
  (let [worker (-> st :req :worker)]
    (when-let [f (:wake-command-waiters! worker)]
      (try (f worker) (catch Throwable _ nil)))))

(defn- signal! [st]
  (locking (:lock st)
    (when-let [work (work-map st)]
      (if (pending-work? st)
        (.put work (:drain-state_ st) st)
        (.remove work (:drain-state_ st)))))
  ;; Admission is complete. Notification failures cannot change its result.
  (try (pi/wake (-> st :req :worker))
       (catch Throwable t
         (try (trove/log! {:level :error :id :response/wake-failed :error t})
              (catch Throwable _ nil)))))

(defn- terminal-callback! [st]
  (when-let [f (-> st :req :on-terminal!)]
    (try (f)
         (catch Throwable t
           (try (trove/log! {:level :error :id :response/terminal-failed :error t})
                (catch Throwable _ nil))))))

(defn- settle-locked! [st receipt result]
  (when (contains? (:receipts @(:state_ st)) receipt)
    ;; Delivery precedes removal while close uses this same monitor.
    (deliver receipt result)
    (swap! (:state_ st) update :receipts disj receipt)
    (.notifyAll ^Object (:lock st))))

(defn- close-locked! [st result]
  (let [s @(:state_ st)]
    (when (:open? s)
      (doseq [p (:receipts s)] (deliver p result))
      (swap! (:state_ st) assoc :open? false :receipts #{}
             :flush-prefix (:accepted s) :end-prefix (:accepted s))
      (.notifyAll ^Object (:lock st))
      true)))

(defn close!
  "Closes admission and requests a final send of the accepted prefix."
  [st]
  (let [changed? (locking (:lock st) (boolean (close-locked! st {:status :closed})))]
    (when changed?
      (notify-capacity! st)
      (signal! st)
      (terminal-callback! st))
    changed?))

(defn flush!
  "Requests the accepted prefix without waiting; returns the last body receipt."
  [st]
  (let [[p changed?]
        (locking (:lock st)
          (let [s @(:state_ st)]
            (when (:open? s)
              (swap! (:state_ st) assoc :flush-prefix (:accepted s)))
            [(:last-receipt s) (:open? s)]))]
    (when changed? (signal! st))
    p))

(defn- update-pressure! [st]
  (locking (:lock st)
    (let [s @(:state_ st)
          unflushed? (some #(and (> (:seq %) (:flush-prefix s)) (pos? (:bytes %)))
                           (concat (:queued s) (vals (:unconsumed s))))]
      (if unflushed?
        (bp/track-pressure! (:buffer-pool st) (:drain-state_ st) #(flush! st))
        (bp/untrack-pressure! (:buffer-pool st) (:drain-state_ st))))))

(defn- pressure! [st]
  (flush! st)
  (bp/relieve-pressure! (:buffer-pool st)))

(defn- acquire-producer! [st wait?]
  (locking (:lock st)
    (loop []
      (let [s @(:state_ st)
            thread (Thread/currentThread)]
        (cond
          (not (:open? s)) :closed
          (identical? thread (:producer s))
          (throw (IllegalStateException. "Recursive emit! on the same response"))
          (nil? (:producer s))
          (do (swap! (:state_ st) assoc :producer thread) :accepted)
          (not wait?) :full
          :else (do (.wait ^Object (:lock st)) (recur)))))))

(defn- release-producer! [st]
  (locking (:lock st)
    (swap! (:state_ st) assoc :producer nil)
    (.notifyAll ^Object (:lock st))))

(defn- validate-head! [st head informational?]
  (when (and head (not informational?) (committed? st))
    (throw (IllegalStateException. "Final response headers were already accepted"))))

(defn- reserve-credit! [st capacity receipt? wait?]
  (loop []
    (let [result
          (locking (:lock st)
            (let [s @(:state_ st)]
              (cond
                (not (:open? s)) :closed
                (and (or (not receipt?)
                         (< (+ (count (:receipts s)) (:reserved-receipts s)) 64))
                     (bbq/reserve! (:budget st) capacity))
                (do (when receipt? (swap! (:state_ st) update :reserved-receipts inc))
                    :accepted)
                :else :full)))]
      (if (not= result :full)
        result
        (do
          (pressure! st)
          (if-not wait?
            :full
            (do
              (locking (:lock st)
                (let [s @(:state_ st)]
                  (when (and (:open? s)
                             (or (> capacity (bbq/remaining-bytes (:budget st)))
                                 (and receipt? (>= (+ (count (:receipts s))
                                                      (:reserved-receipts s)) 64))))
                    (.wait ^Object (:lock st)))))
              (recur))))))))

(defn- release-credit! [st capacity receipt?]
  (locking (:lock st)
    (bbq/release! (:budget st) capacity)
    (when receipt? (swap! (:state_ st) update :reserved-receipts dec)))
  (notify-capacity! st))

(defn- borrow-block! [st wait?]
  (let [pool (:buffer-pool st)]
    (loop []
      (let [observed (bp/epoch pool)
            result (if (open? st) (bp/try-borrow! pool) :closed)]
        (if (not= result :full)
          result
          (do
            (pressure! st)
            (if-not wait?
              :full
              (do (bp/await-change! pool observed #(not (open? st))) (recur)))))))))

(defn- reserve-command! [st wait?]
  (let [worker (-> st :req :worker)]
    ((:reserve-command! worker) worker wait? #(not (open? st)))))

(defn- release-command! [st permit]
  (let [worker (-> st :req :worker)]
    ((:release-command! worker) worker permit)))

(defn- release-record! [st record]
  (when (compare-and-set! (:resource_ record) :pending :released)
    (bbq/release! (:budget st) (:bytes record))
    (bp/release! (:buffer-pool st) (:block record))
    (notify-capacity! st)))

(defn- accept-record!
  [st record receipt? end? flush?]
  (locking (:lock st)
    (when (open? st)
      (let [s @(:state_ st)
            n (inc (:accepted s))
            head (or (:head record)
                     (when (and (:body? record) (nil? (:head s)))
                       {:status 200 :headers [] :content-length -1 :compress-hint 0}))
            record (assoc record :seq n :head head :resource_ (atom :pending))
            final-head? (and head (not (:informational? record)))]
        (validate-head! st head (:informational? record))
        (swap! (:state_ st)
               (fn [s]
                 (cond-> (-> s
                             (assoc :accepted n)
                             (assoc-in [:unconsumed n] record))
                   final-head? (assoc :head head)
                   receipt? (-> (update :reserved-receipts dec)
                                (update :receipts conj (:receipt record))
                                (assoc :last-receipt (:receipt record)))
                   (or flush? end?) (assoc :flush-prefix n))))
        (when end? (close-locked! st {:status :closed}))
        record))))

(defn- publish! [st permit record end?]
  (try
    (let [worker (-> st :req :worker)]
      ((:publish-command! worker) worker permit
                                  [:h2o/output (-> st :req :dispatch-module-id)
                                   (-> st :req :dispatch-request-seq) st record]))
    (catch Throwable t
      ;; The accepted record remains reachable until this explicit discard.
      (try (release-command! st permit) (catch Throwable _ nil))
      (fail! st t)))
  (update-pressure! st)
  (signal! st)
  (when end?
    (notify-capacity! st)
    (terminal-callback! st)))

(defn- prepare-record!
  [st head capacity producer {:keys [wait? body? informational? end? flush? suppress-body?]
                              :or {wait? true body? true}} receipt receipt? marker?]
  (let [credit (reserve-credit! st capacity receipt? wait?)]
    (if (not= credit :accepted)
      {:status credit}
      (let [block_ (volatile! nil)
            permit_ (volatile! nil)
            accepted_ (volatile! false)
            credit_ (volatile! capacity)]
        (try
          (let [block (when (pos? capacity) (borrow-block! st wait?))]
            (if (keyword? block)
              {:status block}
              (do
                (vreset! block_ block)
                (let [permit (reserve-command! st wait?)]
                  (if (keyword? permit)
                    {:status permit}
                    (do
                      (vreset! permit_ permit)
                      (let [written (if block
                                      (bp/fill-native-range! (:segment block) capacity producer)
                                      0)
                            n (if suppress-body? 0 written)
                            _ (bbq/release! (:budget st) (- capacity n))
                            _ (vreset! credit_ n)
                            _ (when (and block (zero? n))
                                (bp/release! (:buffer-pool st) block)
                                (vreset! block_ nil))
                            record (accept-record! st
                                                   {:head head :informational? informational?
                                                    :body? body? :block @block_ :bytes n
                                                    :receipt receipt :marker? marker?}
                                                   receipt? end? flush?)]
                        (if-not record
                          {:status :closed}
                          (do
                            (vreset! accepted_ true)
                            (publish! st permit record end?)
                            (cond-> {:status :accepted} body? (assoc :progress receipt)))))))))))
          (finally
            (when-not @accepted_
              (when @permit_ (release-command! st @permit_))
              (bp/release! (:buffer-pool st) @block_)
              (release-credit! st @credit_ receipt?))))))))

(defn emit-bounded!
  "Accepts one normalized head/body operation. `producer` fills at most `capacity` bytes.
  Header-only calls set `:body? false`; application option validation precedes this call."
  [st head capacity producer opts]
  (if-not (open? st)
    {:status :closed}
    (let [_ (validate-head! st head (:informational? opts))
          _ (when-not (and (integer? capacity) (<= 0 capacity (:output-buffer-size st)))
              (throw (IllegalArgumentException. "Body capacity exceeds output-buffer-size")))
          wait? (get opts :wait? true)
          body? (get opts :body? true)
          permission (acquire-producer! st wait?)]
      (if (not= permission :accepted)
        {:status permission}
        (try
          (validate-head! st head (:informational? opts))
          (prepare-record! st head capacity producer opts (when body? (promise)) body? body?)
          (finally (release-producer! st)))))))

(defn- begin-incremental! [st head receipt]
  (let [capacity (:output-buffer-size st)
        credit (reserve-credit! st capacity true true)]
    (if (not= :accepted credit)
      {:status credit}
      (let [block_ (volatile! nil)
            permit_ (volatile! nil)
            accepted_ (volatile! false)]
        (try
          (let [block (borrow-block! st true)]
            (if (keyword? block)
              {:status block}
              (do
                (vreset! block_ block)
                (let [permit (reserve-command! st true)]
                  (if (keyword? permit)
                    {:status permit}
                    (do
                      (vreset! permit_ permit)
                      (if-let [record (accept-record! st
                                                      {:head head :body? true :bytes 0
                                                       :receipt receipt :marker? false}
                                                      true false false)]
                        (do
                          (vreset! accepted_ true)
                          (publish! st permit record false)
                          {:status :accepted :progress receipt :initial block})
                        {:status :closed})))))))
          (finally
            (when-not @accepted_
              (when @permit_ (release-command! st @permit_))
              (bp/release! (:buffer-pool st) @block_)
              (release-credit! st capacity true))))))))

(defn- publish-initial! [st initial receipt n producer opts]
  (let [capacity (:output-buffer-size st)
        block @initial
        retained_ (volatile! block)]
    (reset! initial nil)
    (let [permit_ (volatile! nil)
          accepted_ (volatile! false)
          credit_ (volatile! capacity)]
      (try
        (let [permit (reserve-command! st true)]
          (when (keyword? permit) (throw (IOException. "Response closed")))
          (vreset! permit_ permit)
          (let [written (bp/fill-native-range! (:segment block) n producer)
                used (if (:suppress-body? opts) 0 written)]
            (when (zero? used)
              (bp/release! (:buffer-pool st) block)
              (vreset! retained_ nil))
            (bbq/release! (:budget st) (- capacity used))
            (vreset! credit_ used)
            (if-let [record (accept-record! st {:body? true :bytes used :block @retained_
                                                :receipt receipt :marker? false}
                                            false false (:flush? opts))]
              (do (vreset! accepted_ true) (publish! st permit record false))
              (throw (IOException. "Response closed")))))
        (finally
          (when-not @accepted_
            (when @permit_ (release-command! st @permit_))
            (bp/release! (:buffer-pool st) @retained_)
            (release-credit! st @credit_ false)))))))

(definterface BlockOutput
  (fillBlock [producer]))

(defn emit-incremental!
  "Accepts a begin record before calling `write-body` with an OutputStream.
  The stream also implements `BlockOutput`: `fillBlock` publishes the bytes that
  `producer` writes into one native block of up to `:output-buffer-size` bytes.
  One receipt covers all chunks and EOF. Failures after begin return that receipt."
  [st head write-body opts]
  (if-not (open? st)
    {:status :closed}
    (let [permission (acquire-producer! st true)]
      (if (not= permission :accepted)
        {:status permission}
        (try
          (validate-head! st head false)
          (let [receipt (promise)
                begin (begin-incremental! st head receipt)]
            (if (not= :accepted (:status begin))
              begin
              (let [initial (atom (:initial begin))
                    fill! (fn [n producer]
                            (when-not (open? st) (throw (IOException. "Response closed")))
                            (if @initial
                              (publish-initial! st initial receipt n producer opts)
                              (let [result (prepare-record!
                                            st nil n producer
                                            (assoc opts :wait? true :end? false :body? true)
                                            receipt false false)]
                                (when-not (= :accepted (:status result))
                                  (throw (IOException. "Response closed"))))))
                    out (proxy [OutputStream BlockOutput] []
                          (write
                            ([x]
                             (if (number? x)
                               (.write ^OutputStream this (byte-array [(unchecked-byte x)]) 0 1)
                               (.write ^OutputStream this x 0 (alength ^bytes x))))
                            ([bytes off len]
                             (java.util.Objects/checkFromIndexSize (int off) (int len) (alength ^bytes bytes))
                             (loop [offset off remaining len]
                               (when (pos? remaining)
                                 (let [n (min remaining (:output-buffer-size st))]
                                   (fill! n (fn [^ByteBuffer buffer]
                                              (.put buffer ^bytes bytes (int offset) (int n))))
                                   (recur (+ offset n) (- remaining n)))))))
                          (fillBlock [producer] (fill! (:output-buffer-size st) producer))
                          (flush [] (flush! st))
                          (close [] (flush! st)))]
                (try
                  (write-body out)
                  (when-let [block @initial]
                    (reset! initial nil)
                    (bp/release! (:buffer-pool st) block)
                    (release-credit! st (:output-buffer-size st) false))
                  (when (open? st)
                    (prepare-record! st nil 0 nil (assoc opts :wait? true :body? true)
                                     receipt false true))
                  (catch Throwable t
                    (fail! st t {:if-open? true}))
                  (finally
                    (when-let [block @initial]
                      (bp/release! (:buffer-pool st) block)
                      (release-credit! st (:output-buffer-size st) false))))
                {:status :accepted :progress receipt})))
          (finally (release-producer! st)))))))

(defn- release-native! [st flight result]
  (doseq [record (:records flight)]
    (when (compare-and-set! (:resource_ record) :native :released)
      (bp/release! (:buffer-pool st) (:block record))))
  (when-let [^Arena arena (:arena flight)] (.close arena))
  (locking (:lock st)
    (doseq [record (:records flight)
            :when (:marker? record)]
      (settle-locked! st (:receipt record) result)))
  (notify-capacity! st))

(defn- detach-pending! [st]
  (locking (:lock st)
    (let [s @(:state_ st)
          records (into (vec (:queued s)) (vals (:unconsumed s)))]
      (swap! (:state_ st) assoc :queued [] :unconsumed {})
      records)))

(defn fail!
  "Stops admission and settles pending receipts after failure.
  Discards queued records; an installed native send stays retained until a safe
  non-final proceed or request cleanup. Private producers release on return.

  | Option | Default | Meaning |
  |--------|---------|---------|
  | `:if-open?` | `false` | Apply an application failure only if admission is still open. |

  Transport failures use the default even after graceful closure."
  ([st cause] (fail! st cause {}))
  ([st cause {:keys [if-open?]}]
   (let [result (if cause {:status :failed :cause cause} {:status :closed})]
     (when-some [changed? (locking (:lock st)
                            (when (or (not if-open?) (open? st))
                              (let [changed? (boolean (close-locked! st result))]
                                (swap! (:state_ st) assoc :aborted? true :failure cause)
                                changed?)))]
       (doseq [record (detach-pending! st)] (release-record! st record))
       (bp/untrack-pressure! (:buffer-pool st) (:drain-state_ st))
       (notify-capacity! st)
       (signal! st)
       (when changed? (terminal-callback! st))))))

(defn on-stop [st reason]
  (fail! st (when (instance? Throwable reason) reason))
  (locking (:lock st) (swap! (:state_ st) assoc :native-stopped? true)))

(defn on-cleanup
  "Releases the final native send only after request-pool cleanup."
  [st]
  (on-stop st nil)
  (let [flight (locking (:lock st)
                 (let [f (:in-flight @(:state_ st))]
                   (swap! (:state_ st) assoc :in-flight nil :cleaned? true)
                   f))]
    (when flight (release-native! st flight {:status :closed})))
  (when-let [work (work-map st)] (.remove work (:drain-state_ st))))

(defn discard-record!
  "Discards an accepted mailbox record whose request identity is no longer live."
  [st record]
  (on-stop st nil)
  (release-record! st record))

(defn consume-record!
  "Receives an accepted record on the request's platform worker."
  [st record]
  (let [live? (locking (:lock st)
                (not (:aborted? @(:state_ st))))]
    (if-not live?
      (release-record! st record)
      (try
        (when-let [head (:head record)]
          ((:apply-head! (:req st)) head (boolean (:informational? record)))
          (when-not (:informational? record)
            (locking (:lock st) (swap! (:state_ st) assoc :started? true))))
        (locking (:lock st)
          (let [s @(:state_ st)]
            (when-not (= (:seq record) (inc (:consumed s)))
              (throw (IllegalStateException. "Response mailbox record out of order")))
            (swap! (:state_ st)
                   (fn [s]
                     (cond-> (-> s
                                 (assoc :consumed (:seq record))
                                 (update :unconsumed dissoc (:seq record)))
                       (and (not (:aborted? s))
                            (or (pos? (:bytes record)) (:marker? record)))
                       (update :queued conj record))))))
        (when-not (or (pos? (:bytes record)) (:marker? record))
          (release-record! st record))
        (signal! st)
        (catch Throwable t
          (fail! st t)
          (let [worker (-> st :req :worker)]
            (when-let [f (:on-native-failure! worker)] (f worker t))))))))

(defn- ready-prefix [s]
  (min (:consumed s) (:flush-prefix s)))

(defn runnable?
  "Returns true only for work that can run without a native proceed callback."
  [st]
  (let [s @(:state_ st)]
    (boolean
     (and (not (:cleaned? s)) (not (:native-stopped? s))
          (not (:final-sent? s)) (nil? (:in-flight s))
          (or (:aborted? s)
              (some #(<= (:seq %) (ready-prefix s)) (:queued s))
              (and (some? (:end-prefix s))
                   (>= (:consumed s) (:end-prefix s))))))))

(defn pending-work? [st]
  (or (some? (:in-flight @(:state_ st))) (runnable? st)))

(defn- refresh-tracking! [st]
  ;; Removal and producer registration share the response monitor.
  (locking (:lock st)
    (when-let [work (work-map st)]
      (if (pending-work? st)
        (.put work (:drain-state_ st) st)
        (.remove work (:drain-state_ st))))))

(defn package-chunks
  "Returns `{:seg descriptors :veccnt count}` allocated in `arena` on the worker.
  Payload addresses refer to retained pool blocks; no payload bytes are copied.
  Keep the blocks valid until native use ends."
  [records arena]
  (let [data (filterv #(pos? (:bytes %)) records)
        count (count data)
        seg (mem/alloc arena (* count h2o/size-of-h2o-sendvec-t))]
    (doseq [[i record] (map-indexed vector data)]
      (h2o/sendvec-init-raw
       (mem/slice seg (* i h2o/size-of-h2o-sendvec-t) h2o/size-of-h2o-sendvec-t)
       ^MemorySegment (-> record :block :segment) (:bytes record)))
    {:seg seg :veccnt count}))

(defn- declared-length
  "Returns the content length that final `head` declares for body bytes that go
  on the wire, or -1. HEAD responses and statuses 204, 205 and 304 send none."
  ^long [st head]
  (let [length (long (or (:content-length head) -1))]
    (if (or (neg? length)
            (= :head (-> st :req :req-ctx :request-method))
            (contains? #{204 205 304} (:status head)))
      -1
      length)))

(defn- next-flight!
  "Takes the next ready output. Returns a flight, `:local` when it settled empty
  markers itself, or `::length-mismatch`, taking nothing, when the output would
  pass the declared content length or end short of it."
  [st]
  (locking (:lock st)
    (when (runnable? st)
      (let [s @(:state_ st)
            prefix (ready-prefix s)
            records (vec (take-while #(<= (:seq %) prefix) (:queued s)))
            remaining (subvec (vec (:queued s)) (count records))
            final? (or (:aborted? s)
                       (and (some? (:end-prefix s))
                            (>= (:consumed s) (:end-prefix s)) (empty? remaining)))
            bytes (reduce + 0 (map :bytes records))
            declared (if (:aborted? s) -1 (declared-length st (:head s)))
            ^longs sent-bytes (:sent-bytes st)
            sent (+ (aget sent-bytes 0) (long bytes))]
        (cond
          (and (not (neg? declared))
               (or (> sent declared) (and final? (< sent declared))))
          ::length-mismatch

          (and (zero? bytes) (not final?))
          (do
            (swap! (:state_ st) assoc :queued remaining)
            (doseq [record records]
              (release-record! st record)
              (when (:marker? record)
                (settle-locked! st (:receipt record) {:status :proceeded})))
            :local)
          :else
          (let [flight {:records records :final? final? :error? (:aborted? s)}]
            (swap! (:state_ st) assoc :queued remaining)
            (aset sent-bytes 0 sent)
            (doseq [record records]
              (when-not (compare-and-set! (:resource_ record) :pending :native)
                (throw (IllegalStateException. "Stream record already released")))
              (bbq/release! (:budget st) (:bytes record)))
            ;; Complete payload retention precedes every possible native upcall.
            (swap! (:state_ st) assoc :in-flight flight :final-sent? final?)
            flight))))))

(defn send-vecs
  "Sends ready output on the request's platform worker.
  Installs the in-flight record before native entry because proceed or cleanup
  may run synchronously. Reentrant proceed uses a bounded, nonrecursive loop.
  Uncertain native-entry failure retains the send and reports worker failure.
  Returns true when it handed output to H2O."
  [st]
  (when (.compareAndSet ^AtomicBoolean (:sending?_ st) false true)
    (try
      (loop [turns 0
             sent? false]
        (if-let [flight (next-flight! st)]
          (if (= ::length-mismatch flight)
            (let [error (ex-info "Response body length does not match its content-length"
                                 {:content-length (:content-length (committed-head st))})]
              (trove/log! {:level :error :id :response/content-length-mismatch :error error})
              (fail! st error)
              (recur turns sent?))
            (let [local? (= flight :local)]
              (when-not local?
                (let [arena (Arena/ofConfined)]
                  (locking (:lock st)
                    (swap! (:state_ st) assoc-in [:in-flight :arena] arena))
                  (try
                    (let [{:keys [seg veccnt]} (package-chunks (:records flight) arena)]
                      (when-not (:started? @(:state_ st))
                        ((:apply-head! (:req st))
                         {:status 200 :headers [] :content-length -1 :compress-hint 0} false)
                        (locking (:lock st)
                          (swap! (:state_ st) assoc :started? true)))
                      (when-not (:cleaned? @(:state_ st))
                        (h2o/sendvec (-> st :req :req-ctx :req) seg veccnt
                                     (cond (:error? flight) h2o/H2O_SEND_STATE_ERROR
                                           (:final? flight) h2o/H2O_SEND_STATE_FINAL
                                           :else h2o/H2O_SEND_STATE_IN_PROGRESS))))
                    (catch Throwable t
                      ;; Entry may have reached native code: retain the installed send.
                      (fail! st t)
                      (let [worker (-> st :req :worker)]
                        (when-let [f (:on-native-failure! worker)] (f worker t)))))))
              ;; Only a synchronous proceed can remove this native flight here.
              ;; Locally settled markers consume this response's ordinary turn.
              (if (and (not local?) (< turns 255) (runnable? st))
                (recur (inc turns) true)
                (or sent? (not local?)))))
          sent?))
      (finally
        (.set ^AtomicBoolean (:sending?_ st) false)
        (update-pressure! st)
        (refresh-tracking! st)
        (notify-capacity! st)))))

(defn on-proceed [st]
  (let [flight (locking (:lock st)
                 (let [f (:in-flight @(:state_ st))]
                   (when (and f (not (:final? f)))
                     (swap! (:state_ st) assoc :in-flight nil)
                     f)))]
    (when flight (release-native! st flight {:status :proceeded}))
    (send-vecs st)))

;; `sent-bytes` holds the body bytes handed to native sends. Only next-flight!
;; reads and writes it, under `lock`.
(defrecord H2OResponseWriter [req lock state_ budget buffer-pool output-buffer-size
                              drain-state_ sending?_ sent-bytes]
  pi/Stopable
  (stop [this] (on-stop this nil)))

(defn new-response-state [req]
  (let [lock (Object.)
        size (-> req :config :output-buffer-size)]
    (->H2OResponseWriter
     req lock
     (atom {:open? true :producer nil :head nil :receipts #{} :reserved-receipts 0
            :accepted 0 :consumed 0 :flush-prefix 0 :end-prefix nil
            :unconsumed (sorted-map) :queued [] :in-flight nil})
     (bbq/byte-budget size lock) (-> req :worker :stream-pool) size
     (Object.) (AtomicBoolean. false) (long-array 1))))

(defn create-response-writer [req]
  (let [st (new-response-state req)
        {:keys [proceed stop]} (:callback-pointers req)]
    (assoc st :on-proceed-cb-ptr proceed :on-stop-cb-ptr stop)))