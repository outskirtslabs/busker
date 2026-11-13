(ns ol.busker.http3-spread-test
  (:require
   [babashka.ffi :as mem]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [ol.busker :as busker]
   [ol.busker.generation :as generation]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as proto]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign Arena FunctionDescriptor Linker Linker$Option MemoryLayout
    MemorySegment ValueLayout]
   [java.lang.invoke MethodHandle]
   [java.nio ByteBuffer ByteOrder]
   [java.nio.file Files Path]))

(defn- worker-contexts
  "HTTP/3 contexts of each worker of the active generation."
  [server]
  (::generation/http3-worker-contexts (:instance (:active @(:busker/state server)))))

(defn- sum-over-workers
  "`f` of each worker's HTTP/3 contexts, summed per worker."
  [f worker-ctxs]
  (mapv (fn [ctxs]
          (reduce (fn [total ctx]
                    (if (or (nil? ctx) (mem/null? ctx))
                      total
                      (+ total (long (f ctx)))))
                  0
                  ctxs))
        worker-ctxs))

(defn- worker-counts
  "HTTP/3 connections of each worker of the active generation."
  [server]
  (sum-over-workers h2o/http3-num-connections (worker-contexts server)))

(defn- active-workers
  "The UDP transport, generation id and worker count of the active
  generation, which name its workers' counters also after it retires."
  [server]
  (let [generation (:instance (:active @(:busker/state server)))]
    {:transport (first (#'generation/http3-transport-resources
                        (::generation/listener-claims generation)))
     :node (::generation/generation-id generation)
     :n (count (::generation/http3-worker-contexts generation))}))

(defn- per-worker
  [f {:keys [transport node n]}]
  (mapv #(long (f transport node (int %))) (range n)))

(defn- connected-sockets
  "Connected UDP sockets of each of `workers`."
  [workers]
  (per-worker h2o/http3-connected-sockets workers))

(defn- forwarded-datagrams
  "Datagrams each of `workers` has forwarded to other workers."
  [workers]
  (per-worker h2o/http3-forwarded-datagrams workers))

(defn- wait-until
  "Polls `f` until `done?` holds for its value and returns the last value."
  [f done?]
  (loop [attempt 0]
    (let [value (f)]
      (if (or (done? value) (= 100 attempt))
        value
        (do (Thread/sleep 50)
            (recur (inc attempt)))))))

(defn- wait-for-counts
  "Waits until `done?` holds for the worker counts and returns them."
  [server done?]
  (wait-until #(worker-counts server) done?))

(defn- settled
  "Polls `f` until its value stays the same for 300 ms and returns it."
  [f]
  (loop [value (f) attempt 0]
    (Thread/sleep 300)
    (let [next-value (f)]
      (if (or (= value next-value) (= 20 attempt))
        next-value
        (recur next-value (inc attempt))))))

(defn- udp-sockets-on-port
  "UDP sockets of this network namespace bound to local `port`, and how many
  of them are connected, from /proc/net/udp and /proc/net/udp6."
  [port]
  (let [hex-port (format "%04X" port)
        rows (for [file ["/proc/net/udp" "/proc/net/udp6"]
                   line (rest (Files/readAllLines (Path/of file (make-array String 0))))
                   :let [[_ local remote] (str/split (str/trim line) #"\s+")]
                   :when (str/ends-with? local (str ":" hex-port))]
               remote)]
    {:bound (count rows)
     :connected (count (remove #(str/ends-with? % ":0000") rows))}))

(defn- start-holding-server
  "Starts a server whose `/hold/N` responses stay open until `release!` is
  called with N, so each request keeps one HTTP/3 connection open."
  [n-workers]
  (let [port (util/free-port)
        releases (atom {})
        release-of (fn [id] (get (swap! releases update id #(or % (promise))) id))
        handler (fn [{:keys [uri] emitter :ol.busker.request/emitter}]
                  (if-let [[_ id] (re-matches #"/hold/(\d+)" uri)]
                    (do
                      (future
                        (proto/emit! emitter {:status 200 :headers {"content-type" "text/plain"}})
                        (proto/emit! emitter "held-")
                        (proto/flush! emitter)
                        (deref (release-of id) 30000 nil)
                        (proto/emit! emitter "done")
                        (proto/close! emitter))
                      {:body emitter})
                    {:status 200 :body "ready"}))
        config (util/with-static-tls
                 (util/with-handler
                   handler
                   {:n-workers n-workers
                    :entrypoints {:tls {:bind (str "127.0.0.1:" port)
                                        :http3? true
                                        :tls {:tls-compatibility-mode :modern}}}}))
        server (busker/start! config)]
    (is (zero? (:exit (util/wait-for-curl-ready! :https :h2 port "/ready" :max-time 5))))
    {:server server
     :config config
     :port port
     :release! (fn [id] (deliver (release-of (str id)) true))}))

(defn- open-held!
  "Opens HTTP/3 connection `id` and waits until the server counts it. Returns
  the request future and the index of the worker that took the connection."
  [{:keys [server port]} id]
  (let [before (worker-counts server)
        total (inc (reduce + before))
        request (future (util/curl :https :h3 port (str "/hold/" id) :max-time 40))
        after (wait-for-counts server #(= total (reduce + %)))]
    (is (= total (reduce + after)) (str "connection " id " is counted"))
    {:request request
     :worker (first (keep-indexed (fn [idx [b a]] (when (< b a) idx))
                                  (map vector before after)))}))

(defn- finish!
  [{:keys [release!]} id {:keys [request]}]
  (release! id)
  (let [result (deref request 20000 nil)]
    (is (zero? (:exit result)) (str "request " id " succeeds: " (:err result)))
    (is (= "held-done" (:out result)))))

(defn- libc ^MethodHandle [fn-name ^MemoryLayout ret & args]
  (let [linker (Linker/nativeLinker)]
    (.downcallHandle linker
                     (.orElseThrow (.find (.defaultLookup linker) fn-name))
                     (FunctionDescriptor/of ret (into-array MemoryLayout args))
                     ^"[Ljava.lang.foreign.Linker$Option;" (make-array Linker$Option 0))))

(defn- native-bytes ^MemorySegment [^Arena arena ^ByteBuffer buf]
  (let [bytes (.array buf)
        seg (.allocate arena (long (alength bytes)) 8)]
    (MemorySegment/copy (MemorySegment/ofArray bytes) 0 seg 0 (alength bytes))
    seg))

(defn- force-udp-group-to-first-socket!
  "Attaches a classic BPF program to the SO_REUSEPORT group of UDP
  127.0.0.1:`port` that sends every datagram to the first socket of the group
  (the first worker's), then closes the helper socket; the program stays.
  Returns false when the kernel refuses a step."
  [port]
  (let [int-l ValueLayout/JAVA_INT
        addr-l ValueLayout/ADDRESS
        socket (libc "socket" int-l int-l int-l int-l)
        setsockopt (libc "setsockopt" int-l int-l int-l int-l addr-l int-l)
        bind (libc "bind" int-l int-l addr-l int-l)
        close (libc "close" int-l int-l)
        le #(.order (ByteBuffer/allocate %) ByteOrder/LITTLE_ENDIAN)]
    (with-open [arena (Arena/ofConfined)]
      (let [fd (.invokeWithArguments socket (object-array [(int 2) (int 2) (int 0)]))
            one (native-bytes arena (.putInt ^ByteBuffer (le 4) 1))
            sockaddr (native-bytes arena (doto ^ByteBuffer (le 16)
                                           (.putShort (short 2))
                                           (.put (unchecked-byte (bit-shift-right port 8)))
                                           (.put (unchecked-byte port))
                                           (.put (byte-array [127 0 0 1]))))
            ret-0 (native-bytes arena (.putShort ^ByteBuffer (le 8) (short 6)))
            fprog (native-bytes arena (doto ^ByteBuffer (le 16)
                                        (.putShort (short 1))
                                        (.position 8)
                                        (.putLong (.address ret-0))))
            call #(.invokeWithArguments ^MethodHandle %1 (object-array %&))]
        (try
          (and (<= 0 (long fd))
               (= 0 (call setsockopt fd (int 1) (int 15) one (int 4)))
               (= 0 (call bind fd sockaddr (int 16)))
               (= 0 (call setsockopt fd (int 1) (int 51) fprog (int 16))))
          (finally
            (when (<= 0 (long fd))
              (call close fd))))))))

(defn- linux? []
  (str/starts-with? (System/getProperty "os.name") "Linux"))

(deftest new-connections-spread-evenly-test
  (testing "connections opened one after another split evenly over 2 workers"
    (let [{:keys [server] :as ctx} (start-holding-server 2)]
      (try
        (let [held (mapv #(open-held! ctx %) (range 4))]
          (is (= [2 2] (worker-counts server)))
          (doseq [[id h] (map-indexed vector held)] (finish! ctx id h)))
        (is (= [0 0] (wait-for-counts server #(= [0 0] %))))
        (let [held (mapv #(open-held! ctx %) (range 10 18))]
          (is (= [4 4] (worker-counts server)))
          (doseq [[idx h] (map-indexed vector held)] (finish! ctx (+ 10 idx) h)))
        (finally
          (busker/stop! server))))))

(deftest closed-connections-leave-the-count-test
  (testing "a worker whose connections closed takes the next ones"
    (let [{:keys [server] :as ctx} (start-holding-server 2)]
      (try
        (let [held (mapv #(open-held! ctx %) (range 4))
              on-worker (group-by (comp :worker second) (map-indexed vector held))]
          (is (= [2 2] (worker-counts server)))
          (doseq [[id h] (get on-worker 0)] (finish! ctx id h))
          (is (= [0 2] (wait-for-counts server #(= [0 2] %))))
          (let [more (mapv #(open-held! ctx %) (range 4 6))]
            (is (= [2 2] (worker-counts server)))
            (doseq [[id h] (concat (get on-worker 1) (map vector (range 4 6) more))]
              (finish! ctx id h))))
        (finally
          (busker/stop! server))))))

(deftest moved-connections-work-through-forwarding-test
  (when (linux?)
    (testing "with every datagram delivered to worker 0, worker 1 still serves
    half of the connections, whose packets all reach it by forwarding"
      (let [{:keys [server port] :as ctx} (start-holding-server 2)]
        (try
          (if-not (force-udp-group-to-first-socket! port)
            (println "SKIP moved-connections-work-through-forwarding-test:"
                     "the kernel refused the reuseport BPF program")
            (let [held (mapv #(open-held! ctx %) (range 4))]
              (is (= [2 2] (worker-counts server)))
              (is (= [0 1 0 1] (mapv :worker held)))
              (doseq [[id h] (map-indexed vector held)] (finish! ctx id h))))
          (finally
            (busker/stop! server)))))))

(deftest withdraw-skips-freed-contexts-test
  (testing "a worker failure reported after its HTTP/3 contexts were freed
  does not reach them"
    (let [guard (#'generation/http3-ctxs-guard)
          withdraw! #'generation/withdraw-http3-worker!
          calls (atom 0)
          ctx (MemorySegment/ofAddress 4096)]
      (with-redefs [h2o/http3-withdraw-worker (fn [_] (swap! calls inc))]
        (withdraw! guard [ctx])
        (is (= 1 @calls))
        (.set ^java.util.concurrent.atomic.AtomicBoolean (:disposed? guard) true)
        (withdraw! guard [ctx])
        (is (= 1 @calls))))))

(defn- open-moved!
  "Opens HTTP/3 connections `ids` with every datagram delivered to worker 0,
  so worker 1 serves every second one by forwarding, and waits until worker 1
  has a connected socket for each of those."
  [{:keys [server] :as ctx} ids]
  (let [held (mapv #(open-held! ctx %) ids)
        moved (count (filter #(= 1 (:worker %)) held))]
    (is (= (vec (take (count ids) (cycle [0 1]))) (mapv :worker held)))
    (is (= [0 moved] (wait-until #(connected-sockets (active-workers server))
                                 #(= [0 moved] %))))
    held))

(deftest moved-connections-get-connected-sockets-test
  (when (linux?)
    (testing "once a moved connection has a connected socket, worker 0
    forwards none of its datagrams, and closing the connection closes the
    socket"
      (let [{:keys [server port] :as ctx} (start-holding-server 2)]
        (try
          (if-not (force-udp-group-to-first-socket! port)
            (println "SKIP moved-connections-get-connected-sockets-test:"
                     "the kernel refused the reuseport BPF program")
            (let [held (open-moved! ctx (range 4))
                  workers (active-workers server)
                  [forwarded-before] (settled #(forwarded-datagrams workers))]
              (is (pos? forwarded-before) "the handshakes were forwarded")
              (is (= {:bound 4 :connected 2} (udp-sockets-on-port port)))
              (doseq [[id h] (map-indexed vector held)] (finish! ctx id h))
              (is (= [0 0] (wait-until #(connected-sockets workers) #(= [0 0] %))))
              (is (= [0 0] (wait-for-counts server #(= [0 0] %))))
              (is (= forwarded-before (first (forwarded-datagrams workers)))
                  "worker 0 forwarded nothing after the connect")
              (is (= {:bound 2 :connected 0} (udp-sockets-on-port port)))))
          (finally
            (busker/stop! server)))))))

(deftest connected-sockets-do-not-outlive-connections-test
  (when (linux?)
    (testing "rounds of moved connections and many short ones leave only the
    listener's sockets"
      (let [{:keys [server port] :as ctx} (start-holding-server 2)]
        (try
          (if-not (force-udp-group-to-first-socket! port)
            (println "SKIP connected-sockets-do-not-outlive-connections-test:"
                     "the kernel refused the reuseport BPF program")
            (let [workers (active-workers server)]
              (doseq [round (range 3)
                      :let [ids (range (* 10 round) (+ 4 (* 10 round)))
                            held (open-moved! ctx ids)]]
                (doseq [[id h] (map vector ids held)] (finish! ctx id h))
                (is (= [0 0] (wait-for-counts server #(= [0 0] %))))
                (is (= [0 0] (wait-until #(connected-sockets workers) #(= [0 0] %))))
                (is (= {:bound 2 :connected 0} (udp-sockets-on-port port))))
              (dotimes [idx 20]
                (let [result (util/curl :https :h3 port "/ready" :max-time 5)]
                  (is (zero? (:exit result)) (str "short request " idx ": " (:err result)))))
              (is (= [0 0] (wait-for-counts server #(= [0 0] %))))
              (is (= [0 0] (wait-until #(connected-sockets workers) #(= [0 0] %))))
              (is (= {:bound 2 :connected 0} (udp-sockets-on-port port)))))
          (finally
            (busker/stop! server)))))))

(deftest reload-with-connected-sockets-test
  (when (linux?)
    (testing "moved connections keep their connected sockets while the old
    generation drains, and its retirement leaves only the new generation's
    sockets"
      (let [{:keys [server port config] :as ctx} (start-holding-server 2)]
        (try
          (if-not (force-udp-group-to-first-socket! port)
            (println "SKIP reload-with-connected-sockets-test:"
                     "the kernel refused the reuseport BPF program")
            (let [held (open-moved! ctx (range 4))
                  old-workers (active-workers server)]
              (is (= :activated (busker/reload! server config {:force? true})))
              (is (= {:bound 6 :connected 2} (udp-sockets-on-port port)))
              (let [result (util/curl :https :h3 port "/ready" :max-time 5)]
                (is (zero? (:exit result)) (:err result))
                (is (= "ready" (:out result))))
              (is (= [0 2] (connected-sockets old-workers)))
              (doseq [[id h] (map-indexed vector held)] (finish! ctx id h))
              (is (empty? (wait-until #(:draining @(:busker/state server)) empty?)))
              (is (= [0 0] (connected-sockets old-workers)))
              (is (= {:bound 2 :connected 0}
                     (wait-until #(udp-sockets-on-port port)
                                 #(= {:bound 2 :connected 0} %))))))
          (finally
            (busker/stop! server)))))))
