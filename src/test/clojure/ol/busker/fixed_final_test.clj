(ns ol.busker.fixed-final-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.string :as str]
   [babashka.ffi :as mem]
   [ol.busker :as busker]
   [ol.busker.fixed-final :as fixed-final]
   [ol.busker.generation :as generation]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.response :as response]
   [ol.busker.test-utils :as util])
  (:import
   [java.lang.foreign MemorySegment ValueLayout]
   [java.security MessageDigest]
   [java.net InetSocketAddress Socket]
   [java.nio ByteBuffer]
   [java.nio.charset StandardCharsets]
   [java.util.concurrent CountDownLatch TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicReference]
   [java.util.concurrent.locks ReentrantReadWriteLock]))

(defn- direct-plan
  [module-id request-seq status headers content-length compress-hint body body-length]
  (fixed-final/direct-response-plan (long module-id) (long request-seq)
                                    (long status) headers
                                    (fixed-final/header-utf8-bytes headers) body
                                    (long body-length) (long content-length)
                                    (long compress-hint)))

(defn- direct-response-worker
  [receiver response-slots response-slot-payload-capacity]
  {:response-receiver_             (AtomicReference. receiver)
   :response-receiver-open?_       (AtomicBoolean. true)
   :response-receiver-lock         (ReentrantReadWriteLock.)
   :response-slot-monitor          (Object.)
   :response-slots                 response-slots
   :response-slot-payload-capacity response-slot-payload-capacity})

(defn- unary-long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OL] []
    (invokePrim [value] (long (f value)))
    (invoke [value] (f value))))

(defn- receiver-handle->long-fn
  [f]
  (proxy [clojure.lang.AFn clojure.lang.IFn$OLL] []
    (invokePrim [receiver claim-handle] (long (f receiver claim-handle)))
    (invoke [receiver claim-handle] (f receiver claim-handle))))

;; Slot contents that write-direct-response-slot! produces for these responses,
;; saved as literals. The slot is filled with 0xAA
;; first, so every byte the write touches shows. Descriptor pointers are offsets
;; from the payload start; the payload window ends two bytes after the payload.
(def ^:private pinned-data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t))
(def ^:private pinned-headers-offset (- pinned-data-size (* 64 32)))
(def ^:private pinned-capacity (+ fixed-final/max-header-staging-bytes fixed-final/response-ring-max-body-bytes))

(defn- sha256 [^bytes bs]
  (apply str (map #(format "%02x" (bit-and % 0xff)) (.digest (MessageDigest/getInstance "SHA-256") bs))))

(defn- slot-capture [response]
  (with-open [arena (mem/confined-arena)]
    (let [[head plan] (#'response/ordinary-fixed-plan
                       {:dispatch-module-id 7 :dispatch-request-seq 11
                        :config {:output-buffer-size 32768}}
                       response)
          ^MemorySegment slot (mem/alloc arena (+ pinned-data-size pinned-capacity 1))
          _ (.fill slot (unchecked-byte 0xAA))
          _ (fixed-final/write-direct-response-slot! slot pinned-capacity plan)
          payload-base (+ (.address slot) pinned-data-size)
          i64 #(.get slot ValueLayout/JAVA_LONG_UNALIGNED (long %))
          i32 #(.get slot ValueLayout/JAVA_INT_UNALIGNED (long %))
          n (i64 24)
          payload-len (i64 56)
          relative #(let [p (i64 %)] (if (zero? p) 0 (- p payload-base)))
          used (mapv (fn [i]
                       (let [d (+ pinned-headers-offset (* 32 i))]
                         [(relative d) (i64 (+ d 8)) (relative (+ d 16)) (i64 (+ d 24))]))
                     (range n))
          unused (.toArray (.asSlice slot (+ pinned-headers-offset (* 32 n)) (* 32 (- 64 n)))
                           ValueLayout/JAVA_BYTE)
          window (+ payload-len 2)
          payload (.toArray (.asSlice slot pinned-data-size window) ValueLayout/JAVA_BYTE)]
      {:head (select-keys head [:status :content-length :compress-hint])
       :fields {:claim-token (i64 0) :module-id (i64 8) :request-seq (i64 16)
                :headers-len n :content-length (i64 32) :body-offset (i64 40)
                :body-len (i64 48) :payload-len payload-len
                :status (i32 64) :compress-hint (i32 68)}
       :descriptors used
       :unused-descriptors-sha256 (sha256 unused)
       :payload-window window
       :payload-sha256 (sha256 payload)})))

(def ^:private pinned-responses
  [[:benchmark {:status 200 :headers {"content-type" "text/plain"} :body "Hello, World!"}]
   [:twelve-headers {:status 200
                     :headers (apply array-map
                                     (concat (mapcat (fn [i] [(str "x-header-" i) (str "value number " i)])
                                                     (range 11))
                                             ["content-type" "text/plain"]))
                     :body (apply str (repeat 2048 "a"))}]
   [:byte-array {:status 201 :headers {"content-type" "application/octet-stream"}
                 :body (byte-array (range -128 128))}]
   [:non-ascii {:status 200 :headers {"x-name" "café ☕"}
                :body (str "héllo 漢字 😀 " (char 0xd800) "x" (char 0xdc00) " end")}]
   [:empty-body {:status 200 :headers {"x-a" "b"} :body ""}]
   [:reset-content {:status 205 :headers {"x-a" "b"} :body ""}]
   [:sequential {:status 200 :headers {"x-multi" ["a" "b" "c"] "x-one" "1"} :body "s"}]
   [:pair-limit {:status 200 :headers {"x-repeat" (repeat 64 "v")} :body "p"}]
   [:staging-limit {:status 200 :headers {"x-large" (apply str (repeat 16343 "x"))} :body "ok"}]])

(def ^:private pinned-slot-contents
  {:benchmark
   {:head {:status 200, :content-length 13, :compress-hint 2},
    :fields
    {:payload-len 35,
     :module-id 7,
     :headers-len 1,
     :content-length 13,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 13,
     :body-offset 22,
     :claim-token -6148914691236517206},
    :descriptors [[0 12 12 10]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 37,
    :payload-sha256
    "b4bf75407b1c4a895bec3cc01df0e49f4f187071c7f4107bf7dff6710398a7f9"},
   :byte-array
   {:head {:status 201, :content-length 256, :compress-hint 2},
    :fields
    {:payload-len 292,
     :module-id 7,
     :headers-len 1,
     :content-length 256,
     :status 201,
     :request-seq 11,
     :compress-hint 2,
     :body-len 256,
     :body-offset 36,
     :claim-token -6148914691236517206},
    :descriptors [[0 12 12 24]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 294,
    :payload-sha256
    "57c95116884b45498fb9c638b1fca6dfffb550e0ac53e065c73723830f0a7c3d"},
   :empty-body
   {:head {:status 200, :content-length 0, :compress-hint 2},
    :fields
    {:payload-len 4,
     :module-id 7,
     :headers-len 1,
     :content-length 0,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 0,
     :body-offset 4,
     :claim-token -6148914691236517206},
    :descriptors [[0 3 3 1]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 6,
    :payload-sha256
    "6ce9c7a44fb0817d707490147762918079d04fb920452ad720cb7625995f4953"},
   :non-ascii
   {:head {:status 200, :content-length 26, :compress-hint 2},
    :fields
    {:payload-len 41,
     :module-id 7,
     :headers-len 1,
     :content-length 26,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 26,
     :body-offset 15,
     :claim-token -6148914691236517206},
    :descriptors [[0 6 6 9]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 43,
    :payload-sha256
    "17c34e44fe4ac8d78aef0e69e223b31b3d8a3152d8a828e74e1265b44d13932f"},
   :pair-limit
   {:head {:status 200, :content-length 1, :compress-hint 2},
    :fields
    {:payload-len 577,
     :module-id 7,
     :headers-len 64,
     :content-length 1,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 1,
     :body-offset 576,
     :claim-token -6148914691236517206},
    :descriptors
    [[0 8 8 1]
     [9 8 17 1]
     [18 8 26 1]
     [27 8 35 1]
     [36 8 44 1]
     [45 8 53 1]
     [54 8 62 1]
     [63 8 71 1]
     [72 8 80 1]
     [81 8 89 1]
     [90 8 98 1]
     [99 8 107 1]
     [108 8 116 1]
     [117 8 125 1]
     [126 8 134 1]
     [135 8 143 1]
     [144 8 152 1]
     [153 8 161 1]
     [162 8 170 1]
     [171 8 179 1]
     [180 8 188 1]
     [189 8 197 1]
     [198 8 206 1]
     [207 8 215 1]
     [216 8 224 1]
     [225 8 233 1]
     [234 8 242 1]
     [243 8 251 1]
     [252 8 260 1]
     [261 8 269 1]
     [270 8 278 1]
     [279 8 287 1]
     [288 8 296 1]
     [297 8 305 1]
     [306 8 314 1]
     [315 8 323 1]
     [324 8 332 1]
     [333 8 341 1]
     [342 8 350 1]
     [351 8 359 1]
     [360 8 368 1]
     [369 8 377 1]
     [378 8 386 1]
     [387 8 395 1]
     [396 8 404 1]
     [405 8 413 1]
     [414 8 422 1]
     [423 8 431 1]
     [432 8 440 1]
     [441 8 449 1]
     [450 8 458 1]
     [459 8 467 1]
     [468 8 476 1]
     [477 8 485 1]
     [486 8 494 1]
     [495 8 503 1]
     [504 8 512 1]
     [513 8 521 1]
     [522 8 530 1]
     [531 8 539 1]
     [540 8 548 1]
     [549 8 557 1]
     [558 8 566 1]
     [567 8 575 1]],
    :unused-descriptors-sha256
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    :payload-window 579,
    :payload-sha256
    "3c5b39c5b53a00a441178a10f683a3b86e7025195c7e27f916725878b6a99e64"},
   :reset-content
   {:head {:status 205, :content-length 0, :compress-hint 2},
    :fields
    {:payload-len 4,
     :module-id 7,
     :headers-len 1,
     :content-length 0,
     :status 205,
     :request-seq 11,
     :compress-hint 2,
     :body-len 0,
     :body-offset 4,
     :claim-token -6148914691236517206},
    :descriptors [[0 3 3 1]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 6,
    :payload-sha256
    "6ce9c7a44fb0817d707490147762918079d04fb920452ad720cb7625995f4953"},
   :sequential
   {:head {:status 200, :content-length 1, :compress-hint 2},
    :fields
    {:payload-len 31,
     :module-id 7,
     :headers-len 4,
     :content-length 1,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 1,
     :body-offset 30,
     :claim-token -6148914691236517206},
    :descriptors [[0 7 7 1] [8 7 15 1] [16 7 23 1] [24 5 29 1]],
    :unused-descriptors-sha256
    "3f15590ce1d67c8f24e08a1a0374398d90790586663c7b741470ff761a3d5b68",
    :payload-window 33,
    :payload-sha256
    "65bcbd24721572160453de282741dad18d567deead4ec5c570be1004f61a11a7"},
   :staging-limit
   {:head {:status 200, :content-length 2, :compress-hint 2},
    :fields
    {:payload-len 16352,
     :module-id 7,
     :headers-len 1,
     :content-length 2,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 2,
     :body-offset 16350,
     :claim-token -6148914691236517206},
    :descriptors [[0 7 7 16343]],
    :unused-descriptors-sha256
    "bab34e6c6d17c9914821a70b04e88ecb889669ac9ba99549d3c446eaf39534b2",
    :payload-window 16354,
    :payload-sha256
    "8975dfecc5e147892952a023c33d708b2aa73815863cf98b73b5ea1de64f9cb0"},
   :twelve-headers
   {:head {:status 200, :content-length 2048, :compress-hint 2},
    :fields
    {:payload-len 2336,
     :module-id 7,
     :headers-len 12,
     :content-length 2048,
     :status 200,
     :request-seq 11,
     :compress-hint 2,
     :body-len 2048,
     :body-offset 288,
     :claim-token -6148914691236517206},
    :descriptors
    [[0 10 10 14]
     [24 10 34 14]
     [48 10 58 14]
     [72 10 82 14]
     [96 10 106 14]
     [120 10 130 14]
     [144 10 154 14]
     [168 10 178 14]
     [192 10 202 14]
     [216 10 226 14]
     [240 11 251 15]
     [266 12 278 10]],
    :unused-descriptors-sha256
    "693b529d984a7d206f416d6ed14c4d17cba0559782adc4e31f5d3adc8f95d0e2",
    :payload-window 2338,
    :payload-sha256
    "695b631fc7603d0e560f568c88dc255caaf659fa2461fa358536f0c9695a7580"}})

(deftest slot-field-offsets-match-the-layout-test
  (with-open [arena (mem/confined-arena)]
    (let [layout h2o/ffi-clj-fixed-response-slot-data-t
          slot (mem/alloc arena (mem/sizeof layout))
          offset-of (fn [field]
                      (.fill ^MemorySegment slot (byte 0))
                      (mem/write slot (mem/place layout field) 1)
                      (first (keep-indexed (fn [i b] (when-not (zero? b) i))
                                           (mem/read-array slot :byte (mem/sizeof layout)))))]
      (is (= {:module-id fixed-final/slot-module-id-offset
              :request-seq fixed-final/slot-request-seq-offset
              :headers-len fixed-final/slot-headers-len-offset
              :content-length fixed-final/slot-content-length-offset
              :body-offset fixed-final/slot-body-offset-offset
              :body-len fixed-final/slot-body-len-offset
              :payload-len fixed-final/slot-payload-len-offset
              :status fixed-final/slot-status-offset
              :compress-hint fixed-final/slot-compress-hint-offset}
             (into {} (map (juxt identity offset-of))
                   [:module-id :request-seq :headers-len :content-length :body-offset
                    :body-len :payload-len :status :compress-hint]))))))

(deftest fixed-response-slot-contents-are-pinned-test
  (doseq [[k response] pinned-responses]
    (is (= (get pinned-slot-contents k) (slot-capture response)) (name k))))

(deftest response-slot-contains-packed-metadata-headers-and-body
  (with-open [arena (mem/confined-arena)]
    (let [body (.getBytes "cat" StandardCharsets/UTF_8)
          headers [["content-type" "text/plain"]]
          data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size 65))
          _ (fixed-final/write-direct-response-slot!
             slot 64 (direct-plan 1 1 200 headers 3 2 body 3))
          data (mem/read (mem/slice slot 0 data-size)
                         h2o/ffi-clj-fixed-response-slot-data-t)
          staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                 [(h2o/->string name name_len)
                                  (h2o/->string value value_len)])
                               (take (:headers-len data) (:headers data)))
          payload (mem/read-array (mem/slice slot data-size 64)
                                  :byte (long (:payload-len data)))]
      (is (= {:module-id 1
              :claim-token 0
              :request-seq 1
              :headers-len 1
              :content-length 3
              :body-offset 22
              :body-len 3
              :payload-len 25
              :status 200
              :compress-hint 2
              :headers [["content-type" "text/plain"]]
              :payload "content-typetext/plaincat"}
             (assoc (dissoc data :headers)
                    :headers staged-headers
                    :payload (String. payload StandardCharsets/UTF_8)))))))

(deftest direct-response-slot-enforces-exact-header-and-payload-limits
  (with-open [arena (mem/confined-arena)]
    (let [headers (mapv (fn [index] [(str "x" index) "v"]) (range fixed-final/max-header-pairs))
          body (byte-array [1 2 3])
          header-bytes (fixed-final/header-utf8-bytes headers)
          payload-capacity (+ header-bytes (alength body))
          data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size (inc payload-capacity)))
          plan (direct-plan 1 1 200 headers (alength body) 0 body (alength body))]
      (fixed-final/write-direct-response-slot! slot payload-capacity plan)
      (let [data (mem/read (mem/slice slot 0 data-size)
                           h2o/ffi-clj-fixed-response-slot-data-t)
            payload-start (.address ^MemorySegment (mem/slice slot data-size (inc payload-capacity)))
            payload-end (+ payload-start payload-capacity)
            descriptors (take (:headers-len data) (:headers data))]
        (is (= fixed-final/max-header-pairs (:headers-len data)))
        (is (= payload-capacity (:payload-len data)))
        (is (every? (fn [{:keys [name name_len value value_len]}]
                      (and (<= payload-start (.address ^MemorySegment name) payload-end)
                           (<= name_len (- payload-end (.address ^MemorySegment name)))
                           (<= payload-start (.address ^MemorySegment value) payload-end)
                           (<= value_len (- payload-end (.address ^MemorySegment value)))))
                    descriptors)))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"does not fit"
                            (fixed-final/write-direct-response-slot!
                             slot payload-capacity
                             (direct-plan 1 1 200 (conj headers ["overflow" "v"])
                                          (alength body) 0 body (alength body))))))))
(deftest direct-response-slot-utf8-matches-java-encoding
  (doseq [body ["ascii" "λ" "🐈" (str (char 0xd800) "x") (str "x" (char 0xdc00))]]
    (with-open [arena (mem/confined-arena)]
      (let [expected (.getBytes ^String body StandardCharsets/UTF_8)
            headers [["x-🐈" "välue"]]
            header-bytes (fixed-final/header-utf8-bytes headers)
            data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
            slot (mem/alloc arena (+ data-size 129))]
        (fixed-final/write-direct-response-slot!
         slot 128 (direct-plan 7 9 201 headers (alength expected) 1 body
                               (alength expected)))
        (let [data (mem/read (mem/slice slot 0 data-size)
                             h2o/ffi-clj-fixed-response-slot-data-t)
              staged-headers (mapv (fn [{:keys [name name_len value value_len]}]
                                     [(h2o/->string name name_len)
                                      (h2o/->string value value_len)])
                                   (take (:headers-len data) (:headers data)))
              payload (mem/read-array (mem/slice slot data-size 128)
                                      :byte (long (:payload-len data)))]
          (is (= {:module-id 7
                  :request-seq 9
                  :headers-len 1
                  :content-length (alength expected)
                  :body-offset header-bytes
                  :payload-len (+ header-bytes (alength expected))
                  :status 201
                  :compress-hint 1}
                 (select-keys data [:module-id :request-seq :headers-len
                                    :content-length :body-offset :payload-len
                                    :status :compress-hint])))
          (is (= headers staged-headers))
          (is (= (alength expected) (:body-len data)))
          (is (= (vec expected)
                 (vec (drop header-bytes payload)))))))))
(deftest direct-string-body-uses-hidden-terminator-byte-at-logical-capacity
  (with-open [arena (mem/confined-arena)]
    (let [data-size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ data-size 4))]
      (fixed-final/write-direct-response-slot!
       slot 3 (direct-plan 1 1 200 [] 3 0 "cat" 3))
      (is (= [99 97 116 0]
             (vec (mem/read-array (mem/slice slot data-size 4) :byte 4)))))))

(deftest full-response-ring-does-not-pack-direct-response
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          worker (direct-response-worker receiver [] 64)
          writes_ (atom 0)]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim (unary-long-fn (constantly 0))
                    fixed-final/write-direct-response-slot!
                    (fn [& _] (swap! writes_ inc))]
        (is (= :overloaded
               (fixed-final/publish-direct-producer!
                worker (direct-plan 1 1 200 [] -1 0 nil 0) 1
                (fn [_] (swap! writes_ inc))
                false (constantly false) (fn [publish!] (publish!)))))
        (is (zero? @writes_))))))

(deftest direct-response-slot-is-aborted-when-packing-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena h2o/ffi-clj-fixed-response-slot-data-t)
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish (receiver-handle->long-fn (constantly 1))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"does not fit"
                              (fixed-final/publish-direct-response!
                               worker (direct-plan 1 1 200 [] 1 0
                                                   (byte-array [1]) 1))))
        (is (= [[receiver claim-handle]] @aborted_))))))
(deftest direct-response-slot-is-aborted-when-publication-fails
  (with-open [arena (mem/confined-arena)]
    (let [receiver (mem/alloc arena 1)
          slot (mem/alloc arena (inc (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)))
          claim-handle 65537
          worker (direct-response-worker receiver [slot] 0)
          aborted_ (atom [])]
      (with-redefs [h2o/mt-response-use-acquire (constantly 1)
                    h2o/mt-response-use-release (fn [_])
                    h2o/mt-response-try-claim
                    (unary-long-fn (constantly claim-handle))
                    h2o/mt-response-publish
                    (receiver-handle->long-fn (constantly 0))
                    h2o/mt-response-abort
                    (receiver-handle->long-fn
                     (fn [actual-receiver actual-handle]
                       (swap! aborted_ conj [actual-receiver actual-handle])
                       1))]
        (is (thrown-with-msg? IllegalStateException
                              #"publication failed"
                              (fixed-final/publish-direct-response!
                               worker (direct-plan 1 1 200 [] 0 0 (byte-array 0) 0))))
        (is (= [[receiver claim-handle]] @aborted_))))))

(deftest producer-view-expires-without-invalidating-slot-storage
  (with-open [arena (mem/shared-arena)]
    (let [size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ size 65))
          aliases_ (atom [])
          foreign-result (promise)
          caller_ (atom nil)
          result (promise)
          producer (fn [^ByteBuffer buffer]
                     (reset! caller_ (Thread/currentThread))
                     (reset! aliases_ [buffer (.slice buffer) (.duplicate buffer)])
                     (let [thread (Thread/startVirtualThread
                                   #(deliver foreign-result
                                             (try (.put buffer 0 (byte 99)) :wrote
                                                  (catch java.lang.WrongThreadException _ :rejected))))]
                       (.join thread))
                     (.put buffer (byte-array [99 97 116])))
          thread (Thread/startVirtualThread
                  #(deliver result
                            (try
                              (#'fixed-final/write-direct-producer-slot!
                               slot 64 (direct-plan 1 1 200 [] -1 0 nil 0) 32 producer)
                              {:status :prepared
                               :aliases (mapv (fn [^ByteBuffer buffer]
                                                (try (.get buffer 0) :readable
                                                     (catch IllegalStateException _ :expired)))
                                              @aliases_)}
                              (catch Throwable error error))))]
      (.join thread 2000)
      (is (= {:status :prepared :aliases [:expired :expired :expired]}
             (deref result 1000 :timeout)))
      (is (= :rejected (deref foreign-result 1000 :timeout)))
      (is (identical? thread @caller_))
      (is (= {:body-len 3 :payload-len 3 :content-length 3}
             (select-keys (mem/read slot h2o/ffi-clj-fixed-response-slot-data-t)
                          [:body-len :payload-len :content-length])))
      (is (= [99 97 116] (vec (mem/read-array (mem/slice slot size 3) :byte 3)))))))

(deftest producer-exception-closes-alias-and-preserves-explicit-content-length
  (with-open [arena (mem/shared-arena)]
    (let [size (mem/sizeof h2o/ffi-clj-fixed-response-slot-data-t)
          slot (mem/alloc arena (+ size 65))
          alias_ (atom nil)
          plan (direct-plan 1 1 200 [] 7 0 nil 0)]
      (is (thrown-with-msg? Exception #"producer failed"
                            (#'fixed-final/write-direct-producer-slot!
                             slot 64 plan 16
                             (fn [buffer]
                               (reset! alias_ buffer)
                               (throw (Exception. "producer failed"))))))
      (is (thrown? IllegalStateException (.get ^ByteBuffer @alias_ 0)))
      (#'fixed-final/write-direct-producer-slot!
       slot 64 plan 16 (fn [^ByteBuffer buffer] (.put buffer (byte 1))))
      (is (= {:body-len 1 :content-length 7}
             (select-keys (mem/read slot h2o/ffi-clj-fixed-response-slot-data-t)
                          [:body-len :content-length]))))))

(deftest accepted-fixed-publication-survives-wake-failure
  (let [worker {:response-slot-monitor (Object.)
                :response-receiver-open?_ (AtomicBoolean. true)
                :response-slots [:slot]
                :response-slot-payload-capacity 64}
        released?_ (atom false)]
    (with-redefs [h2o/mt-response-publish (receiver-handle->long-fn (constantly 1))
                  h2o/mt-response-abort (fn [& _] (throw (AssertionError. "published slot aborted")))
                  h2o/mt-response-use-release (fn [_] (reset! released?_ true))
                  pi/wake (fn [_] (throw (Exception. "wake failed")))]
      (is (= :accepted
             (#'fixed-final/prepare-and-publish!
              worker [:receiver 65537] (constantly false) (fn [& _])
              (fn [publish!] (publish!))))))
    (is (true? @released?_))))

(deftest fixed-producer-rejection-skips-callback
  (doseq [rejection [:closed :overloaded]]
    (with-redefs-fn
      {#'fixed-final/claim-response! (fn [& _] rejection)}
      #(is (= rejection
              (fixed-final/publish-direct-producer!
               {:response-slot-payload-capacity 64}
               (direct-plan 1 1 200 [] -1 0 nil 0) 16
               (fn [_] (throw (AssertionError. "rejected producer invoked")))
               false (constantly false)
               (fn [_] (throw (AssertionError. "rejected response accepted")))))))))

(deftest fixed-capacity-wait-rechecks-release-and-cancellation
  (doseq [cancel? [false true]]
    (let [worker {:response-slot-monitor (Object.)}
          checked (CountDownLatch. 1)
          available?_ (atom false)
          cancelled?_ (atom false)
          result (promise)]
      (with-redefs-fn
        {#'fixed-final/claim-once!
         (fn [_]
           (if @available?_
             [:receiver 65537]
             (do (.countDown checked) :overloaded)))}
        (fn []
          (let [thread (Thread/startVirtualThread
                        #(deliver result
                                  (#'fixed-final/claim-response!
                                   worker true (fn [] @cancelled?_))))]
            (try
              (is (.await checked 2 TimeUnit/SECONDS))
              (if cancel? (reset! cancelled?_ true) (reset! available?_ true))
              (fixed-final/notify-capacity! worker)
              (is (= (if cancel? :closed [:receiver 65537])
                     (deref result 2000 :timeout)))
              (finally
                (.interrupt thread)
                (.join thread 2000)))))))))

(defn- counted-slot-worker []
  {:response-slot-monitor (Object.)
   :response-slot-waiters (AtomicInteger.)
   :response-receiver-open?_ (AtomicBoolean. true)})

(defn- await-count
  [^AtomicInteger counter ^long expected]
  (let [deadline (+ (System/nanoTime) (* 2000 1000000))]
    (loop []
      (cond
        (>= (.get counter) expected) true
        (> (System/nanoTime) deadline) false
        :else (do (Thread/onSpinWait) (recur))))))

(deftest freed-slots-reach-every-counted-waiter-test
  (dotimes [round 300]
    (let [worker (counted-slot-worker)
          free (AtomicInteger. 0)
          done (AtomicInteger. 0)
          n-waiters 4]
      (with-redefs-fn
        {#'fixed-final/claim-once!
         (fn [_]
           (loop []
             (let [available (.get free)]
               (cond
                 (zero? available) :overloaded
                 (.compareAndSet free available (dec available)) [:receiver 65537]
                 :else (recur)))))}
        (fn []
          (let [threads (vec (repeatedly n-waiters
                                         #(Thread/startVirtualThread
                                           (fn []
                                             (#'fixed-final/claim-response!
                                              worker true (constantly false))
                                             (.incrementAndGet done)))))]
            (try
              (when (odd? round) (Thread/sleep 1))
              (doseq [freed (range 1 (inc n-waiters))]
                (.incrementAndGet free)
                (fixed-final/notify-capacity! worker)
                (is (await-count done freed)
                    (str "round " round ": " (.get done) " of " freed " waiters proceeded")))
              (is (zero? (.get ^AtomicInteger (:response-slot-waiters worker))))
              (finally
                (doseq [^Thread thread threads]
                  (.interrupt thread)
                  (.join thread 2000))))))))))

(deftest closing-admission-wakes-counted-waiters-test
  (let [worker (counted-slot-worker)
        open?_ ^AtomicBoolean (:response-receiver-open?_ worker)
        results (vec (repeatedly 4 promise))]
    (with-redefs-fn
      {#'fixed-final/claim-once! (fn [_] (if (.get open?_) :overloaded :closed))}
      (fn []
        (let [threads (mapv (fn [result]
                              (Thread/startVirtualThread
                               #(deliver result (#'fixed-final/claim-response!
                                                 worker true (constantly false)))))
                            results)]
          (try
            (is (await-count (:response-slot-waiters worker) 4))
            (#'generation/close-response-receiver-admission! worker)
            (is (= [:closed :closed :closed :closed]
                   (mapv #(deref % 2000 :timeout) results)))
            (is (zero? (.get ^AtomicInteger (:response-slot-waiters worker))))
            (finally
              (doseq [^Thread thread threads]
                (.interrupt thread)
                (.join thread 2000)))))))))

(deftest capacity-notice-without-waiters-skips-the-monitor-test
  (let [worker (counted-slot-worker)
        monitor (:response-slot-monitor worker)
        held (CountDownLatch. 1)
        release (CountDownLatch. 1)
        holder (Thread/startVirtualThread
                #(locking monitor
                   (.countDown held)
                   (.await release)))]
    (try
      (is (.await held 2 TimeUnit/SECONDS))
      (is (= :returned (deref (future (fixed-final/notify-capacity! worker) :returned)
                              1000 :blocked)))
      (.incrementAndGet ^AtomicInteger (:response-slot-waiters worker))
      (let [notice (future (fixed-final/notify-capacity! worker) :returned)]
        (is (= :blocked (deref notice 200 :blocked)))
        (.countDown release)
        (is (= :returned (deref notice 2000 :blocked))))
      (finally
        (.countDown release)
        (.join holder 2000)))))

(defn- fixed-final-server
  []
  (let [port (util/free-port)
        server (busker/start!
                (util/with-handler
                  (constantly {:status 200 :body "generic"})
                  ;; One worker: accepting on a worker wakes the others from its
                  ;; event-loop thread, which the wake-thread assertions would see.
                  {:n-workers 1
                   :entrypoints {:http {:bind (str "127.0.0.1:" port)
                                        :http3? false
                                        :tls false}}}))]
    (util/wait-for-curl-ready! :http :h1 port "/" :max-time 2)
    [server port]))

(defn- request-text
  [port method]
  (with-open [socket (Socket.)]
    (.connect socket (InetSocketAddress. "127.0.0.1" (int port)) 2000)
    (let [out (.getOutputStream socket)
          request (str method " / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")]
      (.write out (.getBytes request StandardCharsets/US_ASCII))
      (.flush out)
      (slurp (.getInputStream socket)))))

(deftest response-ring-applies-live-response-from-a-virtual-thread
  (let [[server port] (fixed-final-server)
        claim-threads_ (atom [])
        wake-threads_ (atom [])
        drain-threads_ (atom [])
        try-claim h2o/mt-response-try-claim
        wake h2o/mt-wakeup
        drain h2o/mt-response-ring-drain]
    (try
      (with-redefs-fn
        {#'h2o/mt-response-try-claim
         (fn [receiver]
           (swap! claim-threads_ conj (Thread/currentThread))
           (try-claim receiver))
         #'h2o/mt-wakeup
         (fn [receiver]
           (swap! wake-threads_ conj (Thread/currentThread))
           (wake receiver))
         #'h2o/mt-response-ring-drain
         (fn [receiver]
           (swap! drain-threads_ conj (Thread/currentThread))
           (drain receiver))}
        #(let [http-response (request-text port "GET")]
           (is (.contains ^String http-response "generic"))))
      (is (seq @claim-threads_))
      (is (every? #(.isVirtual ^Thread %) @claim-threads_))
      (is (seq @wake-threads_))
      (is (every? #(.isVirtual ^Thread %) @wake-threads_))
      (is (seq @drain-threads_))
      (is (every? (fn [^Thread thread] (not (.isVirtual thread)))
                  @drain-threads_))
      (finally
        (busker/stop! server)))))
(deftest retained-fixed-response-suppresses-head-body
  (let [[server port] (fixed-final-server)]
    (try
      (is (= "generic" (second (str/split (request-text port "GET") #"\r\n\r\n" 2))))
      (is (= "" (second (str/split (request-text port "HEAD") #"\r\n\r\n" 2))))
      (finally
        (busker/stop! server)))))