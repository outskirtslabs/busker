(ns ol.busker.callback-dispatch-test
  (:require
   [babashka.ffi :as ffi]
   [clojure.test :refer [deftest is]]
   [ol.busker.buffer-pool :as pool]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.internal.protocols :as pi]
   [ol.busker.native :as h2o]
   [ol.busker.protocols :as protocols]
   [ol.busker.response :as response]
   [ol.busker.response-queue :as queue])
  (:import
   [java.io IOException InputStream]
   [java.nio ByteBuffer]))

(deftest stable-pointers-and-nonrepeating-identities-test
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          pointers (callback-dispatch/callback-pointers dispatch)
          first-identity (callback-dispatch/allocate-identity! dispatch)
          second-identity (callback-dispatch/allocate-identity! dispatch)]
      (is (= pointers (callback-dispatch/callback-pointers dispatch)))
      (is (identical? pointers (callback-dispatch/callback-pointers dispatch)))
      (is (= #{:body :proceed :stop} (set (keys pointers))))
      (is (every? some? (vals pointers)))
      (is (= (:module-id dispatch) (first first-identity) (first second-identity)))
      (is (= [(inc (second first-identity))]
             [(second second-identity)])))))

(deftest empty-final-body-callback-preserves-null-and-empty-chunks-test
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          [module-id request-seq] (callback-dispatch/allocate-identity! dispatch)
          received (atom [])
          req {:write-req {:write-chunk (fn [chunk final?]
                                          (swap! received conj [chunk final?]))}}
          body (ffi/cfn (:body (callback-dispatch/callback-pointers dispatch))
                        [:long :long :pointer :long :int] :void)]
      (callback-dispatch/register! dispatch module-id request-seq req nil)
      (body module-id request-seq ffi/null 0 1)
      (body module-id request-seq (ffi/alloc arena 1) 0 1)
      (is (= 2 (count @received)))
      (is (= [nil true] (first @received)))
      (let [[chunk final?] (second @received)]
        (is (bytes? chunk))
        (is (zero? (alength ^bytes chunk)))
        (is (true? final?)))
      (is (zero? (:callback-fault (callback-dispatch/diagnostics dispatch)))))))

(deftest scalar-identity-retirement-rejects-stale-and-wrong-thread-entries-test
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          [module-id request-seq] (callback-dispatch/allocate-identity! dispatch)]
      (callback-dispatch/register! dispatch module-id request-seq nil nil)
      (is (= [nil nil]
             (callback-dispatch/entry dispatch module-id request-seq)))
      (is (nil? (callback-dispatch/entry dispatch (inc module-id) request-seq)))
      (is (nil? @(future (callback-dispatch/entry dispatch module-id request-seq))))
      (callback-dispatch/retire! dispatch module-id request-seq)
      (is (nil? (callback-dispatch/entry dispatch module-id request-seq)))
      (is (= {:malformed      1
              :late           1
              :wrong-thread   1
              :callback-fault 0
              :duplicate      0
              :retired        1
              :forced         0}
             (callback-dispatch/diagnostics dispatch))))))

(deftest duplicate-retirement-does-not-revive-an-entry-test
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          [module-id request-seq] (callback-dispatch/allocate-identity! dispatch)]
      (callback-dispatch/register! dispatch module-id request-seq nil nil)
      (callback-dispatch/retire! dispatch module-id request-seq)
      (callback-dispatch/retire! dispatch module-id request-seq)
      (is (callback-dispatch/no-live-entries? dispatch))
      (is (= {:malformed      0
              :late           0
              :wrong-thread   0
              :callback-fault 0
              :duplicate      1
              :retired        1
              :forced         0}
             (callback-dispatch/diagnostics dispatch))))))

(deftest request-context-layout-matches-the-shim-test
  (is (= (h2o/req-ctx-size)
         (ffi/sizeof h2o/ffi-clj-req-ctx-t))))

(deftest input-close-failure-does-not-suppress-response-cleanup-test
  (with-open [arena (ffi/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          module-id (:module-id dispatch)
          request-seq (callback-dispatch/allocate-identity! dispatch)
          p (pool/create-stream-pool 8)
          commands (atom [])
          input-closes (atom 0)
          input (proxy [InputStream] []
                  (read ([] -1) ([_ _ _] -1))
                  (close []
                    (swap! input-closes inc)
                    (throw (IOException. "input close failed"))))
          req {:config {:output-buffer-size 8}
               :write-req {:input-stream input}
               :worker {:stream-pool p
                        :reserve-command! (fn [_ _ _] (Object.))
                        :publish-command! (fn [_ _ command] (swap! commands conj command))
                        :release-command! (fn [_ _] nil)}}
          emitter (response/new-response-emitter (pi/map->Request req))
          closed (promise)]
      (try
        (with-redefs [pi/wake (fn [_] nil)
                      h2o/sendvec (fn [& _] nil)]
          (protocols/on-close! emitter #(deliver closed true))
          (let [writer (queue/create-response-writer (assoc req :apply-head! (fn [_ _] nil)))
                _ (reset! (.writer_ ^ol.busker.response.H2OResponseEmitter emitter) writer)
                result (queue/emit-bounded! writer nil 1
                                            (fn [^ByteBuffer b] (.put b (byte 1)))
                                            {:end? true})]
            (doseq [command @commands] (queue/consume-record! writer (peek command)))
            (queue/send-vecs writer)
            (is (= 1 (pool/checked-out p)) "The final send keeps its block until cleanup.")
            (callback-dispatch/register! dispatch module-id request-seq req emitter)
            (binding [*err* (java.io.StringWriter.)]
              (callback-dispatch/retire! dispatch module-id request-seq))
            (is (= 1 @input-closes))
            (is (= {:status :closed} @(:progress result)))
            (is (true? (deref closed 5000 false)))
            (is (false? (protocols/open? emitter)))
            (is (= 0 (pool/checked-out p)))))
        (finally (pool/retire! p))))))
