(ns ol.busker.callback-dispatch-test
  (:require
   [clojure.test :refer [deftest is]]
   [coffi.mem :as mem]
   [ol.busker.callback-dispatch :as callback-dispatch]
   [ol.busker.native :as h2o]))

(deftest stable-pointers-and-nonrepeating-identities-test
  (with-open [arena (mem/shared-arena)]
    (let [dispatch (callback-dispatch/create arena)
          _ (callback-dispatch/bind-thread! dispatch (Thread/currentThread))
          pointers (callback-dispatch/callback-pointers dispatch)
          first-identity (callback-dispatch/allocate-identity! dispatch)
          second-identity (callback-dispatch/allocate-identity! dispatch)]
      (is (= pointers (callback-dispatch/callback-pointers dispatch)))
      (is (= #{:body :proceed :stop} (set (keys pointers))))
      (is (every? some? (vals pointers)))
      (is (= (:module-id dispatch) (first first-identity) (first second-identity)))
      (is (= [(inc (second first-identity))]
             [(second second-identity)])))))

(deftest scalar-identity-retirement-rejects-stale-and-wrong-thread-entries-test
  (with-open [arena (mem/shared-arena)]
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
  (with-open [arena (mem/shared-arena)]
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
         (mem/size-of ::h2o/clj-req-ctx-t))))
