(ns tempo.profile-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [tempo.profile :as profile]))

(deftest reads-live-linux-counters
  (let [snapshot (#'profile/counters)]
    (is (string? (get-in snapshot [:process :stat])))
    (is (re-find #"^\d+ \(" (get-in snapshot [:process :stat])))
    (is (seq (:threads snapshot)))
    (is (pos-int? (:captured-at-ms snapshot)))
    (is (or (nil? (:host-pressure snapshot)) (string? (:host-pressure snapshot))))))

(deftest rejects-unsupported-recordings-before-starting-a-server
  (testing "unsupported events must not silently select another event"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported recording event"
                          (#'profile/collect! {:event :unsupported}))))
  (testing "invalid workload settings cannot start a recording"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid profile workload settings"
                          (#'profile/collect! {:event :ctimer :protocol :h1 :warmup 0 :duration 1
                                               :connections 128 :streams 64 :threads 2})))))