(ns ol.busker.byte-bounded-queue-test
  (:require
   [clojure.test :refer [deftest is]]
   [ol.busker.byte-bounded-queue :as budget]))

(deftest pending-credit-includes-private-reservations
  (let [b (budget/byte-budget 8 (Object.))]
    (is (true? (budget/reserve! b 6)))
    (is (nil? (budget/reserve! b 3)))
    (is (= [6 2] [(budget/queued-bytes b) (budget/remaining-bytes b)]))
    (budget/release! b 2)
    (is (true? (budget/reserve! b 4)))
    (budget/release! b 8)
    (is (= [0 8] [(budget/queued-bytes b) (budget/remaining-bytes b)]))
    (is (thrown? IllegalStateException (budget/release! b 1)))))

(deftest invalid-credit-is-not-temporary-fullness
  (is (thrown? IllegalArgumentException (budget/byte-budget 0 (Object.))))
  (let [b (budget/byte-budget 8 (Object.))]
    (is (thrown? IllegalArgumentException (budget/reserve! b -1)))
    (is (thrown? IllegalArgumentException (budget/reserve! b 9)))
    (is (true? (budget/reserve! b 0)))
    (is (= 0 (budget/queued-bytes b)))))
