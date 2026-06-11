(ns busker-demo.test-runner
  (:require
   [busker-demo.main-test]
   [clojure.test :as test]))

(defn -main
  [& _]
  (let [{:keys [fail error]} (test/run-tests 'busker-demo.main-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
