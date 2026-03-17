(ns ol.busker.specs-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test :refer [deftest is testing]]
   [ol.busker.specs :as specs]))

(defn- descriptor-var?
  [k]
  (let [sym (symbol (name k))
        v (ns-resolve 'ol.busker.specs sym)]
    (when v
      (let [m (var-get v)]
        (and (map? m)
             (= k (:key m))
             (contains? m :doc)
             (contains? m :default))))))

(deftest every-spec-has-descriptor-map-test
  (testing "each Busker spec key has a descriptor map var with :key/:doc/:default"
    (let [busker-spec-ns (namespace ::specs/config)
          busker-spec-keys (->> (keys (s/registry))
                                (filter keyword?)
                                (filter #(= busker-spec-ns (namespace %)))
                                sort
                                vec)
          missing (->> busker-spec-keys
                       (remove descriptor-var?)
                       vec)]
      (is (empty? missing)
          (str "missing descriptor vars for keys: " missing)))))

(deftest managed-plan-spec-test
  (testing "managed plan conforms when using managed subject names"
    (let [plan {:subject-names ["example.com" "www.example.com"]
                :clave-config {:issuers [{:directory-url
                                          "https://acme.example/directory"}]}}]
      (is (s/valid? ::specs/managed-plan plan))))

  (testing "managed plan rejects empty subject names"
    (let [plan {:subject-names []
                :clave-config {:issuers [{:directory-url
                                          "https://acme.example/directory"}]}}]
      (is (not (s/valid? ::specs/managed-plan plan))))))
