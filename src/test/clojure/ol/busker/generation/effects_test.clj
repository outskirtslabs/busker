(ns ol.busker.generation.effects-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.generation :as generation]
   [ol.busker.generation.effects :as effects]
   [ol.busker.listen :as listen]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign MemorySegment]))

(deftest an-h3-context-that-cannot-free-its-work-frees-only-the-other-contexts-test
  (let [[ptls-0 quicly-0 ptls-1 quicly-1 tag] (map #(MemorySegment/ofAddress %) [1 2 3 4 5])
        ptls_ (atom [ptls-0 ptls-1])
        quicly {ptls-0 quicly-0 ptls-1 quicly-1}
        freed_ (atom [])
        setup-error (ex-info "quic tag failure" {})
        free-error (ex-info "quicly free failure" {})
        error (with-redefs [h2o/http3-create-ptls-ctx (fn [& _]
                                                        (let [p (first @ptls_)]
                                                          (swap! ptls_ rest)
                                                          p))
                            h2o/ticket-manager-create-encrypt-ticket (constantly tag)
                            h2o/ptls-ctx-set-tickets (constantly 0)
                            h2o/http3-create-quicly-ctx (fn [p _] (quicly p))
                            h2o/ticket-manager-set-quic-tag (fn [_ q]
                                                              (when (= quicly-1 q) (throw setup-error)))
                            h2o/http3-free-quicly-ctx (fn [q]
                                                        (swap! freed_ conj q)
                                                        (when (= quicly-1 q) (throw free-error)))
                            h2o/http3-free-ptls-ctx #(swap! freed_ conj %)]
                (try
                  ((:acquire (:h3-contexts effects/table))
                   {::generation/config {:listeners [{:port 1 :tls {}} {:port 2 :tls {}}]}
                    ::generation/native-ticket-mgr tag}
                   nil)
                  nil
                  (catch Throwable t t)))]
    (is (identical? setup-error (effects/partial-acquire error)))
    (is (= [free-error] (vec (.getSuppressed ^Throwable setup-error))))
    (is (not-any? #{ptls-1} @freed_) "the PTLS context that the QUIC context needs stays")
    (is (every? (set @freed_) [quicly-0 ptls-0]))))

(deftest a-release-of-several-items-tries-every-item-test
  (let [[a b c] (map #(MemorySegment/ofAddress %) [1 2 3])
        tcp (fn [port] {:transport :tcp :port port})]
    (doseq [[kind x redef-var]
            [[:socket-claims {::generation/listener-claims {(tcp 1) a (tcp 2) b (tcp 3) c}}
              #'listen/release!]
             [:loops {::generation/loops [a b c]} #'h2o/evloop-destroy]
             [:listeners {:listener-socks [a b c]} #'h2o/socket-close]
             [:ssl-ctxs {::generation/ssl-ctxs [a nil b c]} #'h2o/free-ssl-ctx]
             [:h3-contexts {::generation/http3-contexts [{:quicly-ctx a} nil {:quicly-ctx b}
                                                         {:quicly-ctx c}]}
              #'h2o/http3-free-quicly-ctx]
             [:h3 {:http3-ctxs [a b c]} #'h2o/http3-free-worker-ctx]]]
      (testing (str kind)
        (let [calls_ (atom [])
              errors {a (ex-info "first" {}) b (ex-info "second" {})}
              error (with-redefs-fn {redef-var (fn [item]
                                                 (swap! calls_ conj item)
                                                 (some-> (errors item) throw))}
                      #(try ((:release (kind effects/table)) x 0) nil (catch Throwable t t)))]
          (is (= #{a b c} (set @calls_)))
          (is (identical? (errors a) error))
          (is (= [(errors b)] (vec (.getSuppressed ^Throwable error)))))))))

(deftest a-loops-acquire-that-fails-destroys-every-loop-it-made-test
  (let [[a b] (map #(MemorySegment/ofAddress %) [1 2])
        loops_ (atom [a b])
        destroyed_ (atom [])
        boom (ex-info "simulated loop failure" {})
        error (with-redefs [h2o/evloop-create (fn []
                                                (if-let [l (first @loops_)]
                                                  (do (swap! loops_ rest) l)
                                                  (throw boom)))
                            h2o/evloop-destroy (fn [l]
                                                 (swap! destroyed_ conj l)
                                                 (when (= a l) (throw (ex-info "first" {}))))]
                (try ((:acquire (:loops effects/table)) {::generation/n-workers 3} nil) nil
                     (catch Throwable t t)))]
    (is (identical? boom (effects/partial-acquire error)))
    (is (= [a b] @destroyed_))))

(deftest closing-the-udp-sockets-of-a-kept-worker-tries-every-listener-test
  (let [[a b c] (map #(MemorySegment/ofAddress %) [1 2 3])
        calls_ (atom [])
        e (ex-info "first" {})
        error (with-redefs [h2o/http3-close-worker-sockets (fn [ctx]
                                                             (swap! calls_ conj ctx)
                                                             (cond (= a ctx) (throw e) (= b ctx) 1 :else 0))]
                (try (effects/close-kept-udp! {::generation/http3-worker-contexts [[a b c]]} 0) nil
                     (catch Throwable t t)))]
    (is (= [a b c] @calls_))
    (is (identical? e error))
    (is (= 1 (count (.getSuppressed ^Throwable error))))))
