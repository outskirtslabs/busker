(ns ol.busker.listen
  (:require
   [coffi.mem :as mem]
   [ol.busker.native :as h2o]
   [ol.busker.native.socket :as socket]))

(set! *warn-on-reflection* true)

(defn listener-key
  [listener]
  (cond
    (:unix listener)
    {:transport (or (:transport listener) :tcp)
     :unix (:unix listener)}

    :else
    {:transport (or (:transport listener) :tcp)
     :host (or (:host listener) "0.0.0.0")
     :port (:port listener)}))

(defn open-pool
  []
  {:lock (Object.)
   :state (atom {})})

(defn- open-resource
  [{:keys [transport host port] :as key}]
  (case transport
    :tcp
    (socket/open-master-listener {:host host
                                  :port port})

    :udp
    (let [listener (h2o/http3-open-udp-listener host (short port))]
      (when (or (nil? listener) (mem/null? listener))
        (throw (ex-info "Failed to open pooled HTTP/3 UDP listener"
                        {:listener-key key})))
      listener)

    (throw (ex-info "Unsupported listener transport"
                    {:listener-key key}))))

(defn- close-resource!
  [{:keys [transport]} resource]
  (case transport
    :tcp
    (socket/close-fd! resource)

    :udp
    (when (and resource (not (mem/null? resource)))
      (h2o/http3-release-udp-listener resource))

    nil))

(defn acquire-claim
  [pool listener]
  (let [key (listener-key listener)
        claim {:pool pool
               :key key
               :released? (atom false)
               :fake-closed? (atom false)}]
    (locking (:lock pool)
      (swap! (:state pool)
             (fn [state]
               (if-let [entry (get state key)]
                 (update-in state [key :ref-count] inc)
                 (assoc state key {:resource (open-resource key)
                                   :ref-count 1})))))
    claim))

(defn fake-close!
  [claim]
  (reset! (:fake-closed? claim) true)
  nil)

(defn release!
  [claim]
  (when (compare-and-set! (:released? claim) false true)
    (let [{:keys [pool key]} claim
          resource-to-close
          (locking (:lock pool)
            (let [state (:state pool)
                  entry (get @state key)]
              (when entry
                (let [next-ref-count (dec (:ref-count entry))]
                  (if (pos? next-ref-count)
                    (do
                      (swap! state assoc-in [key :ref-count] next-ref-count)
                      nil)
                    (do
                      (swap! state dissoc key)
                      (:resource entry)))))))]
      (when resource-to-close
        (close-resource! key resource-to-close))))
  nil)
