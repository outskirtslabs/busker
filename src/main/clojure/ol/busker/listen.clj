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

(defn- wrap-stage-error
  [message stage data t]
  (let [error-data (ex-data t)]
    (throw (ex-info message
                    (merge {:stage (or (:stage error-data) stage)}
                           data
                           error-data)
                    t))))

(defn open-pool
  []
  {:lock (Object.)
   :state (atom {})})

(defn- open-resource
  [{:keys [transport host port] :as key}]
  (try
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
                      {:listener-key key})))
    (catch Throwable t
      (wrap-stage-error "Failed to open pooled listener resource"
                        :listener-acquisition
                        {:listener-key key}
                        t))))

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
  [{:keys [state lock] :as pool} listener]
  (let [key (listener-key listener)
        entry (locking lock
                (get
                 (swap! state
                        (fn [current]
                          (if (get current key)
                            (update-in current [key :ref-count] inc)
                            (assoc current key {:resource  (open-resource key)
                                                :ref-count 1}))))
                 key))
        claim {:pool pool
               :key key
               :resource (:resource entry)
               :released? (atom false)
               :fake-closed? (atom false)}]
    claim))

(defn resource
  [claim]
  (:resource claim))

(defn fake-close!
  [claim]
  (reset! (:fake-closed? claim) true)
  nil)

(defn release!
  [claim]
  (when (compare-and-set! (:released? claim) false true)
    (let [{:keys [pool key]} claim
          state (:state pool)
          lock (:lock pool)
          resource-to-close
          (locking lock
            (let [entry (get @state key)]
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
