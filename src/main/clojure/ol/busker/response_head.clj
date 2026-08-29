(ns ^:no-doc ol.busker.response-head
  (:require
   [coffi.mem :as mem]
   [ol.busker.native :as h2o])
  (:import
   [java.lang.foreign MemorySegment]))

(set! *warn-on-reflection* true)

(defrecord StartCommand
           [^long module-id
            ^long request-seq
            ^long status
            headers
            ^long content-length
            ^long compress-hint])

(defrecord InformationalCommand
           [^long module-id
            ^long request-seq
            ^long status
            headers])

(def ^:private get-current-worker
  (delay (requiring-resolve 'ol.busker.evloop/get-current-worker)))

(def ^:private clj-header-size (mem/size-of ::h2o/clj-header-t))
(def ^:private clj-header-name-offset (mem/struct-field-offset ::h2o/clj-header-t :name))
(def ^:private clj-header-name-len-offset (mem/struct-field-offset ::h2o/clj-header-t :name_len))
(def ^:private clj-header-value-offset (mem/struct-field-offset ::h2o/clj-header-t :value))
(def ^:private clj-header-value-len-offset (mem/struct-field-offset ::h2o/clj-header-t :value_len))

(defn start-command
  [module-id request-seq status headers content-length compress-hint]
  (->StartCommand module-id request-seq status (vec headers) content-length compress-hint))

(defn informational-command
  [module-id request-seq status headers]
  (->InformationalCommand module-id request-seq status (vec headers)))

(defn command-module-id
  ^long [command]
  (:module-id command))

(defn command-request-seq
  ^long [command]
  (:request-seq command))

(defn- headers-segment
  [headers arena]
  (let [header-count (count headers)]
    (if (zero? header-count)
      (mem/as-segment 0)
      (let [segment (mem/alloc (* header-count clj-header-size) arena)]
        (doseq [[idx [^String name ^String value]] (map-indexed vector headers)]
          (let [offset (* idx clj-header-size)
                name-ptr ^MemorySegment (mem/serialize name ::mem/c-string arena)
                value-ptr ^MemorySegment (mem/serialize value ::mem/c-string arena)]
            (mem/write-address segment (+ offset clj-header-name-offset) name-ptr)
            (mem/write-int segment (+ offset clj-header-name-len-offset) (dec (.byteSize name-ptr)))
            (mem/write-address segment (+ offset clj-header-value-offset) value-ptr)
            (mem/write-int segment (+ offset clj-header-value-len-offset) (dec (.byteSize value-ptr)))))
        segment))))

(defn- ensure-worker!
  [req]
  (when-not (identical? (:worker req) (@get-current-worker))
    (throw (ex-info "Response head command ran outside its event-loop worker" {}))))

(defn execute-start!
  [req ^StartCommand command]
  (ensure-worker! req)
  (with-open [arena (mem/confined-arena)]
    (let [headers (.-headers command)
          segment (headers-segment headers arena)
          {:keys [proceed stop]} (:callback-pointers req)]
      (h2o/start-response (:req-ctx-ptr req)
                          (.-status command)
                          segment
                          (count headers)
                          (.-content-length command)
                          (.-compress-hint command)
                          proceed
                          stop))))

(defn execute-informational!
  [req ^InformationalCommand command]
  (ensure-worker! req)
  (with-open [arena (mem/confined-arena)]
    (let [headers (.-headers command)]
      (h2o/send-informational (:req-ctx-ptr req)
                              (.-status command)
                              (headers-segment headers arena)
                              (count headers)))))
