(ns ^:no-doc ol.busker.worker-mailbox
  "Coordinates a worker's 256 command permits and FIFO publication.

  The capacity includes both queued commands and reserved, unpublished permits.
  A reservation occupies no FIFO position, so a slow producer cannot delay
  commands that are ready. Each permit must be published or released once.
  Polling a command returns its capacity and wakes waiting producers.

  [[close!]] rejects new reservations but leaves valid permits publishable and
  queued commands available. [[pending?]] includes both kinds of work so worker
  retirement can wait for publication or discard. This module synchronizes Java
  state only; [[ol.busker.evloop]] handles native wakes and command execution."
  (:import [java.util ArrayDeque HashSet]))

(set! *warn-on-reflection* true)

(defn create []
  {:lock (Object.) :queue (ArrayDeque.) :reservations (HashSet.)
   :state_ (atom {:open? true :used 0})})

(defn wake-waiters! [{:keys [^Object lock]}]
  (locking lock (.notifyAll lock)))

(defn reserve!
  "Returns a permit, `:full` without waiting, or `:closed` on closure/cancellation.
  With `wait?` true, waits interruptibly in Java. Cancellation must also call
  [[wake-waiters!]] so a parked caller rechecks `cancelled?`."
  [{:keys [^Object lock] :as mailbox} wait? cancelled?]
  (locking lock
    (loop []
      (let [{:keys [open? used]} @(:state_ mailbox)]
        (cond
          (or (not open?) (cancelled?)) :closed
          (< used 256)
          (let [permit (Object.)]
            (.add ^HashSet (:reservations mailbox) permit)
            (swap! (:state_ mailbox) update :used inc)
            permit)
          (not wait?) :full
          :else (do (.wait lock) (recur)))))))

(defn release!
  "Returns an unpublished permit's capacity. Repeated release has no effect."
  [{:keys [lock] :as mailbox} permit]
  (locking lock
    (when (.remove ^HashSet (:reservations mailbox) permit)
      (swap! (:state_ mailbox) update :used dec)
      (wake-waiters! mailbox))))

(defn publish!
  "Consumes a valid permit and appends `command`, even after admission closes.
  Throws for a released or published permit; cannot fail because of fullness."
  [{:keys [lock] :as mailbox} permit command]
  (locking lock
    (when-not (.remove ^HashSet (:reservations mailbox) permit)
      (throw (IllegalStateException. "Command permit already published or released")))
    (.add ^ArrayDeque (:queue mailbox) command)
    (wake-waiters! mailbox)))

(defn poll! [{:keys [lock] :as mailbox}]
  (locking lock
    (when-let [command (.poll ^ArrayDeque (:queue mailbox))]
      (swap! (:state_ mailbox) update :used dec)
      (wake-waiters! mailbox)
      command)))

(defn close!
  "Rejects new reservations and wakes waiters without discarding accepted work."
  [{:keys [lock] :as mailbox}]
  (locking lock
    (swap! (:state_ mailbox) assoc :open? false)
    (wake-waiters! mailbox)))

(defn queued [{:keys [lock] :as mailbox}]
  (locking lock (.size ^ArrayDeque (:queue mailbox))))

(defn pending? [{:keys [lock] :as mailbox}]
  (locking lock (pos? (:used @(:state_ mailbox)))))
