(ns ol.h2o.streaming-input
  (:import
   [java.nio ByteBuffer]
   [java.nio.channels Channels ReadableByteChannel]
   [java.util.concurrent LinkedBlockingQueue]))
(set! *warn-on-reflection* true)

;;; --------------------- BORDER

(defprotocol WriteReq
  (add-chunk [this chunk is-last] "Add a chunk of bytes to the channel. is-last indicates end of stream.")
  (input-stream [this]))

(defn create-write-req-channel
  "Creates a ReadableByteChannel + RequestBody for streaming request body.
   proceed-callback is called after each chunk is fully consumed."
  [proceed-callback]
  (let [queue (LinkedBlockingQueue. 1)
        eof-marker ::eof
        closed? (volatile! false)
        current-buf (volatile! nil)
        body-channel
        (reify
          WriteReq
          (add-chunk [_ chunk is-last]
            (when-not @closed?

              (when chunk
                (.put queue chunk))
              (when is-last
                (.put queue eof-marker))))

          (input-stream [this]
            (Channels/newInputStream this))

          ReadableByteChannel
          (read [_ dst]
            (when @closed?
              (throw (java.nio.channels.ClosedChannelException.)))

            ;; Load next chunk if current is exhausted
            (when (or (nil? @current-buf)
                      (not (.hasRemaining ^ByteBuffer @current-buf)))
              (let [chunk (.take queue)]
                (if (identical? chunk eof-marker)
                  (vreset! closed? true)
                  (vreset! current-buf (ByteBuffer/wrap chunk)))))

            (if @closed?
              -1
              ;; Copy from current buffer to destination
              (let [buf ^ByteBuffer @current-buf
                    initial-pos (.position dst)
                    to-copy (min (.remaining dst) (.remaining buf))]
                (when (pos? to-copy)
                  ;; Transfer bytes
                  (let [old-limit (.limit buf)]
                    (.limit buf (+ (.position buf) to-copy))
                    (.put dst buf)
                    (.limit buf old-limit))

                  ;; Request next chunk when this one is consumed
                  (when-not (.hasRemaining buf)
                    (proceed-callback)))

                ;; Return bytes read
                (- (.position dst) initial-pos))))

          (isOpen [_]
            (not @closed?))

          (close [_]
            (vreset! closed? true)
            (.clear queue)
            (.offer queue eof-marker)
            (vreset! current-buf nil)))]

    body-channel))
