(ns ol.busker.response-encoding-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [ol.busker.response :as response])
  (:import
   [java.io ByteArrayOutputStream]
   [java.nio ByteBuffer]
   [java.nio.charset Charset StandardCharsets]))

(deftest incremental-encoding-matches-java-replacement
  (doseq [charset ["UTF-8" "UTF-16" "US-ASCII" "ISO-2022-JP"]
          text ["" "plain" "é水😀" (str "a" (char 0xd800) "b")
                (str (apply str (repeat 8191 "x")) "😀水"
                     (apply str (repeat 10000 "日本語")))]]
    (testing (str charset " / " (count text) " characters")
      (let [head {:headers {"content-type" (str "text/plain; charset=" charset)}}
            expected (.getBytes ^String text (Charset/forName charset))
            out (ByteArrayOutputStream.)]
        (#'response/write-string! text head out)
        (is (= {:length (alength expected) :bytes (vec expected)}
               {:length (#'response/encoded-length text head)
                :bytes (vec (.toByteArray out))}))))))

(deftest unknown-charset-uses-utf8
  (let [text "é水😀"
        head {:headers {"content-type" "text/plain; charset=not-a-charset"}}
        expected (.getBytes text StandardCharsets/UTF_8)
        out (ByteArrayOutputStream.)]
    (#'response/write-string! text head out)
    (is (= {:length (alength expected) :bytes (vec expected)}
           {:length (#'response/known-length text head)
            :bytes (vec (.toByteArray out))}))))

(deftest bounded-encoder-count-is-exact
  (doseq [charset ["UTF-8" "UTF-16" "US-ASCII" "ISO-2022-JP"]]
    (let [text "hello é水😀"
          head {:headers {"content-type" (str "text/plain; charset=" charset)}}
          expected (.getBytes text (Charset/forName charset))
          buffer (ByteBuffer/allocate (int (#'response/encoded-length text head)))]
      ((#'response/bounded-producer text head) buffer)
      (is (= {:position (alength expected) :bytes (vec expected)}
             {:position (.position buffer) :bytes (vec (.array buffer))})))))

(deftest byte-buffer-adapters-preserve-input-range
  (let [source (doto (ByteBuffer/wrap (byte-array [0 1 2 3 4])) (.position 1) (.limit 4))
        bounded (ByteBuffer/allocate 3)
        streamed (ByteArrayOutputStream.)]
    ((#'response/bounded-producer source {}) bounded)
    (#'response/write-byte-buffer! source streamed)
    (is (= {:position 1 :limit 4 :length 3 :bounded [1 2 3] :streamed [1 2 3]}
           {:position (.position source) :limit (.limit source)
            :length (#'response/known-length source {})
            :bounded (vec (.array bounded)) :streamed (vec (.toByteArray streamed))}))))
