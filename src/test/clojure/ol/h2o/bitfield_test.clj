(ns ol.h2o.bitfield-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [coffi.mem :as mem]
   [ol.h2o.bitfield :as bf]))

(deftest defbitfield-test
  (testing "single bit fields"
    (let [spec (bf/defbitfield [[:flag-a 0]
                                [:flag-b 1]
                                [:flag-c 2]])]
      (is (= ::mem/byte (:storage-type spec)))
      (is (= 3 (:max-bits spec)))
      (is (= 0x01 (get-in spec [:fields :flag-a :mask])))
      (is (= 0x02 (get-in spec [:fields :flag-b :mask])))
      (is (= 0x04 (get-in spec [:fields :flag-c :mask])))))

  (testing "multi-bit fields"
    (let [spec (bf/defbitfield [[:mode 0 3] ; 3 bits at position 0
                                [:priority 3 2] ; 2 bits at position 3
                                [:enabled 5]])] ; 1 bit at position 5
      (is (= ::mem/byte (:storage-type spec)))
      (is (= 6 (:max-bits spec)))
      (is (= 0x07 (get-in spec [:fields :mode :mask]))) ; 0b00000111
      (is (= 0x18 (get-in spec [:fields :priority :mask]))) ; 0b00011000
      (is (= 0x20 (get-in spec [:fields :enabled :mask]))))) ; 0b00100000

  (testing "storage type selection"
    (is (= ::mem/byte (:storage-type (bf/defbitfield [[:a 0] [:b 7]]))))
    (is (= ::mem/short (:storage-type (bf/defbitfield [[:a 0] [:b 15]]))))
    (is (= ::mem/int (:storage-type (bf/defbitfield [[:a 0] [:b 31]]))))
    (is (= ::mem/long (:storage-type (bf/defbitfield [[:a 0] [:b 63]]))))))

(deftest get-bit-test
  (testing "extract single bit"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])
          field-a (get-in spec [:fields :flag-a])
          field-b (get-in spec [:fields :flag-b])
          field-c (get-in spec [:fields :flag-c])]
      (is (= 1 (bf/get-bit 0x01 field-a)))
      (is (= 0 (bf/get-bit 0x01 field-b)))
      (is (= 1 (bf/get-bit 0x03 field-a)))
      (is (= 1 (bf/get-bit 0x03 field-b)))
      (is (= 0 (bf/get-bit 0x03 field-c)))))

  (testing "extract multi-bit field"
    (let [spec (bf/defbitfield [[:mode 0 3]])
          field (get-in spec [:fields :mode])]
      (is (= 0 (bf/get-bit 0x00 field)))
      (is (= 5 (bf/get-bit 0x05 field)))
      (is (= 7 (bf/get-bit 0xFF field))))))

(deftest set-bit-test
  (testing "set single bit"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])
          field-a (get-in spec [:fields :flag-a])
          field-b (get-in spec [:fields :flag-b])]
      (is (= 0x01 (bf/set-bit 0x00 field-a 1)))
      (is (= 0x02 (bf/set-bit 0x00 field-b 1)))
      (is (= 0x03 (bf/set-bit 0x01 field-b 1)))
      (is (= 0x01 (bf/set-bit 0x03 field-b 0)))))

  (testing "set multi-bit field"
    (let [spec (bf/defbitfield [[:mode 0 3] [:priority 3 2]])
          mode-field (get-in spec [:fields :mode])
          priority-field (get-in spec [:fields :priority])]
      (is (= 0x05 (bf/set-bit 0x00 mode-field 5)))
      (is (= 0x18 (bf/set-bit 0x00 priority-field 3)))
      (is (= 0x1D (bf/set-bit 0x05 priority-field 3)))))

  (testing "value is masked to width"
    (let [spec (bf/defbitfield [[:mode 0 3]])
          field (get-in spec [:fields :mode])]
      (is (= 0x07 (bf/set-bit 0x00 field 0xFF))))))

(deftest test-bit?-test
  (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1]])
        field-a (get-in spec [:fields :flag-a])
        field-b (get-in spec [:fields :flag-b])]
    (is (true? (bf/test-bit? 0x01 field-a)))
    (is (false? (bf/test-bit? 0x01 field-b)))
    (is (true? (bf/test-bit? 0x03 field-a)))
    (is (true? (bf/test-bit? 0x03 field-b)))
    (is (false? (bf/test-bit? 0x00 field-a)))))

(deftest make-bitfield-value-test
  (testing "single bit flags"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])]
      (is (= 0x00 (bf/make-bitfield-value spec {})))
      (is (= 0x01 (bf/make-bitfield-value spec {:flag-a 1})))
      (is (= 0x03 (bf/make-bitfield-value spec {:flag-a 1 :flag-b 1})))
      (is (= 0x07 (bf/make-bitfield-value spec {:flag-a 1 :flag-b 1 :flag-c 1})))))

  (testing "multi-bit fields"
    (let [spec (bf/defbitfield [[:mode 0 3] [:priority 3 2] [:enabled 5]])]
      (is (= 0x05 (bf/make-bitfield-value spec {:mode 5})))
      (is (= 0x18 (bf/make-bitfield-value spec {:priority 3})))
      (is (= 0x20 (bf/make-bitfield-value spec {:enabled 1})))
      (is (= 0x3D (bf/make-bitfield-value spec {:mode 5 :priority 3 :enabled 1})))))

  (testing "unknown fields are ignored"
    (let [spec (bf/defbitfield [[:flag-a 0]])]
      (is (= 0x01 (bf/make-bitfield-value spec {:flag-a 1 :unknown 1}))))))

(deftest read-bitfield-test
  (testing "read from memory segment"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])
          segment (mem/alloc 1)]
      (mem/write-byte segment 0 0x03)
      (is (= {:flag-a 1 :flag-b 1 :flag-c 0}
             (bf/read-bitfield segment 0 spec)))))

  (testing "read multi-bit fields"
    (let [spec (bf/defbitfield [[:mode 0 3] [:priority 3 2]])
          segment (mem/alloc 1)]
      (mem/write-byte segment 0 0x1D) ; mode=5, priority=3
      (is (= {:mode 5 :priority 3}
             (bf/read-bitfield segment 0 spec)))))

  (testing "read with offset"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1]])
          segment (mem/alloc 4)]
      (mem/write-byte segment 0 (unchecked-byte 0xFF))
      (mem/write-byte segment 1 0x01)
      (mem/write-byte segment 2 0x02)
      (is (= {:flag-a 1 :flag-b 1} (bf/read-bitfield segment 0 spec)))
      (is (= {:flag-a 1 :flag-b 0} (bf/read-bitfield segment 1 spec)))
      (is (= {:flag-a 0 :flag-b 1} (bf/read-bitfield segment 2 spec))))))

(deftest write-bitfield-test
  (testing "write to memory segment"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])
          segment (mem/alloc 1)]
      (mem/write-byte segment 0 0x00)
      (bf/write-bitfield segment 0 spec {:flag-a 1 :flag-b 1})
      (is (= 0x03 (mem/read-byte segment 0)))))

  (testing "write preserves other bits"
    (let [spec (bf/defbitfield [[:flag-a 0] [:flag-b 1] [:flag-c 2]])
          segment (mem/alloc 1)]
      (mem/write-byte segment 0 0x07) ; all flags set
      (bf/write-bitfield segment 0 spec {:flag-b 0})
      (is (= 0x05 (mem/read-byte segment 0))))) ; flag-b cleared, others remain

  (testing "write multi-bit fields"
    (let [spec (bf/defbitfield [[:mode 0 3] [:priority 3 2]])
          segment (mem/alloc 1)]
      (mem/write-byte segment 0 0x00)
      (bf/write-bitfield segment 0 spec {:mode 5 :priority 3})
      (is (= 0x1D (mem/read-byte segment 0))))))

(deftest h2o-handler-flags-example
  (testing "h2o_handler_t bitfield pattern"
    (let [spec (bf/defbitfield [[:supports-request-streaming 0]
                                [:handles-expect 1]])
          segment (mem/alloc 1)]

      (testing "create handler with streaming support"
        (let [value (bf/make-bitfield-value spec {:supports-request-streaming 1})]
          (is (= 0x01 value))
          (is (true? (bf/test-bit? value (get-in spec [:fields :supports-request-streaming]))))
          (is (false? (bf/test-bit? value (get-in spec [:fields :handles-expect]))))))

      (testing "read handler flags from memory"
        (mem/write-byte segment 0 0x03)
        (let [flags (bf/read-bitfield segment 0 spec)]
          (is (= 1 (:supports-request-streaming flags)))
          (is (= 1 (:handles-expect flags))))))))
