;; Copyright (c) 2009-2010 Mark McGranaghan
;; Copyright (c) 2009-2018 James Reeves

;; Permission is hereby granted, free of charge, to any person
;; obtaining a copy of this software and associated documentation
;; files (the "Software"), to deal in the Software without
;; restriction, including without limitation the rights to use,
;; copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the
;; Software is furnished to do so, subject to the following
;; conditions:

;; The above copyright notice and this permission notice shall be
;; included in all copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
;; EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
;; OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
;; NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
;; HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
;; WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
;; FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
;; OTHER DEALINGS IN THE SOFTWARE.
;;
;; This protocol is from https://github.com/ring-clojure/ring/blob/content-length/ring-core/src/ring/middleware/content_length.clj
;; one day it may make it into the ring spec.

(ns ol.busker.protocols.content-length
  (:require
   [ol.busker.protocols :as p]
   [ol.busker.util :as util]))

;; Extending primitive arrays prior to Clojure 1.12 requires using the low-level
;; extend function.
(extend (Class/forName "[B")
  p/SizableResponseBody
  {:body-size-in-bytes
   (fn [bs _] (alength bs))})

(extend-protocol p/SizableResponseBody
  String
  (body-size-in-bytes [s response]
    (alength (.getBytes s (or (util/get-charset response) "utf-8"))))
  java.io.File
  (body-size-in-bytes [f _]
    (.length f))
  Object
  (body-size-in-bytes [_ _] nil)
  nil
  (body-size-in-bytes [_ _] 0))
