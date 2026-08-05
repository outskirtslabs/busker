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
(ns ^:no-doc ol.busker.util.headers
  (:require
   [ol.busker.util.parsing :as parsing]
   [taoensso.trove :as trove]))

(defn find-header
  "Looks up a header in a Ring response (or request) case insensitively,
  returning the header map entry, or nil if not present."
  [resp ^String header-name]
  (->> (:headers resp)
       (filter #(.equalsIgnoreCase header-name (key %)))
       (first)))

(defn get-header
  "Looks up a header in a Ring response (or request) case insensitively,
  returning the value of the header, or nil if not present."
  [resp header-name]
  (some-> resp (find-header header-name) val))

(defn get-charset
  "Gets the character encoding of a Ring response."
  [resp]
  (some-> (get-header resp "Content-Type")
          parsing/find-content-type-charset))

(defn header
  "Returns an updated Ring response with the specified header added."
  [resp name value]
  (assoc-in resp [:headers name] (str value)))

(defmacro msg-time
  "Like clojure.core/time, but with a user defined prefix message."
  [prefix expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (trove/log! {:id :time :msg (str ~prefix " " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs")})
     ret#))
