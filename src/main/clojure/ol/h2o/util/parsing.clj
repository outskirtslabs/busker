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
(ns ol.h2o.util.parsing)

(def ^{:doc "HTTP token: 1*<any CHAR except CTLs or tspecials>. See RFC2068"}
  re-token
  #"[!#$%&'*\-+.0-9A-Z\^_`a-z\|~]+")

(def ^{:doc "HTTP quoted-string: <\"> *<any TEXT except \"> <\">. See RFC2068."}
  re-quoted
  #"\"((?:\\\"|[^\"])*)\"")

(def ^{:doc "HTTP value: token | quoted-string. See RFC2109"}
  re-value
  (str "(" re-token ")|" re-quoted))

(def ^{:doc "Pattern for pulling the charset out of the content-type header"}
  re-charset
  (re-pattern (str ";(?:.*\\s)?(?i:charset)=(?:" re-value ")\\s*(?:;|$)")))

(defn find-content-type-charset
  "Return the charset of a given a content-type string."
  [s]
  (when-let [m (re-find re-charset s)]
    (or (m 1) (m 2))))
