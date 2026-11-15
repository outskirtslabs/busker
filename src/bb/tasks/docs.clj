(ns tasks.docs
  "Checks that the committed API pages match the docstrings."
  (:require
   [babashka.fs :as fs]
   [babashka.process :as p]
   [ol.bb-tasks.gen-api-docs :as gen-api-docs]))

(defn check-api-pages!
  "Generates the API pages into a copy of `doc` and throws when they differ from
  the committed pages. Takes the `bb gen-docs` options."
  [opts]
  (fs/with-temp-dir [dir {}]
    (let [out (str (fs/path dir "doc"))]
      (fs/copy-tree "doc" out)
      (gen-api-docs/generate! (assoc opts :antora-start-path out))
      (when-not (zero? (:exit (p/shell {:continue true} "diff" "-ru" "doc" out)))
        (throw (ex-info "API pages differ from the docstrings. Run bb gen-docs." {}))))))
