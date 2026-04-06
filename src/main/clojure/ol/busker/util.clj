(ns ol.busker.util)

(defmacro compile-if
  "Evaluates `test`. If it returns logical true (and doesn't throw), expands
  to `then`, otherwise expands to `else`."
  {:style/indent 1}
  [test then else]
  (if (try (eval test) (catch Throwable _ false))
    `(do ~then)
    `(do ~else)))
