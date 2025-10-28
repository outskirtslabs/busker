(ns user)
(require 'hashp.preload)

(comment
  (do
    (require
     '[portal.colors]
     '[portal.api :as p])
    (p/open {:theme :portal.colors/gruvbox})
    (add-tap p/submit)
    (require '[clj-reload.core :as clj-reload])
    (clj-reload/init {:dirs ["src/main/clojure" "dev" "src/test/cloujure"]}))

  (clj-reload/reload)

  (clojure.repl.deps/sync-deps)
  ;;
  )
