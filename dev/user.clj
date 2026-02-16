(ns user
  (:require
   [clj-reload.core :as clj-reload]))
((requiring-resolve 'hashp.install/install!))

(set! *warn-on-reflection* true)

;; Configure the paths containing clojure sources we want clj-reload to reload
(clj-reload/init {:dirs      ["src/main" "src/test"]
                  :no-reload #{'user 'dev 'ol.dev.portal 'ol.busker.benchmark.server 'ol.busker.benchmark}})

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

  (clj-reload/reload)
  (clj-reload/reload {:only :all}) ;; rcf

  (clojure.repl.deps/sync-deps)
  ;;
  )
