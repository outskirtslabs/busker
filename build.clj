(ns build
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.tools.build.api :as b]
   [clojure.string :as str]))

(def lib 'io.cljnet/http-clj)
(def class-dir "target/classes")

(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-shim
  "Compile C shim library using CMake."
  [_]
  (println "Compiling C shim library...")
  (let [mkdir-result (shell/sh "mkdir" "-p" "cmake-build-debug")
        _ (when (not= 0 (:exit mkdir-result))
            (throw (ex-info "Failed to create cmake-build-debug directory" mkdir-result)))
        cmake-result (shell/sh "cmake" "-DCMAKE_BUILD_TYPE=Debug" "-B" "cmake-build-debug")
        _ (when (not= 0 (:exit cmake-result))
            (println (:err cmake-result))
            (throw (ex-info "CMake configuration failed" cmake-result)))
        build-result (shell/sh "cmake" "--build" "cmake-build-debug" "--config" "Debug" "--target" "h2oclj")]
    (when (not= 0 (:exit build-result))
      (println (:err build-result))
      (throw (ex-info "CMake build failed" build-result)))
    (println "Compiled C shim to cmake-build-debug/libh2oclj.so")))

(defn compile-java
  [_]
  (println "Compiling Java...")
  (b/javac {:src-dirs ["src/main/java"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "25" "--enable-preview"]})
  (println "Compiled Java classes to" class-dir))

(defn compile
  "Run complete build: compile shim."
  [opts]
  (compile-shim opts)
  #_(compile-java opts))
