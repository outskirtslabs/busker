(ns ol.h2o.native
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem :refer [defalias]])
  (:import [java.nio.file Files]))

(defn copy-resource [resource-path output-path]
  (with-open [in (io/input-stream (io/resource resource-path))
              out (io/output-stream (io/file output-path))]
    (io/copy in out)))

(defn get-arch+os []
  (let [os-name (str/lower-case (System/getProperty "os.name"))]
    (str (System/getProperty "os.arch") "-"
         (cond (str/includes? os-name "win") "windows"
               (str/includes? os-name "nux") "linux"
               (str/includes? os-name "mac") "macos"))))

#_(defn load-bundled-library []
    (let [res-file (case (get-arch+os)
                     "aarch64-linux" "libh2o_aarch64-linux-gnu.so"
                     "aarch64-macos" "libh2o_aarch64-macos-none.so"
                     ("x86-linux"
                      "amd64-linux") "libh2o_x86_64-linux-gnu.so"
                     ("x86-macos"
                      "amd64-macos") "libh2o_x86_64-macos-none.so"
                     ("x86-windows"
                      "amd64-windows") "libh2o_x86_64-windows-gnu.dll")
          temp-lib-filename (str "h2oclj_temp_" res-file)]
      (copy-resource res-file temp-lib-filename)
      (ffi/load-library temp-lib-filename)
      (Files/deleteIfExists (.toPath (io/file temp-lib-filename)))))

#_(defn load-system-library []
    (ffi/load-system-library "libh2o"))

(ffi/load-system-library "h2o-evloop")
(ffi/load-library (System/getProperty "ol.libh2o.path"))

(defcfn evloop-create
  "Creates a new event loop. Returns a pointer to h2o_evloop_t."
  h2o_evloop_create
  [] ::mem/pointer)

(defcfn evloop-destroy
  "Destroys an event loop and frees associated resources."
  h2o_evloop_destroy
  [::mem/pointer] ::mem/void)

(defcfn evloop-run
  "Runs the event loop once. Returns 0 if successful, -1 on error (typically EINTR).
   
   Parameters:
   - loop: pointer to h2o_evloop_t
   - max-wait: maximum time to wait in milliseconds (int32)"
  h2o_evloop_run
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn config-init
  "Initialize h2o global configuration structure"
  h2o_config_init
  [::mem/pointer] ::mem/void)

(defcfn config-dispose
  "Dispose h2o global configuration and free resources"
  h2o_config_dispose
  [::mem/pointer] ::mem/void)

;; h2o_iovec_t is:
;;   typedef struct { char *base; size_t len; } h2o_iovec_t;
;; Use a typed pointer for clarity; size_t→::mem/long is OK on typical *nix.
(mem/defalias ::h2o-iovec-t
  (layout/with-c-layout
    [::mem/struct
     [[:base [::mem/pointer ::mem/byte]]
      [:len  ::mem/long]]]))

(defcfn config-register-host
  "Register a virtual host with the h2o configuration.
   Returns pointer to h2o_hostconf_t"
  h2o_config_register_host
  [::mem/pointer ::h2o-iovec-t ::mem/short] ::mem/pointer)

(defcfn context-init
  "Initialize h2o context for an event loop.
   
   Parameters:
   - context: pointer to h2o_context_t
   - loop: pointer to h2o_evloop_t
   - config: pointer to h2o_globalconf_t"
  h2o_context_init
  [::mem/pointer ::mem/pointer ::mem/pointer] ::mem/void)

(defcfn context-dispose
  "Dispose h2o context and free resources"
  h2o_context_dispose
  [::mem/pointer] ::mem/void)

(defcfn context-request-shutdown
  "Request graceful shutdown of all connections in this context (sends GOAWAY)"
  h2o_context_request_shutdown
  [::mem/pointer] ::mem/void)

(defcfn evloop-socket-create
  "Create h2o socket wrapper for file descriptor.
   
   Parameters:
   - loop: pointer to h2o_evloop_t
   - fd: file descriptor (int)
   - flags: socket flags (int)
   
   Returns: pointer to h2o_socket_t"
  h2o_evloop_socket_create
  [::mem/pointer ::mem/int ::mem/int] ::mem/pointer)

(defcfn socket-read-start
  "Start reading from socket with callback.
   
   Parameters:
   - sock: pointer to h2o_socket_t
   - cb: callback function pointer"
  h2o_socket_read_start
  [::mem/pointer ::mem/pointer] ::mem/void)

(defcfn socket-reading?  "clj_h2o_socket_is_reading"  [::mem/pointer] ::mem/int)
(defcfn socket-writing?  "clj_h2o_socket_is_writing"  [::mem/pointer] ::mem/int)
(defcfn socket-read-cb   "clj_h2o_socket_get_read_cb" [::mem/pointer] ::mem/pointer)
(defcfn socket-write-cb  "clj_h2o_socket_get_write_cb" [::mem/pointer] ::mem/pointer)

(defcfn evloop-socket-accept
  "Accept new connection from listening socket.
   Returns pointer to h2o_socket_t or NULL"
  h2o_evloop_socket_accept
  [::mem/pointer] ::mem/pointer)

(defcfn socket-close
  "Close h2o socket"
  h2o_socket_close
  [::mem/pointer] ::mem/void)

(defcfn h2o-accept
  "Pass accepted socket to h2o for HTTP handling.
   
   Parameters:
   - ctx: pointer to h2o_accept_ctx_t
   - sock: pointer to h2o_socket_t"
  h2o_accept
  [::mem/pointer ::mem/pointer] ::mem/void)

(defn create-iovec
  "Create an h2o_iovec_t from a string"
  [s]
  {:base (mem/serialize s ::mem/c-string)
   :len (count s)})

(defn create-server-config
  "Create and initialize h2o global configuration with a default host.
   Returns map with ::arena, ::config-ptr, ::hostconf-ptr"
  []
  (let [arena (mem/auto-arena)
        config-ptr (mem/alloc-instance ::mem/pointer arena)]
    (config-init config-ptr)
    (let [host-iovec (create-iovec "default")
          hostconf-ptr (config-register-host config-ptr host-iovec 65535)]
      {::arena arena
       ::config-ptr config-ptr
       ::hostconf-ptr hostconf-ptr})))

(defn create-context
  "Create and initialize h2o context for an event loop.
   Returns pointer to h2o_context_t"
  [arena loop-ptr config-ptr]
  (let [ctx-ptr (mem/alloc-instance ::mem/pointer arena)]
    (context-init ctx-ptr loop-ptr config-ptr)
    ctx-ptr))

(defn dispose-server-config
  "Dispose h2o configuration and free resources"
  [config-ptr]
  (config-dispose config-ptr))

(defn create-loops
  "Create n event loops.
   Returns vector of h2o_evloop_t pointers"
  [n]
  (vec (repeatedly n evloop-create)))

(defn create-contexts
  "Create h2o contexts for the given event loops.
   Returns vector of h2o_context_t pointers"
  [arena loops config-ptr]
  (vec (for [loop loops]
         (create-context arena loop config-ptr))))

(defn create-socket-for-loop
  "Create h2o socket wrapper for file descriptor in event loop"
  [loop-ptr fd flags]
  (evloop-socket-create loop-ptr fd flags))

(defn dispose-contexts
  "Dispose all h2o contexts"
  [contexts]
  (doseq [ctx contexts]
    (context-dispose ctx)))

(defn destroy-loops
  "Destroy all event loops"
  [loops]
  (doseq [loop loops]
    (evloop-destroy loop)))

;; POSIX socket syscalls via libc


