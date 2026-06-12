(ns ol.busker.native.socket
  "low-level socket helpers via coffi/FFM.

   - open/bind/listen family-aware TCP listeners
   - open/bind/listen Unix domain socket listeners
   - duplicate the listener per worker thread (ownership: native side after handoff)
   - small utilities for flags and options"
  (:require
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.mem :as mem]
   [ol.busker.native.loader]))

(set! *warn-on-reflection* true)

(def ^:private F_SETFD 2)
(def ^:private FD_CLOEXEC 1)

;; libc bindings (thin)

(defcfn fcntl "fcntl" [::mem/int ::mem/int ::mem/int] ::mem/int)
(defcfn dup "dup" [::mem/int] ::mem/int)
(defcfn close "close" [::mem/int] ::mem/int)
(defcfn strerror "strerror" [::mem/int] ::mem/c-string)
(defcfn open-tcp-listener* "clj_h2o_open_tcp_listener"
  [::mem/c-string ::mem/int ::mem/int ::mem/int ::mem/int ::mem/int ::mem/int] ::mem/int)
(defcfn open-unix-listener* "clj_h2o_open_unix_listener"
  [::mem/c-string ::mem/int ::mem/int ::mem/int] ::mem/int)
(defcfn unlink-unix-socket-if-still-socket* "clj_h2o_unlink_unix_socket_if_still_socket"
  [::mem/c-string] ::mem/int)

;; helpers

(def ^:private errno-location-symbols
  ["__errno_location" "__error"])

(def ^:private errno-location-fn
  (delay
    (some (fn [sym]
            (when-let [addr (ffi/find-symbol sym)]
              (ffi/make-downcall addr [] ::mem/pointer)))
          errno-location-symbols)))

(def ^:private errno->keyword
  {9 :ebadf
   13 :eacces
   22 :einval
   23 :enfile
   24 :emfile
   88 :enotsock
   93 :eprotonosupport
   95 :eopnotsupp
   97 :eafnosupport
   98 :eaddrinuse
   99 :eaddrnotavail})

(defn- errno-details
  []
  (if-let [get-errno* @errno-location-fn]
    (let [errno-ptr (get-errno*)
          errno-int (if errno-ptr
                      (-> errno-ptr
                          (mem/reinterpret 4)
                          (mem/read-int 0))
                      -1)
          errno-key (get errno->keyword errno-int :unknown-errno)
          errno-msg (try
                      (strerror errno-int)
                      (catch Throwable _
                        nil))]
      {:errno-int errno-int
       :errno errno-key
       :errno-message errno-msg})
    {:errno-int nil
     :errno :unknown-errno
     :errno-message nil}))

(defn- ex-info-with-errno
  [message data]
  (let [{:keys [errno errno-int errno-message]} (errno-details)
        msg (if errno-message
              (str message " (" errno-message ")")
              message)]
    (ex-info msg
             (merge data
                    {:errno errno
                     :errno-int errno-int
                     :errno-message errno-message}))))

#_{:clj-kondo/ignore [:type-mismatch]}
(defn- set-cloexec! [fd]
  (when (neg? (fcntl fd F_SETFD FD_CLOEXEC))
    (throw (ex-info-with-errno "fcntl(F_SETFD,FD_CLOEXEC) failed" {:fd fd}))))

(defn open-master-listener
  "Create a master TCP socket, set NB/CLOEXEC and options, bind + listen.
   opts:
     :host        (default \"0.0.0.0\")
     :port        (required)
     :backlog     (default 65535)
     :reuseaddr?  (default true)
     :reuseport?  (default false)  ;; beware: platform semantics differ
     :nonblock?   (default true)
     :cloexec?    (default true)
  returns fd (int). Caller owns fd and must close on error."
  [{:keys [host port backlog reuseaddr? reuseport? nonblock? cloexec?]
    :or   {host "0.0.0.0" backlog 65535 reuseaddr? true reuseport? false nonblock? true cloexec? true}}]
  (when-not (integer? port)
    (throw (ex-info "port must be int" {:port port})))
  (let [fd (open-tcp-listener* host
                               (int port)
                               (int backlog)
                               (if reuseaddr? 1 0)
                               (if reuseport? 1 0)
                               (if nonblock? 1 0)
                               (if cloexec? 1 0))]
    #_{:clj-kondo/ignore [:type-mismatch]}
    (when (neg? fd)
      (throw (ex-info-with-errno
              (str "failed to open TCP listener for " host ":" port)
              {:host host
               :port port
               :backlog backlog})))
    fd))

(defn open-unix-listener
  "Create a Unix domain socket listener.
   `path` may be a filesystem socket path like `/tmp/busker.sock` or an
   abstract Linux socket name prefixed with `@`."
  [{:keys [path backlog nonblock? cloexec?]
    :or {backlog 65535 nonblock? true cloexec? true}}]
  (when-not (string? path)
    (throw (ex-info "path must be string" {:path path})))
  (let [fd (open-unix-listener* path
                                (int backlog)
                                (if nonblock? 1 0)
                                (if cloexec? 1 0))]
    #_{:clj-kondo/ignore [:type-mismatch]}
    (when (neg? fd)
      (throw (ex-info-with-errno
              (str "failed to open unix listener for " path)
              {:path path
               :backlog backlog})))
    fd))

(defn dup-fd
  "Duplicate `fd` and set FD_CLOEXEC on the duplicate."
  [fd]
  #_{:clj-kondo/ignore [:type-mismatch]}
  (let [d (dup fd)]
    (when (neg? d)
      (throw (ex-info-with-errno "dup() failed" {:fd fd})))
    (set-cloexec! d)
    d))

(defn dup-for-threads
  "Given a master listener fd and N, returns a vector of N dup'd fds.
   Each dup is set FD_CLOEXEC. Nonblocking inherits from master (per-FD flag)."
  [master-fd n]
  (when (neg? master-fd)
    (throw (ex-info "invalid master-fd" {:fd master-fd})))
  (when (or (nil? n) (neg? n))
    (throw (ex-info "n must be >= 0" {:n n})))
  (vec
   (for [_ (range n)]
     (dup-fd master-fd))))

(defn close-fd! [fd]
  (when (pos? fd)
    (close fd)))

(defn unlink-unix-socket-if-still-socket!
  "Remove `path` only if it still exists and is still a Unix socket.
   Returns true when a socket path was removed, false when cleanup was skipped."
  [path]
  (when-not (string? path)
    (throw (ex-info "path must be string" {:path path})))
  (let [rc (unlink-unix-socket-if-still-socket* path)]
    #_{:clj-kondo/ignore [:type-mismatch]}
    (when (neg? rc)
      (throw (ex-info-with-errno
              (str "failed to safely unlink unix socket path " path)
              {:path path})))
    #_{:clj-kondo/ignore [:type-mismatch]}
    (pos? rc)))

;; how this integrates with h2o
;; For each worker i:
;;   - allocate an h2o_evloop_t
;;   - pass (dup’d-fds[i]) to h2o_evloop_socket_create(loop, fd, H2O_SOCKET_FLAG_DONT_READ)
;;   - set sock->data to your accept ctx and start read with on_accept
;;
;; See libh2o headers: h2o_evloop_socket_create / h2o_evloop_run. :contentReference[oaicite:3]{index=3}
