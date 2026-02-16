(ns ol.busker.native.socket
  "low-level socket helpers via coffi/FFM.

   - Build sockaddr_in (IPv4) from host/port
   - open/bind/listen a nonblocking CLOEXEC master listener
   - duplicate the listener per worker thread (ownership: native side after handoff)
   - small utilities for flags and options "
  (:require
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.layout :as layout]
   [coffi.mem :as mem]))

(set! *warn-on-reflection* true)

;; minimal constants (POSIX/Linux values)
;; TODO: check on macos

(def ^:private AF_INET 2)
(def ^:private SOCK_STREAM 1)
(def ^:private SOL_SOCKET 1)
(def ^:private SO_REUSEADDR 2)
(def ^:private SO_REUSEPORT 15) ;; may differ on some BSDs; set only if requested

(def ^:private F_GETFL 3)
(def ^:private F_SETFL 4)
(def ^:private F_SETFD 2)
(def ^:private O_NONBLOCK 0x800)
(def ^:private FD_CLOEXEC 1)

(def ^:private INADDR_ANY 0x00000000)

;; struct in_addr { uint32_t s_addr; };
(mem/defalias ::in_addr
  [::mem/struct
   [[:s_addr ::mem/int]]])

;; struct sockaddr_in {
;;   uint16_t        sin_family;
;;   uint16_t        sin_port;
;;   struct in_addr  sin_addr;
;;   unsigned char   sin_zero[8];
;; }
;;
;; Use with-c-layout to ensure C padding/packing. :contentReference[oaicite:2]{index=2}
(mem/defalias ::sockaddr_in
  (layout/with-c-layout
    [::mem/struct
     [[:sin_family ::mem/short]
      [:sin_port ::mem/short]
      [:sin_addr ::in_addr]
      [:sin_zero [::mem/array ::mem/byte 8]]]]))

;; libc bindings (thin)

(defcfn socket "socket" [::mem/int ::mem/int ::mem/int] ::mem/int)
(defcfn setsockopt "setsockopt" [::mem/int ::mem/int ::mem/int ::mem/pointer ::mem/int] ::mem/int)
(defcfn fcntl "fcntl" [::mem/int ::mem/int ::mem/int] ::mem/int)
(defcfn bind "bind" [::mem/int ::mem/pointer ::mem/int] ::mem/int)
(defcfn listen "listen" [::mem/int ::mem/int] ::mem/int)
(defcfn dup "dup" [::mem/int] ::mem/int)
(defcfn close "close" [::mem/int] ::mem/int)
(defcfn htons "htons" [::mem/short] ::mem/short) ;; network byte order

;; optional: inet_pton for non-ANY binds; keeping IPv4 only here
(defcfn inet_pton "inet_pton" [::mem/int ::mem/c-string ::mem/pointer] ::mem/int)
(defcfn strerror "strerror" [::mem/int] ::mem/c-string)

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
(defn- set-nonblocking! [fd]
  (let [flags (fcntl fd F_GETFL 0)]
    (when (neg? flags)
      (throw (ex-info-with-errno "fcntl(F_GETFL) failed" {:fd fd})))
    (when (neg? (fcntl fd F_SETFL (bit-or flags O_NONBLOCK)))
      (throw (ex-info-with-errno "fcntl(F_SETFL,O_NONBLOCK) failed" {:fd fd})))))

#_{:clj-kondo/ignore [:type-mismatch]}
(defn- set-cloexec! [fd]
  (when (neg? (fcntl fd F_SETFD FD_CLOEXEC))
    (throw (ex-info-with-errno "fcntl(F_SETFD,FD_CLOEXEC) failed" {:fd fd}))))

#_{:clj-kondo/ignore [:type-mismatch]}
(defn- set-bool-sockopt! [fd level opt on?]
  (with-open [arena (mem/confined-arena)]
    (let [v (if on? 1 0)
          ptr (mem/alloc-instance ::mem/int arena)]
      (mem/write-int ptr 0 v)
      (when (neg? (setsockopt fd level opt ptr 4))
        (throw (ex-info-with-errno "setsockopt failed"
                                   {:fd fd :level level :opt opt :val v}))))))

(defn- sockaddr-in
  "Build a sockaddr_in for IPv4.
   host can be nil/\"0.0.0.0\" for INADDR_ANY."
  [{:keys [host port] :or {host "0.0.0.0"}} arena]
  (let [port-val (long port)]
    (when (or (neg? port-val) (> port-val 0xFFFF))
      (throw (ex-info "port must be in [0, 65535]" {:port port})))
    (let [s_addr (if (or (nil? host) (= host "0.0.0.0"))
                   INADDR_ANY
                   (with-open [tmp-arena (mem/confined-arena)]
                     #_{:clj-kondo/ignore [:type-mismatch]}
                     (let [dst (mem/alloc-instance ::in_addr tmp-arena)
                           r   (inet_pton AF_INET host dst)]
                       (when (neg? r)
                         (throw (ex-info "inet_pton error" {:host host :port port})))
                       (when (zero? r)
                         (throw (ex-info "inet_pton: invalid address" {:host host})))
                       (:s_addr (mem/deserialize dst ::in_addr)))))
          data   {:sin_family (short AF_INET)
                  :sin_port   (htons (unchecked-short port-val))
                  :sin_addr   {:s_addr s_addr}
                  :sin_zero   [0 0 0 0 0 0 0 0]}]
      (mem/serialize data ::sockaddr_in arena))))

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
    :or {host "0.0.0.0" backlog 65535 reuseaddr? true reuseport? false nonblock? true cloexec? true}}]
  (when-not (int? port)
    (throw (ex-info "port must be int" {:port port})))
  (let [fd (socket AF_INET SOCK_STREAM 0)]
    #_{:clj-kondo/ignore [:type-mismatch]}
    (when (neg? fd)
      (throw (ex-info-with-errno "socket() failed" {})))
    (try
      (when cloexec? (set-cloexec! fd))
      (when nonblock? (set-nonblocking! fd))
      (when reuseaddr? (set-bool-sockopt! fd SOL_SOCKET SO_REUSEADDR true))
      (when reuseport? (set-bool-sockopt! fd SOL_SOCKET SO_REUSEPORT true))
      (with-open [arena (mem/confined-arena)]
        #_{:clj-kondo/ignore [:type-mismatch]}
        (let [addr    (sockaddr-in {:host host :port port} arena)
              addrlen (int (mem/size-of ::sockaddr_in))]
          (when (neg? (bind fd addr addrlen))
            (throw (ex-info-with-errno
                    (str "bind() failed for " host ":" port)
                    {:host host :port port})))
          (when (neg? (listen fd (int backlog)))
            (throw (ex-info-with-errno
                    (str "listen() failed for " host ":" port)
                                       {:host host :port port :backlog backlog})))))
      fd
      (catch Throwable t
        (try (close fd) (catch Throwable _))
        (throw t)))))

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
     #_{:clj-kondo/ignore [:type-mismatch]}
     (let [d (dup master-fd)]
       (when (neg? d)
         (throw (ex-info-with-errno "dup() failed" {:master-fd master-fd})))
       (set-cloexec! d)
       d))))

(defn close-fd! [fd]
  (when (pos? fd)
    (close fd)))

;; how this integrates with h2o
;; For each worker i:
;;   - allocate an h2o_evloop_t
;;   - pass (dup’d-fds[i]) to h2o_evloop_socket_create(loop, fd, H2O_SOCKET_FLAG_DONT_READ)
;;   - set sock->data to your accept ctx and start read with on_accept
;;
;; See libh2o headers: h2o_evloop_socket_create / h2o_evloop_run. :contentReference[oaicite:3]{index=3}
