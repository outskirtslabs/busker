# Roadmap

## Backlog

(not in order)

- [x] Dynamic runtime configuration
- [ ] Metrics/signals w/out the abominable otel (or with?)
- [x] TLS STEK and pluggable ticket store
- [ ] Cross-process TLS ticket resumption test
- [ ] Reverse proxying
- [ ] Standalone operation mode w/ config file
- [ ] Zero-downtime deployments w/ systemd
- [ ] GraalVM native-image
- [ ] Configurable congestion control -allow selection of congestion control algorithm (Reno, Cubic, BBR) instead of using quicly defaults
- [ ] Connection keep-alive config - add explicit keep-alive interval and timeout configuration instead of relying on implicit ACK frequency
- [x] Dynamic TLS certificate selection based on SNI for multi-domain hosting on a single port
- [ ] User-supplied TLS certificate callback API
- [ ] QUIC datagram support  implement RFC 9221 unreliable datagram frames for use cases like WebRTC over HTTP/3 or gaming protocols
- [ ] Support .ts.net  out of the box


## Small tasks

- [x] implement request cleanup/reaping when client disconnects early, have to interrupt body and response streams
- [x] rewrite response queue from Channel -> OutputStream
- [x] rename all keywords in the state maps (like servers) which are mutable values like AtomicLong so they have a _ at the end to indicate they are mutable references
- [x] ipv6 listener support
- [x] unix domain socket listener support
- [ ] double check all volatile uses for correctness
- [ ] should we replace AtomicBooleans with atoms?
