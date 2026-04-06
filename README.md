# Busker

> *Busker* is your Clojure web server that plays live on the open web

Busker is a Clojure HTTP server library built directly on [libh2o](https://github.com/h2o/h2o) (the web server that powers Fastly).
It brings HTTP/1.1, HTTP/2, and HTTP/3 support to the JVM making extensive use of Panama FFI and virtual threads.

- Busker provides full modern HTTP support including HTTP/1.1, HTTP/2, and HTTP/3 with features like 103 Early Hints and HTTP/2 streams
- Busker handles automatic HTTPS certificate management and renewal
- Busker simplifies deployment by eliminating nginx/caddy and Docker, just deploy your uberjar directly with systemd
- Busker delivers predictable performance under load with sensible production-ready defaults

Platform Requirements:

- requires JDK version 21 or higher (JDK 25 or higher for GraalVM native-image builds).
- pre-built jars containing the native binaries are available for Linux and macOS on aarch64 and x86-64.

## Roadmap

The blurb above is aspirational, Busker is still a work in progress.

The actual state of affairs is:

- [x] libh2o native builds
- [x] HTTP/1.1
- [x] HTTP/2
- [x] 103 early hints
- [x] HTTP/3
- [ ] IPv4
- [ ] unix domain sockets
- [ ] 0-RTT TLS
- [ ] Automatic HTTPS
- [ ] TLS STEK, pluggable ticket store
- [ ] Metrics/signals w/out the abominable otel
- [ ] Reverse proxying
- [ ] Standlone operation mode w/ config file
- [ ] Zero-downtime deployments w/ systemd
- [ ] GraalVM native-image (need to wait until [coffi][coffi] supports it)

See [ROADMAP](./ROADMAP.md) for more info

## Usage

You need this deps.edn dependency:

```clojure
com.outskirtslabs/busker {:git/url "https://github.com/outskirtslabs/busker"
                          :git/sha "cf53e8e003f66b32d3fc3b0b456f764cc665b308"}
```

When using busker as a git dep always run `clj -X:deps prep` after bumping the git sha.

...and you need (at least) one of the native dependencies below.

The native dependency that matches your target platform is required.
If multiple native deps are included, the right one will automatically be chosen (though your uber-jar size will be fatter than necessary):

``` clojure
# Linux
com.outskirtslabs.busker/linux-x86-64 {:mvn/version "0.0.1"}
com.outskirtslabs.busker/linux-aarch64 {:mvn/version "0.0.1"}

# MacOS
com.outskirtslabs.busker/macos-x86-64 {:mvn/version "0.0.1"}
com.outskirtslabs.busker/macos-aarch64 {:mvn/version "0.0.1"}
```


## License

Busker is distributed under the [EUPL-1.2](https://spdx.org/licenses/EUPL-1.2.html).

Copyright © 2025-2026 Casey Link

Some files included in this project and in binary distributions (JAR files on Clojars and GitHub releases)
are from third-party sources and retain their original licenses as indicated in [NOTICE](./NOTICE).

[coffi]: https://github.com/IGJoshua/coffi
