# Busker

> *Busker* is your Clojure web server that plays live on the open web

Busker is a Clojure HTTP server library built directly on [libh2o](https://github.com/h2o/h2o) (the web server that powers Fastly). It brings HTTP/1.1, HTTP/2, and HTTP/3 support to the JVM through Panama FFI and Loom Virtual Threads.

- Busker provides full modern HTTP support including HTTP/1.1, HTTP/2, and HTTP/3 with features like 103 Early Hints and HTTP/2 streams
- Busker handles automatic HTTPS certificate management and renewal
- Busker simplifies deployment by eliminating Docker, just deploy your uberjar directly with systemd
- Busker delivers predictable performance under load with sensible production-ready defaults

**Requirements**

- Busker requires JDK version 21 or higher (JDK 25 or higher for GraalVM native-image builds).
- Busker has out-of-the-box support for Linux and macOS on aarch64 and x86-64.

## Roadmap

The blurb above is aspirational, Busker is still a work in progress.

The actual state of affairs is:

- [x] libh2o native builds
- [x] HTTP/1.1
- [x] HTTP/2
- [x] 103 early hints
- [ ] Automatic HTTPS
- [ ] HTTP/3
- [ ] 0 downtime deployments
- [ ] GraalVM native-image (need to wait until [coffi][coffi] supports it)

## Usage

You need this deps.edn dependency:

```clojure
com.outskirtslabs/busker {:git/url "https://github.com/outskirtslabs/busker"
                          :git/sha ""}
```

...and you need one of the native dependencies:

The native dependency that matches your target platform is required. If multiple native deps are included, the right one will automatically be chosen (though your uber-jar size will be fatter than necessary):

``` clojure
com.outskirtslabs.busker/macos-x86-64 {:mvn/version "0.0.1"}
com.outskirtslabs.busker/linux-x86-64 {:mvn/version "0.0.1"}
com.outskirtslabs.busker/macos-aarch64 {:mvn/version "0.0.1"}
com.outskirtslabs.busker/linux-aarch64 {:mvn/version "0.0.1"}
```

## License

Busker is distributed under the [EUPL-1.2](https://spdx.org/licenses/EUPL-1.2.html).

Copyright © 2025 Casey Link <casey@outskirtslabs.com>

Some files included in this project are from third-party sources and retain their original licenses as indicated in per-file license headers.

Binary distributions (JAR files on Clojars and GitHub releases) bundle the following third-party projects:

- [h2o](https://github.com/h2o/h2o) is licensed under the MIT License and copyright [DeNA Co., Ltd.](http://dena.com/), [Kazuho Oku](https://github.com/kazuho/), and contributors.

- [Brotli](https://github.com/google/brotli) is licensed under the MIT License and copyright (c) 2009, 2010, 2013-2016 by the Brotli Authors.

- [Zstandard (zstd)](https://github.com/facebook/zstd) is licensed under the BSD License and copyright (c) Meta Platforms, Inc.

- [OpenSSL](https://github.com/openssl/openssl) is licensed under the Apache 2.0 License and copyright (c) 1998-2025 The OpenSSL Project Authors, and copyright (c) 1995-1998 Eric A. Young, Tim J. Hudson.




[coffi]: https://github.com/IGJoshua/coffi
