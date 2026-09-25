# HTTP adapter benchmark

## Run a comparison

Use the Nix development shell with Java 25+, `h2load`, and OpenSSL available.
Run from the repository root:

```sh
bb bench:test
bb bench:smoke
bb bench
bb bench --adapter busker --protocol tls-h2 --duration 30 --repetitions 3
```

`bb bench` uses pinned Git Busker and published native libraries.
To use the current checkout and its existing native build output instead:

```sh
bb build  # Only when build output is missing or needs updating; stop other runs first.
bb bench:local --adapter busker --protocol h1
bb bench:local --protocol h1  # All adapters; only Busker uses local dependencies.
bb bench:local --smoke --adapter busker
```

Local mode does not rebuild automatically.
Run it from the checkout root; it rejects a mixture of checkout source and published native libraries.
Each result records local mode, the source location, checkout revision and dirty state, and the native library SHA-256.
Local native output has no claimed release version; its recorded version is `null`.
The dirty flag includes untracked files and does not prove that native output matches the current source; rebuild when native sources change.

Results and raw logs go into a new `bench/results/<timestamp>/` directory.
Use `--output PATH` to select another new directory; existing directories are rejected.
`results.json` contains median requests per second, minimum/maximum rates, and every repetition.
A failed repetition invalidates its entire cell; unsupported cells are distinct from failures.
The command exits nonzero if any selected cell fails.

| Option | Default | Meaning |
| --- | ---: | --- |
| `--warmup` | 10 | Warmup seconds per repetition |
| `--duration` | 30 | Measured seconds per repetition |
| `--repetitions` | 3 | Fresh JVMs per supported protocol/library pair |
| `--connections` | 128 | Total concurrent request budget |
| `--streams` | 64 | Concurrent streams per H2 connection |
| `--threads` | 2 | h2load threads |

`--smoke` selects one repetition with one-second warmup and measurement.
It checks execution, not performance.
`--adapter` selects an adapter ID; `--protocol` selects `h1`, `tls-h1`, or `tls-h2`.
Connections must divide evenly into H2 streams and client threads.

## Measurement reference

Every adapter serves the same 11-byte `Hello World` response with status 200, a text content type and explicit content length.
H1 uses 128 connections without pipelining; H2 uses two connections with 64 streams each.
These settings equalize outstanding requests, not socket overhead or flow-control behavior.
TLS uses a shared RSA-2048 self-signed certificate and h2load's TLS 1.3 AES-128-GCM cipher.
The Java probe trusts the generated certificate and verifies the response, headers, TLS version and actual HTTP version before and after load.
H2 requires ALPN `h2`; H1 may operate without ALPN.

Each repetition starts a fresh JVM with `-Xms512m -Xmx2g -XX:ActiveProcessorCount=2`.
Inherited JVM option environment variables are removed from child processes.
This controls heap settings and processor-based defaults, not CPU affinity or quotas.
Warmup and measurement use separate h2load processes and connections; measurement includes connection establishment.
A fixed warmup does not establish that every implementation has reached steady state.
Adapters run sequentially with their normal execution defaults:

| Adapter ID | H1 | TLS H1 | TLS H2 | Configuration beyond bind/port |
| --- | --- | --- | --- | --- |
| `busker` | yes | yes | yes | Modern TLS; HTTP/3 disabled |
| `aleph` | yes | yes | yes | TLS advertises H2 and H1 |
| `capra` | yes | unsupported | unsupported | Defaults |
| `hirundo` | yes | yes | yes | Helidon TLS context |
| `http-exchange` | yes | yes | unsupported | JDK HTTPS context |
| `http-kit` | yes | unsupported | unsupported | Defaults |
| `jetty` | yes | yes | yes | TLS-only connector with ALPN/H2; nonjoining start |
| `undertow` | yes | yes | yes | TLS-only listener with HTTP/2 enabled |

The benchmark alias uses Git Busker and published native artifacts rather than local source/build output.
Results record the resolved Git revision, native library version/SHA-256, dependency versions, Java version and h2load version.
Load summaries must contain positive throughput, successful completed requests, no failed/errored/timed-out requests, no non-2xx statuses, and the expected protocol/TLS negotiation.
Status counts can exceed completed requests because headers can arrive before the timed interval ends while their bodies remain unfinished.
Raw stdout and stderr are separate to prevent concurrent diagnostic messages from corrupting summary parsing.
Server logs are also checked for common error markers; this is not exhaustive structured error instrumentation.
Exact body checks occur before and after load, not for every measured response.
Inspect retained logs alongside the scores.

## Run on Hetzner

```sh
bb hcloud create
bb hcloud setup
bb hcloud bench
```

The workflow creates a CCX13 host, installs the Nix daemon, and installs `bench/tools.nix` into the root Nix profile.
The tool set uses the repository's locked nixpkgs revision for Java, Clojure, h2load, OpenSSL, curl, Git and rsync.
No apt-based benchmark setup or custom source builds are used.
Upload sends tracked files from the working tree, excluding credentials, local tracking, development files and build output.
New untracked implementation files must be deliberately added to Git before upload.
The client and server share the host, so scores describe this shared-host workload, not isolated server capacity.
The default full comparison takes roughly an hour including JVM startup.
Retrieve `/busker/bench/results/` and inspect all repetitions, including failures.
The host remains running and billable until explicitly deleted with `bb hcloud delete`.
Do not delete it while retained-host analysis is in progress.
