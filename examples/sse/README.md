# SSE

This example shows a long lived Server Sent Events stream that emits its own source code, one source line every second, looping forever. It lets the client opt into Brotli compression on the same route.

Run it from this directory.

```bash
clojure -M:run
```

Watch the stream, then ask for the same stream with Brotli enabled.

The stream keeps running until you stop it.

```bash
curl -N http://127.0.0.1:8082/
curl -k -N -i --http2 --compressed -H 'accept-encoding: br' --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
```

Then check the same streaming route over HTTP/2, and HTTP/3

```bash
curl -i http://127.0.0.1:8082/
curl -k -i --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
curl -k -i --http3-only --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
```
