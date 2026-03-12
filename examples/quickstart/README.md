# Quickstart

This example shows the smallest useful Busker server with one plain HTTP entrypoint, one TLS entrypoint, a hello route, a request body echo route, and a large streaming response.

Run it from this directory.

```bash
clojure -M:run
```

Try the hello, echo, and large routes.

```bash
curl -i http://127.0.0.1:8082/
curl -i -H 'content-type: text/plain' --data 'ping' http://127.0.0.1:8082/echo
curl -D - -o /dev/null http://127.0.0.1:8082/large
```

Then check the same route over HTTP/2, and HTTP/3

```bash
curl -k -i --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
curl -k -i --http3-only --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
```
