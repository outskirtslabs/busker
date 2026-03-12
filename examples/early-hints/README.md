# Early Hints

This example shows a Busker handler that emits `103 Early Hints`, serves an HTML document, and serves the hinted CSS asset from a second route.

Run it from this directory.

```bash
clojure -M:run
```

Watch the early hints response and fetch the hinted stylesheet.

```bash
curl -k -v --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
curl -i http://127.0.0.1:8082/app.css
```

Then check the main page over HTTP/2, and HTTP/3

```bash
curl -k -i --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
curl -k -i --http3-only --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
```
