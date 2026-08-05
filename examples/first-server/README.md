# First server

This example accompanies the "Your first Busker server" tutorial.
It has one plain HTTP entrypoint and a Ring handler, with no certificates, TLS, or HTTP/3.

Prepare the Busker git dependency from this directory:

```bash
clj -X:deps prep
```

Start a REPL:

```bash
clojure -M:repl
```

Load the example and start the server:

```clojure
(require '[main :as app])
(app/start!)
```

Send a plain request from another terminal:

```bash
curl -i http://127.0.0.1:8080/
```

Ask Busker for a gzip-compressed response:

```bash
curl --compressed -H 'accept-encoding: gzip' -i http://127.0.0.1:8080/
```

Stop the server from the REPL:

```clojure
(app/stop!)
```
