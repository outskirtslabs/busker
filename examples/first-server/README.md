# First server

The smallest Busker server has one plain HTTP entrypoint and a Ring handler that greets each request.
It uses no certificates, TLS, or HTTP/3.

This example accompanies the "First Busker server" tutorial in the docs.

Prepare the Busker git dependency, then run the example from this directory.

```bash
clj -X:deps prep
clojure -M:run
```

It prints the server phase and the listen address.
Then send it a request.

```bash
curl -i http://127.0.0.1:8080/
```

Ask Busker for a gzip-compressed response:

```bash
curl --compressed -H 'accept-encoding: gzip' -i http://127.0.0.1:8080/
```

Stop the server with Ctrl-C.
