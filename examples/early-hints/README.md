# Early Hints

This example shows how a Busker handler can emit `103 Early Hints`
before the final response is ready.

[`103 Early Hints`][eh] is an informational response that lets the server tell
the client which subresources it will probably need, such as CSS, before the
final `200` response arrives.

That gives the client a chance to start fetching those assets earlier, which can
reduce the time spent waiting for render-blocking resources.

In this example, the `/` route sends an early hint with:

```http
Link: </app.css>; rel=preload; as=style
```

It then sends the real HTML page, and that page includes
`<link rel="stylesheet" href="/app.css">`.

The stylesheet is still served as a normal asset from `/app.css`. Nothing is
pushed automatically. The early hint is only a signal that the client can act
on sooner.

The protocol behavior differs on purpose:

- Over HTTP/2 and HTTP/3, the handler emits `103`, flushes it, waits
  briefly, and then sends the final `200` HTML response.
  With `curl -v`, you should see both responses.
- Over HTTP/1.1, the handler skips early hints entirely and sends only
  the final `200` response.
  The page still works the same way, but the client only discovers the
  stylesheet when it receives and parses the HTML.

Run it from this directory.

```bash
clojure -M:run
```

Watch the early hints response and fetch the hinted stylesheet.

```bash
# you will see 2 responses
curl -k -v --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/

# only 1 response
curl -i http://127.0.0.1:8082
```

Then check the main page over HTTP/2, and HTTP/3

```bash
curl -k -i --http2 --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
curl -k -i --http3-only --resolve localhost.examp1e.net:8443:127.0.0.1 https://localhost.examp1e.net:8443/
```

[eh]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/103
