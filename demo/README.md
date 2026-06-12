# Busker demo

This directory contains a minimal Busker application for `busker.outskirtslabs.com`.


The application runs from `/var/lib/busker` on the demo host.


It starts HTTP on port `80` for ACME HTTP-01 challenges and HTTPS on port `443` for normal traffic.


It uses Busker's managed certificate configuration with Let's Encrypt production ACME by default.


## Local checks

Run the handler tests from this directory.


```bash
clojure -M:test
```


Run the app locally with unprivileged ports if you only want to verify startup.


```bash
BUSKER_HTTP_BIND=127.0.0.1:8080 \
BUSKER_HTTPS_BIND=127.0.0.1:8443 \
BUSKER_STORAGE_ROOT=target/acme \
clojure -M:run
```


## Deploy

Deploy from this directory.


```bash
./deploy.sh
```


The script connects to `root@ol-busker-demo`, creates the `busker` system user, installs the application under `/var/lib/busker`, installs `busker-demo.service`, prepares Clojure dependencies as the `busker` user, and restarts the service.

