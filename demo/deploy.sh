#!/usr/bin/env bash
set -euo pipefail

REMOTE=${REMOTE:-root@ol-busker-demo}
APP_DIR=/var/lib/busker
SERVICE_NAME=busker-demo.service
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

require_local_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing local tool: $1" >&2
    exit 1
  fi
}

require_local_tool ssh
require_local_tool rsync

ssh "$REMOTE" 'bash -se' <<'REMOTE_PREP'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
if ! apt-cache show openjdk-25-jdk-headless >/dev/null 2>&1; then
  echo "openjdk-25-jdk-headless is not available from apt" >&2
  exit 1
fi
apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  git \
  openssh-client \
  openjdk-25-jdk-headless \
  rsync
if [ -x /usr/lib/jvm/java-25-openjdk-amd64/bin/java ]; then
  update-alternatives --set java /usr/lib/jvm/java-25-openjdk-amd64/bin/java
fi
if ! [ -x /usr/local/bin/clojure ] \
  || ! /usr/local/bin/clojure -Sdescribe >/dev/null 2>&1; then
  tmpdir=$(mktemp -d)
  curl -fsSL -o "$tmpdir/linux-install.sh" \
    https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh
  bash "$tmpdir/linux-install.sh"
  rm -rf "$tmpdir"
fi
if ! getent group busker >/dev/null; then
  groupadd --system busker
fi
if ! id -u busker >/dev/null 2>&1; then
  useradd --system --gid busker --home-dir /var/lib/busker \
    --create-home --shell /usr/sbin/nologin busker
fi
install -d -o busker -g busker -m 0755 /var/lib/busker
install -d -o busker -g busker -m 0755 /var/cache/busker
install -d -o busker -g busker -m 0700 /var/lib/busker/.ssh
ssh-keyscan github.com >/var/lib/busker/.ssh/known_hosts 2>/dev/null
chown busker:busker /var/lib/busker/.ssh/known_hosts
chmod 0644 /var/lib/busker/.ssh/known_hosts
runuser -u busker -- env HOME=/var/lib/busker git config --global \
  url.git@github.com:outskirtslabs/busker.insteadOf \
  https://github.com/outskirtslabs/busker
/usr/local/bin/clojure -Sdescribe >/dev/null
java_spec=$(java -XshowSettings:properties -version 2>&1 \
  | awk -F'= ' '/java.specification.version/ {print $2; exit}')
case "$java_spec" in
  2[5-9]|[3-9][0-9]*) ;;
  *) echo "Java 25 or newer is required, found $java_spec" >&2; exit 1 ;;
esac
REMOTE_PREP

rsync -az --delete --chown=busker:busker --chmod=D755,F644 \
  "$SCRIPT_DIR/src/" "$REMOTE:$APP_DIR/src/"
rsync -az --chown=busker:busker --chmod=F644 \
  "$SCRIPT_DIR/deps.edn" "$REMOTE:$APP_DIR/deps.edn"
rsync -az --chown=root:root --chmod=F644 \
  "$SCRIPT_DIR/systemd/$SERVICE_NAME" "$REMOTE:/etc/systemd/system/$SERVICE_NAME"

ssh "$REMOTE" 'bash -se' <<'REMOTE_DEPLOY'
set -euo pipefail
chown -R busker:busker /var/lib/busker/src /var/lib/busker/deps.edn
find /var/lib/busker/src -type d -exec chmod 0755 {} +
find /var/lib/busker/src -type f -exec chmod 0644 {} +
chmod 0644 /var/lib/busker/deps.edn
rm -rf /var/lib/busker/native
rm -rf /var/lib/busker/.cpcache
rm -rf /var/lib/busker/.gitlibs/_repos/https/github.com/outskirtslabs/busker
systemd-analyze verify /etc/systemd/system/busker-demo.service
systemctl daemon-reload
runuser -u busker -- env HOME=/var/lib/busker \
  JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 bash -lc \
  'cd /var/lib/busker && /usr/local/bin/clojure -X:deps prep'
runuser -u busker -- env HOME=/var/lib/busker \
  JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 bash -lc \
  'cd /var/lib/busker && /usr/local/bin/clojure -P -M:run'
systemctl enable busker-demo.service
systemctl restart busker-demo.service
health_body=$(mktemp)
ready=0
for _ in $(seq 1 60); do
  if systemctl is-active --quiet busker-demo.service \
    && curl -fsS --max-time 2 -H 'Host: busker.outskirtslabs.com' http://127.0.0.1/ >"$health_body" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  systemctl --no-pager --full status busker-demo.service || true
  journalctl -u busker-demo.service -n 100 --no-pager || true
  rm -f "$health_body"
  exit 1
fi
cat "$health_body"
rm -f "$health_body"
systemctl --no-pager --full status busker-demo.service
REMOTE_DEPLOY
