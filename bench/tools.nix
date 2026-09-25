let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);
  source = lock.nodes.nixpkgs.locked;
  pkgs = import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${source.rev}.tar.gz";
    sha256 = source.narHash;
  }) { };
in
pkgs.buildEnv {
  name = "busker-benchmark-tools";
  paths = with pkgs; [
    jdk25
    (clojure.override { jdk = jdk25; })
    nghttp2
    openssl
    curl
    git
    rsync
    coreutils
  ];
}
