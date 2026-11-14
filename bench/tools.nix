# Benchmark tools from the nixpkgs that the flake's root `nixpkgs` input
# resolves to in flake.lock, so the benchmark host runs the devshell's JDK,
# Clojure CLI and curl.
let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);
  source = lock.nodes.${lock.nodes.root.inputs.nixpkgs}.locked;
  url =
    if source.type == "tarball" then
      source.url
    else if source.type == "github" then
      "https://github.com/${source.owner}/${source.repo}/archive/${source.rev}.tar.gz"
    else
      throw "bench/tools.nix: the root nixpkgs input in flake.lock has type '${source.type}'; only 'tarball' and 'github' are supported";
  pkgs = import (builtins.fetchTarball {
    inherit url;
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
