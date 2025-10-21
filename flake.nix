{
  description = "dev env";
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    flakelight.url = "github:nix-community/flakelight";
    flakelight.inputs.nixpkgs.follows = "nixpkgs";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    zig.url = "github:mitchellh/zig-overlay";
  };
  outputs =
    {
      self,
      flakelight,
      treefmt-nix,
      zig,
      ...
    }:
    let
      treefmtEval = pkgs: treefmt-nix.lib.evalModule pkgs ./.treefmt.nix;
    in
    flakelight ./. {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      nixpkgs.config = {
        allowUnsupportedSystem = true;
      };
      legacyPackages = pkgs: pkgs;
      packages = {
        apple-sdk =
          pkgs:
          pkgs.stdenv.mkDerivation {
            name = "apple-sdk_15.2";
            src = pkgs.fetchzip {
              url = "https://github.com/joseluisq/macosx-sdks/releases/download/15.2/MacOSX15.2.sdk.tar.xz";
              sha256 = "sha256:0fgj0pvjclq2pfsq3f3wjj39906xyj6bsgx1da933wyc918p4zi3";
            };
            phases = [ "installPhase" ];
            installPhase = ''
              mkdir -p "$out"
              cp -r "$src"/* "$out"
              ls "$out"
            '';
          };
      };

      devShell =
        pkgs:
        let
          javaVersion = "25";
          jdk = pkgs."jdk${javaVersion}";
          clojure = pkgs.clojure.override { inherit jdk; };
          zigpkgs = zig.packages.${pkgs.system};
          apple-sdk = (self.packages.${pkgs.system}.apple-sdk);
          libraries = [
          ];
        in
        {
          packages = [
            # Java Clojure
            clojure
            jdk
            pkgs.curl

            # H2O build dependencies from it's package.nix
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
            pkgs.makeWrapper
            pkgs.brotli
            pkgs.openssl
            pkgs.libcap
            pkgs.libuv
            pkgs.perl
            pkgs.zlib
            pkgs.wslay
            pkgs.bison
            pkgs.ruby
            pkgs.liburing
            # Development tools
            zigpkgs."0.15.2"
            pkgs.gdb
            pkgs.clojure-lsp
            pkgs.jdt-language-server
            pkgs.clang-tools
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.babashka
            pkgs.git
            apple-sdk

            # HTTP Benchmarking
            pkgs.wrk
            pkgs.nghttp2 # provides h2load
          ];
          env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath libraries;
          env.APPLE_SDK_PATH = "${apple-sdk}";
          env.ZIG_GLOBAL_CACHE_DIR = ".zig-cache-global";
        };

      flakelight.builtinFormatters = false;
      formatter =
        pkgs:
        let
          trfmt = treefmtEval pkgs;
        in
        trfmt.config.build.wrapper;
    };
}
