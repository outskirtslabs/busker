{
  description = "Busker development environment and package";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1";
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
    devenv.url = "github:ramblurr/nix-devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
    clj-helpers.url = "github:outskirtslabs/clojure-nix-locker-helpers";
    clj-helpers.inputs.nixpkgs.follows = "nixpkgs";
    zig2nix.url = "github:Cloudef/zig2nix";
    zig2nix.inputs.nixpkgs.follows = "nixpkgs";
    h2o-zig.url = "github:outskirtslabs/h2o-zig";
    h2o-zig.inputs.nixpkgs.follows = "nixpkgs";
    h2o-zig.inputs.zig2nix.follows = "zig2nix";
  };

  outputs =
    inputs@{
      self,
      devenv,
      devshell,
      clj-helpers,
      h2o-zig,
      zig2nix,
      ...
    }:
    devenv.lib.mkFlake ./. {
      inherit inputs;
      systems = [
        "x86_64-linux"
        #"aarch64-linux"
        #"x86_64-darwin"
        #"aarch64-darwin"
      ];
      nixpkgs.config.allowUnsupportedSystem = true;
      withOverlays = [
        devshell.overlays.default
        devenv.overlays.default
      ];

      packages =
        let
          clojureLib = clj-helpers.lib;
          gitRev =
            if self ? rev then
              self.rev
            else if self ? dirtyRev then
              self.dirtyRev
            else
              "dirty";
        in
        rec {
          apple-sdk = pkgs: pkgs.callPackage ./pkgs/apple-sdk.nix { };
          locker = pkgs: (busker pkgs).locker;
          shim =
            pkgs:
            pkgs.callPackage ./pkgs/shim.nix {
              inherit clojureLib gitRev zig2nix;
              apple-sdk = self.packages.${pkgs.system}.apple-sdk;
            };
          busker =
            pkgs:
            pkgs.callPackage ./pkgs/busker.nix {
              inherit clojureLib gitRev;
              shim = self.packages.${pkgs.system}.shim;
            };
          default = busker;
        };

      devShell =
        pkgs:
        let
          zig = zig2nix.packages.${pkgs.system}."zig-0_16_0";
          zig2nixEnv = zig2nix.outputs.zig-env.${pkgs.system} { inherit zig; };
          apple-sdk = self.packages.${pkgs.system}.apple-sdk;
          javaVersion = "25";
        in
        pkgs.devshell.mkShell {
          imports = [
            devenv.capsules.base
            devenv.capsules.clojure
          ];
          commands = [
            {
              package = self.packages.${pkgs.system}.locker;
            }
          ];
          packages = [
            pkgs.pebble
            pkgs.cfssl
            (pkgs.curlFull.overrideAttrs (prev: {
              pname = prev.pname + "-ssls";
              configureFlags = prev.configureFlags or [ ] ++ [
                "--enable-ssls-export"
              ];
            }))
            pkgs.cmake
            pkgs.ninja
            pkgs.mermaid-cli
            pkgs.pkg-config
            pkgs.makeWrapper
            pkgs.brotli
            pkgs.openssl
            pkgs.libcap
            pkgs.libuv
            pkgs.perl
            pkgs.zlib
            pkgs.zstd
            pkgs.wslay
            pkgs.bison
            pkgs.ruby
            pkgs.liburing
            zig
            zig2nixEnv.zig2nix
            pkgs.gdb
            pkgs.clojure-lsp
            pkgs.jdt-language-server
            pkgs.clang-tools
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.bbin
            pkgs.git
            apple-sdk
            pkgs.wrk
            pkgs.nghttp2
          ];
          env = [
            {
              name = "APPLE_SDK_PATH";
              value = "${apple-sdk}";
            }
            {
              name = "ZIG_GLOBAL_CACHE_DIR";
              value = ".zig-cache-global";
            }
          ];
        };
    };
}
