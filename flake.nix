{
  description = "dev env";
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0.1"; # tracks nixpkgs unstable branch
    flakelight.url = "github:nix-community/flakelight";
    flakelight.inputs.nixpkgs.follows = "nixpkgs";
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };
  outputs =
    {
      self,
      flakelight,
      treefmt-nix,
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
        h2o-shared = pkgs: pkgs.callPackage ./pkgs/h2o.nix { };
        h2o-bundle =
          pkgs:
          pkgs.callPackage ./pkgs/h2o-bundle.nix {
            #h2oSrc = /home/ramblurr/src/ol/http-clj/extra/h2o;
          };
      };

      devShell =
        pkgs:
        let
          javaVersion = "25";
          jdk = pkgs."jdk${javaVersion}";
          clojure = pkgs.clojure.override { inherit jdk; };
          h2o-bundle = (self.packages.${pkgs.system}.h2o-bundle);
          libraries = [
            h2o-bundle
            #pkgs.llvmPackages.clangUseLLVM
            #pkgs.llvmPackages.llvm
            #pkgs.llvmPackages.libclang
            #pkgs.llvmPackages.stdenv
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

            self.packages.${pkgs.system}.h2o-shared

            # Development tools
            pkgs.gdb
            pkgs.clojure-lsp
            pkgs.jdt-language-server
            pkgs.clang-tools
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.babashka
            pkgs.git
            #pkgs.nghttp2 # for h2load
            #pkgs.clang
            #pkgs.llvmPackages.clangUseLLVM
          ];
          env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath libraries;
          env.PKG_CONFIG_PATH = "${h2o-bundle}/lib/pkgconfig";
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
