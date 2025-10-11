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
        # h2o-shared = pkgs:
        # pkgs.h2o.overrideAttrs (oldAttrs: {
        #   cmakeFlags = (oldAttrs.cmakeFlags or [ ]) ++ [ "-DBUILD_SHARED_LIBS=ON" ];
        #   # Disable multiple outputs to avoid cyclic dependency with shared libs
        #   outputs = [ "out" ];
        # });
        #h2o-static =
        #  pkgs:
        #  pkgs.h2o.overrideAttrs (oldAttrs: {
        #    cmakeFlags = (oldAttrs.cmakeFlags or [ ]) ++ [
        #      "-DDISABLE_LIBUV=ON" # Use evloop instead of libuv
        #    ];
        #    # Disable multiple outputs to avoid cyclic dependency with shared libs
        #    outputs = [ "out" ];
        #  });
        #clj-h2o-shim =
        #  pkgs: pkgs.callPackage ./libs/h2o/package.nix { h2o = self.packages.${pkgs.system}.h2o-static; };
      };

      devShell =
        pkgs:
        let
          javaVersion = "25";
          jdk = pkgs."jdk${javaVersion}";
          clojure = pkgs.clojure.override { inherit jdk; };

          inherit (self.packages.${pkgs.system}) h2o-shared;
          #clj-h2o-shim = self.packages.${pkgs.system}.clj-h2o-shim;
          libraries = [
            #clj-h2o-shim
            h2o-shared
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
            pkgs.valgrind
            pkgs.clojure-lsp
            pkgs.jdt-language-server
            pkgs.clang-tools
            pkgs.clj-kondo
            pkgs.cljfmt
            pkgs.babashka
            pkgs.git
            pkgs.nghttp2 # for h2load
          ];
          env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath libraries;
          #env.PKG_CONFIG_PATH = "${h2o-shared}/lib/pkgconfig:${clj-h2o-shim}/lib/pkgconfig";
          env.PKG_CONFIG_PATH = "${h2o-shared}/lib/pkgconfig";
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
