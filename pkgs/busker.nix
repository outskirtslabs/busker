{
  pkgs,
  clojureLib,
  gitRev,
  shim,
}:
let
  jdk = pkgs.jdk25;
  clojure = pkgs.clojure.override { inherit jdk; };
  curlWithSsls = pkgs.curlFull.overrideAttrs (prev: {
    pname = prev.pname + "-ssls";
    configureFlags = prev.configureFlags or [ ] ++ [
      "--enable-ssls-export"
    ];
  });
  shimTargets = [
    {
      dir = "linux-x86-64";
      lib = "libh2oclj.so";
    }
    {
      dir = "linux-aarch64";
      lib = "libh2oclj.so";
    }
    {
      dir = "macos-x86-64";
      lib = "libh2oclj.dylib";
    }
    {
      dir = "macos-aarch64";
      lib = "libh2oclj.dylib";
    }
  ];
  copyShimLibs = pkgs.lib.concatMapStringsSep "\n" (target: ''
    mkdir -p "shim/${target.dir}/resources/${target.dir}"
    cp "${shim}/${target.dir}/${target.lib}" "shim/${target.dir}/resources/${target.dir}/"
  '') shimTargets;
  warmShimJars = pkgs.lib.concatMapStringsSep "\n" (target: ''
    (
      cd "shim/${target.dir}"
      ${clojure}/bin/clojure -Srepro -P -T:build jar || true
    )
  '') shimTargets;
in
clojureLib.mkCljLib {
  inherit pkgs jdk gitRev;
  name = "busker";
  version = "0.0.2";
  src = ../.;
  extraSrcExcludes = [
    "bb.edn"
    ".gitlibs"
    ".m2"
    "shim/.zig-cache"
    "shim/.zig-cache-global"
    "shim/zig-cache"
    "shim/zig-out"
  ]
  ++ pkgs.lib.concatMap (target: [
    "shim/${target.dir}/.cpcache"
    "shim/${target.dir}/resources"
    "shim/${target.dir}/target"
  ]) shimTargets;
  prepAliases = [
    "bench"
    "build"
    "dev"
    "test"
  ];
  checkCommand = ''
    cljfmt check src
    clj-kondo --lint src
    clojure -Srepro -M:dev:test:kaocha
  '';
  nativeBuildInputs = [
    curlWithSsls
    pkgs.iproute2
    pkgs.openssl
    pkgs.perl
    pkgs.cljfmt
    pkgs.clj-kondo
  ];
  preBuild = copyShimLibs;
  lockCommand = ''
    export HOME="$tmp/home"
    export GITLIBS="$HOME/.gitlibs"
    export JAVA_TOOL_OPTIONS="-Duser.home=$HOME"
    unset CLJ_CACHE CLJ_CONFIG XDG_CACHE_HOME XDG_CONFIG_HOME XDG_DATA_HOME

    ${clojure}/bin/clojure -Srepro -P || true
    ${clojure}/bin/clojure -Srepro -X:deps prep :aliases '[:bench :build :dev :test]'
    ${clojure}/bin/clojure -Srepro -P -M:dev:test:kaocha || true

    coffi_dir="$(find "$GITLIBS/libs" -path '*/org.suskalo/coffi/*' -type d | head -n 1)"
    if [ -n "$coffi_dir" ]; then
      (
        cd "$coffi_dir"
        ${clojure}/bin/clojure -Srepro -P || true
        ${clojure}/bin/clojure -Srepro -P -X:build compile-java || true
      )
    fi

    ${clojure}/bin/clojure -Srepro -P -T:build jar || true
    ${warmShimJars}
  '';
}
