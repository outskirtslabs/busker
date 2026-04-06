{
  pkgs,
  stdenv,
  clojureLib,
  gitRev,
  shim,
}:
let
  root = toString ../.;
  projectSrc = pkgs.lib.cleanSourceWith {
    src = ../.;
    filter =
      path: _type:
      let
        rel = pkgs.lib.removePrefix (root + "/") (toString path);
        base = builtins.baseNameOf path;
      in
      !(
        base == ".git"
        || rel == "result"
        || pkgs.lib.hasPrefix "target/" rel
        || pkgs.lib.hasPrefix "bb.edn" rel
        || pkgs.lib.hasPrefix ".clj-kondo/.cache/" rel
        || pkgs.lib.hasPrefix ".cpcache/" rel
        || pkgs.lib.hasPrefix ".gitlibs/" rel
        || pkgs.lib.hasPrefix ".m2/" rel
        || pkgs.lib.hasPrefix "shim/.zig-cache/" rel
        || pkgs.lib.hasPrefix "shim/.zig-cache-global/" rel
        || pkgs.lib.hasPrefix "shim/zig-cache/" rel
        || pkgs.lib.hasPrefix "shim/zig-out/" rel
        || pkgs.lib.hasPrefix "shim/linux-x86-64/.cpcache/" rel
        || pkgs.lib.hasPrefix "shim/linux-x86-64/resources/" rel
        || pkgs.lib.hasPrefix "shim/linux-x86-64/target/" rel
        || pkgs.lib.hasPrefix "shim/linux-aarch64/.cpcache/" rel
        || pkgs.lib.hasPrefix "shim/linux-aarch64/resources/" rel
        || pkgs.lib.hasPrefix "shim/linux-aarch64/target/" rel
        || pkgs.lib.hasPrefix "shim/macos-x86-64/.cpcache/" rel
        || pkgs.lib.hasPrefix "shim/macos-x86-64/resources/" rel
        || pkgs.lib.hasPrefix "shim/macos-x86-64/target/" rel
        || pkgs.lib.hasPrefix "shim/macos-aarch64/.cpcache/" rel
        || pkgs.lib.hasPrefix "shim/macos-aarch64/resources/" rel
        || pkgs.lib.hasPrefix "shim/macos-aarch64/target/" rel
      );
  };
  curlWithSsls = pkgs.curlFull.overrideAttrs (prev: {
    pname = prev.pname + "-ssls";
    configureFlags = prev.configureFlags or [ ] ++ [
      "--enable-ssls-export"
    ];
  });
  jdk = pkgs.jdk25;
  clojure = pkgs.clojure.override { inherit jdk; };
  clojureLocker = clojureLib.mkLockfile {
    inherit pkgs jdk;
    src = projectSrc;
    lockfile = "./deps-lock.json";
  };
in
stdenv.mkDerivation {
  pname = "busker";
  version = "0.0.2";
  src = projectSrc;
  nativeBuildInputs = [
    clojure
    jdk
    pkgs.coreutils
    curlWithSsls
    pkgs.findutils
    pkgs.git
    pkgs.iproute2
    pkgs.openssl
    pkgs.perl
  ];
  GIT_REV = gitRev;
  JAVA_HOME = jdk.home;
  buildPhase = ''
    runHook preBuild

    source ${clojureLocker.shellEnv}
    export JAVA_HOME="${jdk.home}"
    export JAVA_CMD="${jdk}/bin/java"
    export GIT_REV="${gitRev}"

    for dir in linux-x86-64 linux-aarch64 macos-x86-64 macos-aarch64; do
      case "$dir" in
        linux-*) lib_name="libh2oclj.so" ;;
        macos-*) lib_name="libh2oclj.dylib" ;;
        *) echo "Unknown shim target: $dir" >&2; exit 1 ;;
      esac

      mkdir -p "shim/$dir/resources/$dir"
      cp "${shim}/$dir/$lib_name" "shim/$dir/resources/$dir/"
    done

    clojure -Srepro -X:deps prep :aliases '[:build :dev :test]'
    clojure -Srepro -M:dev:test:kaocha
    clojure -Srepro -J-Xmx2g -J-Xms2g -M:dev:test:kaocha --no-capture-output --focus ol.busker.large-payload-test
    clojure -Srepro -T:build jar

    runHook postBuild
  '';
  installPhase = ''
    runHook preInstall

    mkdir -p "$out"
    cp "$(find target -type f -name '*.jar' -print | head -n 1)" "$out/"

    runHook postInstall
  '';
}
