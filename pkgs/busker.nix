{
  pkgs,
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
        || pkgs.lib.hasPrefix ".clj-kondo/.cache/" rel
        || pkgs.lib.hasPrefix ".cpcache/" rel
        || pkgs.lib.hasPrefix ".gitlibs/" rel
        || pkgs.lib.hasPrefix ".m2/" rel
        || pkgs.lib.hasPrefix "shim/.zig-cache/" rel
        || pkgs.lib.hasPrefix "shim/.zig-cache-global/" rel
        || pkgs.lib.hasPrefix "shim/zig-cache/" rel
        || pkgs.lib.hasPrefix "shim/zig-out/" rel
        || pkgs.lib.hasPrefix "shim/tmp/" rel
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
in
pkgs.mkCljLib {
  inherit projectSrc;
  name = "com.outskirtslabs/busker";
  version = "0.0.2";
  nativeBuildInputs = [
    pkgs.coreutils
    curlWithSsls
    pkgs.git
    pkgs.iproute2
    pkgs.openssl
    pkgs.perl
  ];
  GIT_REV = gitRev;
  JAVA_HOME = pkgs.jdk25.home;
  buildCommand = ''
    original_java_tool_options="$JAVA_TOOL_OPTIONS"
    clj_cache_home="$(printf '%s\n' "$original_java_tool_options" | tr ' ' '\n' | sed -n 's/^-Duser.home=//p' | head -n1)"

    export JAVA_HOME="${pkgs.jdk25.home}"
    export JAVA_CMD="${pkgs.jdk25}/bin/java"
    export GIT_REV="${gitRev}"
    export HOME="$TMPDIR/clj-home"
    mkdir -p "$HOME"
    if [ -n "$clj_cache_home" ] && [ -d "$clj_cache_home" ]; then
      cp -R "$clj_cache_home/.m2" "$HOME/.m2"
      cp -R "$clj_cache_home/.gitlibs" "$HOME/.gitlibs"
      cp -R -L "$clj_cache_home/.clojure" "$HOME/.clojure"
      chmod -R u+w "$HOME"
    fi
    export JAVA_TOOL_OPTIONS="$(printf '%s\n' "$original_java_tool_options" | tr ' ' '\n' | grep -v '^-Duser.home=' | tr '\n' ' ') -Duser.home=$HOME"

    mkdir -p "shim/linux-x86-64/resources/linux-x86-64"
    mkdir -p "shim/linux-aarch64/resources/linux-aarch64"
    mkdir -p "shim/macos-x86-64/resources/macos-x86-64"
    mkdir -p "shim/macos-aarch64/resources/macos-aarch64"

    cp "${shim}/linux-x86-64/libh2oclj.so" "shim/linux-x86-64/resources/linux-x86-64/"
    cp "${shim}/linux-aarch64/libh2oclj.so" "shim/linux-aarch64/resources/linux-aarch64/"
    cp "${shim}/macos-x86-64/libh2oclj.dylib" "shim/macos-x86-64/resources/macos-x86-64/"
    cp "${shim}/macos-aarch64/libh2oclj.dylib" "shim/macos-aarch64/resources/macos-aarch64/"

    clojure -Xdeps prep :aliases '[:build :dev :test]'
    clojure -M:dev:test:kaocha
    clojure -J-Xmx2g -J-Xms2g -M:dev:test:kaocha --no-capture-output --focus ol.busker.large-payload-test
    clojure -T:build jar
  '';
}
