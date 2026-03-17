{
  lib,
  clojure,
  git,
  jdk25,
  mk-deps-cache,
  perl,
  stdenv,
  zig2nix,
  apple-sdk,
  gitRev,
}:
let
  system = stdenv.hostPlatform.system;
  root = toString ../shim;
  zig = zig2nix.packages.${system}."zig-0_15_2";
  zig2nixEnv = zig2nix.outputs.zig-env.${system} { inherit zig; };
  deps-cache = mk-deps-cache {
    lockfile = ../deps-lock.json;
  };
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
  targetNames = lib.concatStringsSep " " (map (target: target.dir) shimTargets);
  targetChecks = lib.concatStringsSep "\n" (
    map (target: ''test -e "$out/${target.dir}/${target.lib}"'') shimTargets
  );
  jarChecks = lib.concatStringsSep "\n" (
    map (target: ''
      test -e "$out/jars/${target.dir}-$(sed -n 's/.*:version[[:space:]]*"\([^"]*\)".*/\1/p' "$src/${target.dir}/deps.edn" | head -n1).jar"
    '') shimTargets
  );
in
zig2nixEnv.package {
  pname = "busker-shim";
  version = "0.0.2";
  src = lib.cleanSourceWith {
    src = ../shim;
    filter =
      path: _type:
      let
        rel = lib.removePrefix (root + "/") (toString path);
        base = builtins.baseNameOf path;
      in
      !(
        base == ".git"
        || lib.hasPrefix ".zig-cache/" rel
        || lib.hasPrefix ".zig-cache-global/" rel
        || lib.hasPrefix "zig-cache/" rel
        || lib.hasPrefix "zig-out/" rel
        || lib.hasPrefix "linux-x86-64/.cpcache/" rel
        || lib.hasPrefix "linux-x86-64/resources/" rel
        || lib.hasPrefix "linux-x86-64/target/" rel
        || lib.hasPrefix "linux-aarch64/.cpcache/" rel
        || lib.hasPrefix "linux-aarch64/resources/" rel
        || lib.hasPrefix "linux-aarch64/target/" rel
        || lib.hasPrefix "macos-x86-64/.cpcache/" rel
        || lib.hasPrefix "macos-x86-64/resources/" rel
        || lib.hasPrefix "macos-x86-64/target/" rel
        || lib.hasPrefix "macos-aarch64/.cpcache/" rel
        || lib.hasPrefix "macos-aarch64/resources/" rel
        || lib.hasPrefix "macos-aarch64/target/" rel
      );
  };
  nativeBuildInputs = [
    clojure
    git
    jdk25
    perl
  ];
  APPLE_SDK_PATH = "${apple-sdk}";
  preBuild = ''
    export PATH="${
      lib.makeBinPath [
        git
        perl
      ]
    }:$PATH"
  '';
  zigBuildFlags = [
    "-Doptimize=Debug"
  ];
  postInstall = ''
    export HOME="${deps-cache}"
    export JAVA_TOOL_OPTIONS="-Duser.home=${deps-cache}"
    export JAVA_HOME="${jdk25.home}"
    export PATH="$JAVA_HOME/bin:$PATH"
    export GIT_REV="${gitRev}"
    cp ${../deps.edn} ../deps.edn
    mkdir -p "$out/jars"
    for dir in ${targetNames}; do
      case "$dir" in
        linux-x86-64|linux-aarch64) lib_name="libh2oclj.so" ;;
        macos-x86-64|macos-aarch64) lib_name="libh2oclj.dylib" ;;
        *) echo "Unknown shim target: $dir" >&2; exit 1 ;;
      esac
      mkdir -p "$dir/resources/$dir"
      cp "$out/$dir/$lib_name" "$dir/resources/$dir/"
      (
        cd "$dir"
        clj -T:build jar
      )
      cp "$dir/target/"*.jar "$out/jars/"
    done

    ${targetChecks}
    ${jarChecks}
  '';
}
