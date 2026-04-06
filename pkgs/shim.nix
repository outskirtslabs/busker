{
  pkgs,
  lib,
  clojureLib,
  git,
  jdk25,
  perl,
  stdenv,
  zig2nix,
  apple-sdk,
  gitRev,
}:
let
  system = stdenv.hostPlatform.system;
  root = toString ../.;
  clojure = pkgs.clojure.override { jdk = jdk25; };
  zig = zig2nix.packages.${system}."zig-0_15_2";
  zig2nixEnv = zig2nix.outputs.zig-env.${system} { inherit zig; };
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
      test -e "$out/jars/${target.dir}-$(sed -n 's/.*:version[[:space:]]*"\([^"]*\)".*/\1/p' "$src/shim/${target.dir}/deps.edn" | head -n1).jar"
    '') shimTargets
  );
  excludedShimPrefixes =
    [
      "shim/.zig-cache/"
      "shim/.zig-cache-global/"
      "shim/zig-cache/"
      "shim/zig-out/"
    ]
    ++ lib.concatMap (target: [
      "shim/${target.dir}/.cpcache/"
      "shim/${target.dir}/resources/"
      "shim/${target.dir}/target/"
    ]) shimTargets;
  filteredSrc = lib.cleanSourceWith {
    src = ../.;
    filter =
      path: _type:
      let
        rel = lib.removePrefix (root + "/") (toString path);
        base = builtins.baseNameOf path;
      in
      !(
        base == ".git"
        || !(
          rel == "deps-lock.json"
          || rel == "deps.edn"
          || rel == "shim"
          || rel == "src"
          || rel == "src/shim"
          || lib.hasPrefix "src/shim/" rel
          || (
            lib.hasPrefix "shim/" rel
            && !(lib.any (prefix: lib.hasPrefix prefix rel) excludedShimPrefixes)
          )
        )
      );
  };
  clojureLocker = clojureLib.mkLockfile {
    inherit pkgs;
    jdk = jdk25;
    src = filteredSrc;
    lockfile = "./deps-lock.json";
  };
in
zig2nixEnv.package {
  pname = "busker-shim";
  version = "0.0.2";
  src = filteredSrc;
  sourceRoot = "source/shim";
  zigBuildZon = "${filteredSrc}/shim/build.zig.zon";
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
    source ${clojureLocker.shellEnv}
    export JAVA_HOME="${jdk25.home}"
    export JAVA_CMD="${jdk25}/bin/java"
    export PATH="$JAVA_HOME/bin:$PATH"
    export GIT_REV="${gitRev}"
    if [ ! -f ../deps.edn ]; then
      cp ${../deps.edn} ../deps.edn
    fi
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
        clojure -Srepro -T:build jar
      )
      cp "$dir/target/"*.jar "$out/jars/"
    done

    ${targetChecks}
    ${jarChecks}
  '';
}
