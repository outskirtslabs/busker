{
  pkgs,
  clojureLib,
}:
let
  jdk = pkgs.jdk25;
  clojure = pkgs.clojure.override { inherit jdk; };
  shimDirs = [
    "shim/linux-x86-64"
    "shim/linux-aarch64"
    "shim/macos-x86-64"
    "shim/macos-aarch64"
  ];
  warmShimJars = pkgs.lib.concatMapStringsSep "\n" (dir: ''
    (
      cd "${dir}"
      ${clojure}/bin/clojure -Srepro -P -T:build jar || true
    )
  '') shimDirs;
  clojureLocker = clojureLib.mkLocker {
    inherit pkgs jdk;
    src = ../.;
    lockfile = "./deps-lock.json";
    command = ''
      export HOME="$tmp/home"
      export GITLIBS="$HOME/.gitlibs"
      export JAVA_TOOL_OPTIONS="-Duser.home=$HOME"
      unset CLJ_CACHE CLJ_CONFIG XDG_CACHE_HOME XDG_CONFIG_HOME XDG_DATA_HOME

      ${clojure}/bin/clojure -Srepro -P || true
      ${clojure}/bin/clojure -Srepro -X:deps prep :aliases '[:build :dev :test]'
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
  };
in
clojureLocker.locker
