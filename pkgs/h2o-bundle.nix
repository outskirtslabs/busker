{
  lib,
  pkgsStatic,
  stdenv,
  fetchFromGitHub,
  pkg-config,
  cmake,
  makeWrapper,
  ninja,
  perl,
  withMruby ? false,
  bison,
  ruby,
  nixosTests ? null,
}:

let
  # Use static versions of dependencies
  staticDeps = with pkgsStatic; {
    inherit brotli openssl zlib wslay;
  } // lib.optionalAttrs stdenv.hostPlatform.isLinux {
    inherit libcap liburing;
  };

  isDarwin = stdenv.hostPlatform.isDarwin;
  isLinux = stdenv.hostPlatform.isLinux;
in

stdenv.mkDerivation (finalAttrs: {
  pname = "h2o-bundle";
  version = "2.3.0-rolling-2025-09-24";

  src = fetchFromGitHub {
    owner = "h2o";
    repo = "h2o";
    rev = "74012bb501f14e61e5ecc1e9860bd66ba6789e0d";
    hash = "sha256-zEibiI3BdhaTty5vZ3PPXTbHIRLsE2iUiwI6hRZfy8A=";
  };

  outputs = [ "out" ];

  nativeBuildInputs = [
    pkg-config
    cmake
    makeWrapper
    ninja
  ]
  ++ lib.optionals withMruby [
    bison
    ruby
  ];

  buildInputs = [
    staticDeps.brotli
    staticDeps.openssl
    perl
    staticDeps.zlib
    staticDeps.wslay
  ]
  ++ lib.optional isLinux staticDeps.libcap
  ++ lib.optional isLinux staticDeps.liburing;

  cmakeFlags = [
    # Keep h2o output as shared lib (.so/.dylib) for Java System.load()
    "-DBUILD_SHARED_LIBS=ON"
    "-DWITH_MRUBY=${if withMruby then "ON" else "OFF"}"
    # Prefer static libraries for dependencies - platform-specific extensions
    "-DCMAKE_FIND_LIBRARY_SUFFIXES=${if isDarwin then ".a;.dylib" else ".a;.so"}"
    # Force static linking where possible
    "-DOPENSSL_USE_STATIC_LIBS=TRUE"
    "-DZLIB_USE_STATIC_LIBS=TRUE"
  ];

  # Add missing brotlicommon library (pkg-config for brotli forgets this dependency)
  NIX_LDFLAGS = "-lbrotlicommon";

  postInstall = ''
    EXES="$(find "$out/share/h2o" -type f -executable)"
    for exe in $EXES; do
      wrapProgram "$exe" \
        --set "H2O_PERL" "${lib.getExe perl}" \
        --prefix "PATH" : "${lib.getBin staticDeps.openssl}/bin"
    done
  '';

  passthru = lib.optionalAttrs (nixosTests != null) {
    tests = { inherit (nixosTests) h2o; };
  };

  meta = with lib; {
    description = "Optimized HTTP/1.x, HTTP/2, HTTP/3 server (statically bundled)";
    homepage = "https://h2o.examp1e.net";
    license = licenses.mit;
    maintainers = with maintainers; [
      toastal
      thoughtpolice
    ];
    mainProgram = "h2o";
    platforms = platforms.linux ++ platforms.darwin;
  };
})
