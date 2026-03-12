{ fetchzip, stdenv }:
stdenv.mkDerivation {
  name = "apple-sdk_15.2";
  src = fetchzip {
    url = "https://github.com/joseluisq/macosx-sdks/releases/download/15.2/MacOSX15.2.sdk.tar.xz";
    hash = "sha256:0fgj0pvjclq2pfsq3f3wjj39906xyj6bsgx1da933wyc918p4zi3";
  };
  phases = [ "installPhase" ];
  installPhase = ''
    mkdir -p "$out"
    cp -r "$src"/* "$out"
  '';
}
