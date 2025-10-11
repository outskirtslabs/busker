{
  projectRootFile = "flake.nix";
  programs = {
    clang-format.enable = true;
    deadnix.enable = true;
    statix.enable = true;
    shellcheck.enable = true;
    cljfmt.enable = true;
    nixfmt = {
      enable = true;
      strict = true;
    };
  };
  settings = {
    global.excludes = [
      "*.envrc"
      "extra"
      "archive"
    ];
    formatter = { };
  };
}
