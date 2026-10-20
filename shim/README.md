# Native shim

This directory builds the shared libraries from [../src/shim/](../src/shim/).
Use **Zig 0.17.0** and Perl, supplied by `nix develop` at the repository root.
The devshell also supplies the macOS SDK for cross-compilation.

From the repository root:

```bash
bb build:shim       # all four Linux/macOS, x86-64/AArch64 libraries
bb fmt && bb qa     # full project validation; do not run another build concurrently
```

From this directory:

```bash
zig build -Dall-targets=false  # native library only
zig build test                # native QUIC forwarding regression tests
```

The forwarding tests use real socketpairs and the shim's callbacks. They also
run during normal shim builds, including `bb build:shim` and Nix packaging.

## Dependency updates

Follow [the pin-update instructions](../.agents/skills/update-h2o-zig-pin/SKILL.md).
Keep `build.zig.zon`, `build.zig.zon2json-lock`, and the independent `h2o-zig`
flake input aligned on one published commit. Generate hashes with `zig fetch`
and regenerate the lock with `zig2nix`; do not hand-edit hashes.

The pinned zig2nix fetch code does not yet support Zig 0.17. The devshell's
`zig2nix` command and Nix dependency fetches therefore use Zig 0.16 with a
separate cache. **Compilation and tests use Zig 0.17.** Do not mix the fetch
cache with the compiler cache: the versions emit different package tarballs.

These source updates do not replace already-published native JARs. Build from
this checkout to exercise the updated dependencies; publishing a new native
artifact version is a separate release step.
