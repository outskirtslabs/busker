# Native shim

This directory builds the shared libraries from [../src/shim/](../src/shim/).
Use Zig 0.17.1 and Perl, supplied by `nix develop` at the repository root.
The devshell also supplies the macOS SDK for cross-compilation.

From the repository root:

```bash
bb build:shim       # all four Linux/macOS, x86-64/AArch64 libraries
bb fmt && bb qa     # full project validation; do not run another build concurrently
```

Every bb task that builds the shim, including the native jars and their
publication, builds it as ReleaseSafe, and so does the Nix package: optimised,
with Zig's undefined-behaviour checks and the C asserts kept. Pass the
`busker.zig.optimize` system property to bb to pick another Zig mode for the
task and everything it depends on:

```bash
bb -Dbusker.zig.optimize=Debug build:shim
bb -Dbusker.zig.optimize=Debug qa
```

Debug builds also print the shim's debug log to stderr and check a few more
invariants with asserts.

In ReleaseSafe a failed undefined-behaviour check stops the process with a
bare SIGILL, while Debug prints which check failed and where, so reproduce a
crash of a published jar with a Debug build.

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

The devshell's `zig2nix` command runs without `ZIG_LOCAL_PKG_DIR`:
`zig2nix zon2lock` fetches each package into the `zig-pkg` directory of a
temporary directory it creates, and fails if Zig unpacks it elsewhere.

These source updates do not replace already-published native JARs. Build from
this checkout to exercise the updated dependencies; publishing a new native
artifact version is a separate release step.
