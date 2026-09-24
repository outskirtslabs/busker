---
name: update-h2o-zig-pin
description: Updates Busker's h2o-zig Zig and Nix pins, synchronizes duplicated shim dependencies such as BoringSSL, regenerates the Zig dependency lock, and validates the result. Use when changing h2o-zig, BoringSSL, shim/build.zig.zon, shim/build.zig.zon2json-lock, or the h2o-zig flake input.
---

# Update the h2o-zig Pin

Run every project command inside the Nix devshell. Run the steps sequentially; never run another command in parallel with `bb build`.

## Pin locations

Keep these representations aligned:

- `shim/build.zig.zon`: h2o-zig URL, commit, package hash, and direct dependencies duplicated by Busker.
- `shim/build.zig.zon2json-lock`: generated Zig dependency closure.
- `flake.lock`: the independent `h2o-zig` flake input.

Use `extra/h2o-zig/` as the local upstream reference. Do not inspect dependency caches. Preserve unrelated worktree changes.

## 1. Select and inspect the target

Use the requested commit. When the request says to use the local reference HEAD:

```bash
target_rev=$(git -C extra/h2o-zig rev-parse HEAD)
git -C extra/h2o-zig show "$target_rev:build.zig.zon"
```

Confirm that the local reference contains an explicitly requested commit:

```bash
git -C extra/h2o-zig cat-file -e "$target_rev^{commit}"
```

Stop and ask for the reference material to be refreshed if `extra/h2o-zig/` is missing or lacks the requested commit.

## 2. Update `shim/build.zig.zon`

Fetch the exact h2o-zig revision from `shim/` to calculate its Zig package hash:

```bash
(
  cd shim
  zig fetch "git+https://github.com/outskirtslabs/h2o-zig.git#$target_rev"
)
```

Set the h2o-zig `.url` to that exact commit and its `.hash` to the printed hash.

Compare Busker's direct dependency entries with the target h2o-zig `build.zig.zon`. Copy changed entries that Busker already duplicates, especially BoringSSL, including the complete `.url`, `.hash`, and `.lazy` fields. Do not infer a BoringSSL commit from its version or date. Do not add every upstream dependency: Busker only declares dependencies used directly by `shim/build.zig`.

## 3. Regenerate locks

Regenerate the Zig lock from `shim/`; never edit it by hand:

```bash
(
  cd shim
  zig2nix zon2lock build.zig.zon build.zig.zon2json-lock
)
```

Update the independent flake input from the repository root:

```bash
nix flake lock --update-input h2o-zig
```

The flake input tracks upstream HEAD, so verify that it resolved to the same commit:

```bash
test "$(jq -r '.nodes["h2o-zig"].locked.rev' flake.lock)" = "$target_rev"
```

Stop rather than committing mismatched Zig and Nix revisions.

## 4. Review generated changes

Normally this update changes only:

- `shim/build.zig.zon`
- `shim/build.zig.zon2json-lock`
- `flake.lock`

Changes to transitive h2o, BoringSSL, googletest, or similar entries in the generated lock are expected when the upstream closure changed.

```bash
jq empty shim/build.zig.zon2json-lock flake.lock
git diff --check
git diff -- shim/build.zig.zon shim/build.zig.zon2json-lock flake.lock
```

## 5. Validate

Run all checks sequentially from the repository root:

```bash
nix build .#shim -L
bb build
bb test
nix flake check -L
```

Report the exact h2o-zig and BoringSSL revisions, changed files, and validation results.
