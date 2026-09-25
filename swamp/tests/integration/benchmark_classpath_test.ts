import { ok } from "node:assert/strict";
import { delimiter, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

Deno.test("bench:local puts shim resources before copied native files in target/classes", async () => {
  const root = fileURLToPath(new URL("../../../", import.meta.url));
  const result = await new Deno.Command("clojure", {
    cwd: root,
    args: ["-Spath", "-A:bench:bench-local"],
  }).output();
  ok(result.success, new TextDecoder().decode(result.stderr));
  const paths = new TextDecoder().decode(result.stdout).trim().split(delimiter)
    .map((path) => resolve(root, path));
  const classes = paths.indexOf(join(root, "target/classes"));
  ok(classes >= 0, "The fixture must include the normal compiled output path");
  for (
    const platform of [
      "linux-aarch64",
      "linux-x86-64",
      "macos-aarch64",
      "macos-x86-64",
    ]
  ) {
    const native = paths.indexOf(join(root, "shim", platform, "resources"));
    ok(
      native >= 0 && native < classes,
      `${platform} native output must take precedence over target/classes`,
    );
  }
});
