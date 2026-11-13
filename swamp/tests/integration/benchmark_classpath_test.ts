import { ok } from "node:assert/strict";
import { delimiter, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

Deno.test("bench:local loads native libraries from shim resources, not target/classes", async () => {
  const root = fileURLToPath(new URL("../../../", import.meta.url));
  const result = await new Deno.Command("clojure", {
    cwd: root,
    args: ["-Spath", "-A:bench:bench-local"],
  }).output();
  ok(result.success, new TextDecoder().decode(result.stderr));
  const paths = new TextDecoder().decode(result.stdout).trim().split(delimiter)
    .map((path) => resolve(root, path));
  ok(
    !paths.includes(join(root, "target/classes")),
    "target/classes holds copies made by the last jar build and must stay off the classpath",
  );
  for (
    const platform of [
      "linux-aarch64",
      "linux-x86-64",
      "macos-aarch64",
      "macos-x86-64",
    ]
  ) {
    ok(
      paths.includes(join(root, "shim", platform, "resources")),
      `${platform} native output must be on the classpath`,
    );
  }
});
