import { deepStrictEqual, rejects } from "node:assert/strict";
import { join } from "node:path";
import { authorizePath, loadPolicy } from "../../pi/guard.ts";

Deno.test("Pi file policy denies commands, traversal, symlinks, hard links, and protected writes", async () => {
  const directory = await Deno.makeTempDir({ prefix: "tempo-policy-fixture-" });
  try {
    const workspace = join(directory, "work");
    const references = join(directory, "references");
    await Deno.mkdir(join(workspace, "src/main"), { recursive: true });
    await Deno.mkdir(references);
    await Deno.writeTextFile(join(workspace, "src/main/code.clj"), "initial");
    await Deno.writeTextFile(join(references, "api.md"), "reference");
    const policyPath = join(directory, "policy.json");
    await Deno.writeTextFile(
      policyPath,
      JSON.stringify({
        workspace,
        readRoots: [references],
        stage: "implement",
      }),
    );
    const policy = await loadPolicy(policyPath);
    deepStrictEqual(
      await authorizePath(policy, "tempo_edit", "src/main/code.clj"),
      join(workspace, "src/main/code.clj"),
    );
    deepStrictEqual(
      await authorizePath(policy, "tempo_write", "src/main/new/file.clj"),
      join(workspace, "src/main/new/file.clj"),
    );
    deepStrictEqual(
      await authorizePath(policy, "tempo_read", join(references, "api.md")),
      join(references, "api.md"),
    );
    for (const tool of ["bash", "powershell", "read", "write", "unknown"]) {
      await rejects(
        () => authorizePath(policy, tool, "src/main/code.clj"),
        /not permitted/,
      );
    }
    for (
      const path of [
        "../policy.json",
        "/etc/passwd",
        "@/etc/passwd",
        "~/.ssh/id_ed25519",
        ".git/config",
        "bench/config.edn",
        "swamp/prompts/system.md",
        "src/bb/tasks.clj",
      ]
    ) {
      await rejects(() => authorizePath(policy, "tempo_write", path));
    }
    await Deno.symlink(references, join(workspace, "src/main/escape"));
    await rejects(
      () => authorizePath(policy, "tempo_read", "src/main/escape/api.md"),
      /Symlinks/,
    );
    await rejects(
      () => authorizePath(policy, "tempo_write", "src/main/escape/new.md"),
      /Symlinks/,
    );
    await Deno.link(
      join(references, "api.md"),
      join(workspace, "src/main/alias.md"),
    );
    await rejects(
      () => authorizePath(policy, "tempo_edit", "src/main/alias.md"),
      /Hard-linked/,
    );
    for (const stage of ["analyze", "research", "review"] as const) {
      await rejects(
        () =>
          authorizePath(
            { ...policy, stage },
            "tempo_edit",
            "src/main/code.clj",
          ),
        /read-only/,
      );
    }
    deepStrictEqual(
      await Deno.readTextFile(join(references, "api.md")),
      "reference",
    );
    await Deno.writeTextFile(policyPath, "{}");
    await rejects(() => loadPolicy(policyPath), /Invalid Tempo policy/);
  } finally {
    await Deno.remove(directory, { recursive: true });
  }
});
