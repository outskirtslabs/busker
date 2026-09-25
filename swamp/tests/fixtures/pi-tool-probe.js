import { deepStrictEqual, rejects, throws } from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import restrictedTools from "../../pi/restricted-tools.js";

export default async function () {
  const root = await mkdtemp(join(tmpdir(), "tempo-sdk-tools-"));
  try {
    await mkdir(join(root, "src/main"), { recursive: true });
    const source = join(root, "src/main/code.txt");
    await writeFile(source, "before\n");
    const policyPath = join(root, "policy.json");
    const policy = { workspace: root, readRoots: [], stage: "implement" };
    await writeFile(policyPath, JSON.stringify(policy));
    const tools = new Map();
    const handlers = new Map();
    restrictedTools({
      registerFlag() {},
      getFlag: () => policyPath,
      registerTool: (tool) => tools.set(tool.name, tool),
      on: (event, handler) => handlers.set(event, handler),
      appendEntry() {},
    });
    const read = tools.get("tempo_read");
    const edit = tools.get("tempo_edit");
    const write = tools.get("tempo_write");
    await rejects(
      () => read.execute("before-init", { path: source }),
      /not loaded/,
    );
    await handlers.get("session_start")();
    const contents = await read.execute("read", { path: source });
    deepStrictEqual(
      contents.content.some((item) => item.text?.includes("before")),
      true,
    );
    await edit.execute("edit", {
      path: source,
      edits: [{ oldText: "before", newText: "after" }],
    });
    deepStrictEqual(await readFile(source, "utf8"), "after\n");
    await write.execute("write", {
      path: join(root, "src/main/new.txt"),
      content: "new\n",
    });
    deepStrictEqual(
      await readFile(join(root, "src/main/new.txt"), "utf8"),
      "new\n",
    );
    await rejects(
      () =>
        write.execute("escape", {
          path: join(root, "../escape.txt"),
          content: "forbidden",
        }),
      /outside/,
    );
    await rejects(
      () =>
        write.execute("configuration", {
          path: policyPath,
          content: "forbidden",
        }),
      /Only application/,
    );
    policy.stage = "review";
    await writeFile(policyPath, JSON.stringify(policy));
    await handlers.get("session_start")();
    await rejects(
      () =>
        edit.execute("review-edit", {
          path: source,
          edits: [{ oldText: "after", newText: "forbidden" }],
        }),
      /read-only/,
    );
    throws(() => handlers.get("user_bash")(), /cannot execute shell/);
    deepStrictEqual(await readFile(source, "utf8"), "after\n");
    await writeFile(
      process.env.TEMPO_TOOL_PROBE_REPORT,
      JSON.stringify({ checked: true, tools: [...tools.keys()] }),
    );
  } finally {
    await rm(root, { recursive: true });
  }
}
