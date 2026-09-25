import { deepStrictEqual } from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { z } from "npm:zod@4.4.3";

Deno.test("installed Pi file tools enforce Tempo restrictions without starting an inference", async () => {
  const root = await Deno.makeTempDir({ prefix: "tempo-tool-probe-" });
  try {
    const report = join(root, "report.json");
    const result = await new Deno.Command("pi", {
      args: [
        "--offline",
        "--no-extensions",
        "--no-skills",
        "--no-prompt-templates",
        "--no-context-files",
        "--no-themes",
        "--no-approve",
        "--no-builtin-tools",
        "--extension",
        fileURLToPath(new URL("../fixtures/pi-tool-probe.js", import.meta.url)),
        "--mode",
        "rpc",
        "--no-session",
      ],
      env: { PI_CODING_AGENT_DIR: root, TEMPO_TOOL_PROBE_REPORT: report },
      stdin: "null",
    }).output();
    deepStrictEqual(
      result.success,
      true,
      new TextDecoder().decode(result.stderr),
    );
    let data;
    try {
      data = z.strictObject({
        checked: z.boolean(),
        tools: z.array(z.string()),
      }).parse(JSON.parse(await Deno.readTextFile(report)));
    } catch (cause) {
      throw new Error(
        `Pi tool probe did not complete: ${
          new TextDecoder().decode(result.stderr)
        }\n${new TextDecoder().decode(result.stdout).slice(0, 3000)}`,
        { cause },
      );
    }
    deepStrictEqual(data, {
      checked: true,
      tools: ["tempo_read", "tempo_edit", "tempo_write", "tempo_repl_eval"],
    });
  } finally {
    await Deno.remove(root, { recursive: true });
  }
});
