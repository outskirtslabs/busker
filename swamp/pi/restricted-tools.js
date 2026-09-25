import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { join } from "node:path";
import { Type } from "@earendil-works/pi-ai";
import {
  createEditTool,
  createReadTool,
  createWriteTool,
} from "@earendil-works/pi-coding-agent";
import { authorizePath, loadPolicy } from "./guard.ts";

export default function (pi) {
  pi.registerFlag("tempo-policy", {
    description: "Path to Swamp's immutable per-step file-access policy",
    type: "string",
  });
  let policy = null;
  for (
    const tool of [
      createReadTool(process.cwd()),
      createEditTool(process.cwd()),
      createWriteTool(process.cwd()),
    ]
  ) {
    const name = `tempo_${tool.name}`;
    pi.registerTool({
      ...tool,
      name,
      async execute(id, params, signal, onUpdate) {
        if (policy === null) throw new Error("Tempo policy has not loaded");
        const path = await authorizePath(policy, name, params.path);
        return tool.execute(id, { ...params, path }, signal, onUpdate);
      },
    });
  }
  let evaluation = Promise.resolve();
  pi.registerTool({
    name: "tempo_repl_eval",
    label: "Worktree REPL",
    description:
      "Evaluate Clojure forms with brepl in the experiment worktree. Reload changed namespaces before focused tests. Diagnostic feedback does not replace Swamp QA.",
    parameters: Type.Object({
      form: Type.String({ description: "Clojure forms to evaluate" }),
    }),
    async execute(_id, { form }) {
      if (policy === null || !["implement", "repair"].includes(policy.stage)) {
        throw new Error("REPL evaluation is limited to implement and repair");
      }
      if (!form.trim() || form.length > 65536) {
        throw new Error("Invalid REPL form");
      }
      const task = evaluation.then(async () => {
        try {
          if (!(await stat(join(policy.workspace, ".nrepl-port"))).isFile()) {
            throw new Error("Experiment REPL is not ready");
          }
        } catch (error) {
          throw new Error("Experiment REPL is not ready", { cause: error });
        }
        return await new Promise((resolve, reject) => {
          const child = spawn("brepl", [], {
            cwd: policy.workspace,
            env: { ...process.env, BREPL_PORT: "" },
            stdio: ["pipe", "pipe", "pipe"],
          });
          let output = "";
          const timer = setTimeout(() => child.kill(), 60000);
          for (const stream of [child.stdout, child.stderr]) {
            stream.on("data", (chunk) => {
              output += chunk.toString();
              if (output.length > 131072) child.kill();
            });
          }
          child.on("error", (error) => {
            clearTimeout(timer);
            reject(error);
          });
          child.on("close", (code, signal) => {
            clearTimeout(timer);
            if (signal || code !== 0 || output.length > 131072) {
              reject(
                new Error(
                  `brepl failed (${code ?? signal}): ${
                    output.slice(0, 131072)
                  }`,
                ),
              );
            } else {resolve({
                content: [{ type: "text", text: output }],
                details: undefined,
              });}
          });
          child.stdin.end(`${form}\n`);
        });
      });
      evaluation = task.then(() => undefined, () => undefined);
      return await task;
    },
  });
  pi.on("session_start", async () => {
    const path = pi.getFlag("tempo-policy");
    if (typeof path !== "string") throw new Error("Tempo policy is required");
    policy = await loadPolicy(path);
  });
  pi.on("before_agent_start", () => {
    if (policy === null) throw new Error("Tempo policy has not loaded");
    pi.appendEntry("tempo-policy", policy);
  });
  pi.on("user_bash", () => {
    throw new Error("Tempo inference cannot execute shell commands");
  });
}
