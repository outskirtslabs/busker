import registerTempoTools from "../../pi/restricted-tools.js";

export default function (pi) {
  const tools = new Map();
  const handlers = new Map();
  const policyPath = process.env.TEMPO_FIXTURE_POLICY;
  registerTempoTools({
    registerFlag() {},
    registerTool(tool) {
      tools.set(tool.name, tool);
    },
    on(event, handler) {
      handlers.set(event, handler);
    },
    getFlag() {
      return policyPath;
    },
    appendEntry() {},
  });
  pi.registerCommand("fixture-repl-eval", {
    description:
      "Exercise the registered Tempo worktree REPL tool without inference",
    async handler(form) {
      await handlers.get("session_start")();
      let result;
      try {
        const response = await tools.get("tempo_repl_eval").execute(
          "fixture",
          { form },
          undefined,
          () => {},
        );
        result = { ok: true, text: response.content[0].text };
      } catch (error) {
        result = { ok: false, error: String(error) };
      }
      pi.appendEntry("fixture-repl-eval", {
        activeTools: pi.getActiveTools(),
        form,
        result,
      });
    },
  });
}
