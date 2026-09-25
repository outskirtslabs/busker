#!/usr/bin/env python3
import json
import os
import pathlib
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parents[3]
workspace = str((root / ".worktrees/tempo-tool-smoke-04602").resolve())
forms = {
    "implement": "(do (require '[clojure.test :as t] :reload) (t/deftest tempo-tool-test (t/is (= 3 (+ 1 2)))) (t/run-tests 'user))",
    "repair": "(do (require '[clojure.string :as s] :reload) (s/upper-case \"repair\"))",
    "analyze": "(+ 1 2)",
    "research": "(+ 1 2)",
    "review": "(+ 1 2)",
}
with tempfile.TemporaryDirectory(prefix="tempo-pi-tool-") as tmp:
    for stage, form in forms.items():
        policy = pathlib.Path(tmp) / (stage + ".json")
        policy.write_text(json.dumps({"workspace": workspace, "readRoots": [], "stage": stage}))
        tools = "tempo_read,tempo_edit,tempo_write,tempo_repl_eval" if stage in ("implement", "repair") else "tempo_read"
        args = ["pi", "--mode", "rpc", "--no-session", "--no-approve", "--no-context-files",
                "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes",
                "--no-builtin-tools", "--tools", tools,
                "--extension", str(root / "swamp/pi/restricted-tools.js"),
                "--extension", str(root / "swamp/tests/fixtures/repl_tool_fixture.js"),
                "--tempo-policy", str(policy)]
        env = {**os.environ, "TEMPO_FIXTURE_POLICY": str(policy), "PI_TELEMETRY": "0",
               "PI_CODING_AGENT_DIR": str(pathlib.Path(tmp) / ("agent-" + stage))}
        process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, env=env, cwd=workspace)
        try:
            assert process.stdin is not None and process.stdout is not None and process.stderr is not None
            def command(kind, **kwargs):
                process.stdin.write(json.dumps({"type": kind, **kwargs}) + "\n")
                process.stdin.flush()
                for _ in range(300):
                    line = process.stdout.readline()
                    if not line:
                        raise RuntimeError("Pi exited: " + process.stderr.read()[:2000])
                    event = json.loads(line)
                    if event.get("type") == "response" and event.get("command") == kind:
                        if not event.get("success"):
                            raise RuntimeError(str(event))
                        return event
                raise RuntimeError("Missing RPC response")
            command("prompt", message="/fixture-repl-eval " + form)
            entries = command("get_entries")["data"]["entries"]
            observed = [entry.get("data") for entry in entries
                        if entry.get("type") == "custom" and entry.get("customType") == "fixture-repl-eval"]
            if len(observed) != 1:
                raise RuntimeError("Missing fixture entry: " + str(entries)[-1500:])
            entry = observed[0]
            allowed = stage in ("implement", "repair")
            if ("tempo_repl_eval" in entry["activeTools"]) != allowed or entry["result"]["ok"] != allowed:
                raise RuntimeError("Tool policy mismatch " + stage + ": " + str(entry))
            print(json.dumps({"stage": stage, "activeTools": entry["activeTools"],
                              "result": entry["result"]}), flush=True)
        finally:
            process.stdin.close()
            process.wait(timeout=15)
            stderr = process.stderr.read()
            if process.returncode or stderr:
                print("Pi exit:", process.returncode, "stderr:", stderr[:1000], flush=True)
