#!/usr/bin/env python3
# Fixed local fixtures; this program never invokes Pi or a provider.
import hashlib
import json
import os
import re
import subprocess
import signal
import sys
from pathlib import Path

case = os.environ["TEMPO_FIXTURE_CASE"]
tool = Path(sys.argv[0]).name
root = Path.cwd()
if tool == "tmux":
    workspace = Path(sys.argv[sys.argv.index("-c") + 1])
    if case == "hold-edit":
        unit = re.search(r"--unit=(tempo-[a-zA-Z0-9_-]+-repl\.scope)", sys.argv[-1])[1]
        description = re.search(r"--description='([^']+)'", sys.argv[-1])[1]
        subprocess.Popen(["systemd-run", "--user", "--scope", "--quiet", "--collect",
                          f"--unit={unit}", f"--description={description}",
                          "--", "sleep", "120"], stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
    (workspace / ".nrepl-port").write_text("fixture\n")
    sys.exit(0)
if tool == "brepl":
    print("3")
    sys.exit(0)

source = root / "src/main/clojure/ol/busker.clj"
marker = source.read_text()
arch = "aarch64" if os.uname().machine == "aarch64" else "x86-64"
native = root / f"shim/linux-{arch}/resources/linux-{arch}/libh2oclj.so"

def capture(pattern, text):
    match = re.search(pattern, text)
    if match is None:
        raise ValueError("Missing fixture input: " + pattern)
    return match[1]

def save(path, value):
    Path(path).write_text(json.dumps(value))


def affinity(directory, samples, warmup, duration):
    assert re.search(r"(?m)^Cpus_allowed_list:\s*(\S+)", Path("/proc/self/status").read_text())[1] == "10-11"
    rows = []
    for sample in samples:
        sample.mkdir(parents=True, exist_ok=True)
        for phase in ["ready", "measurement-start", "measurement-end"]:
            save(sample / f"server-affinity-{phase}.json", {"method": "pinned-server-10-11-client-12-13",
                "phase": phase, "threads": [{"tid": os.getpid(), "allowed": "10-11"}]})
        rows += [f"2026-09-25T00:00:00Z\t{os.getpid()}\t{os.environ['TEMPO_H2LOAD_REAL']}\t{os.getpid()}:12-13\t-D {length} fixture"
                 for length in [warmup, duration]]
    (directory / "client-affinity.tsv").write_text("\n".join(rows) + "\n")
def environment():
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    return {"busker-mode": "local", "busker-checkout": str(root), "busker-revision": revision,
            "busker-dirty?": False, "native-version": None, "native-sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
            "native-resource": native.as_uri(), "busker-resource": source.as_uri(), "java-version": "fixture-25",
            "h2load-version": "fixture-h2load", "processors": 2, "max-heap-bytes": 2147483648,
            "jvm-options": ["-Xms512m", "-Xmx2g", "-XX:ActiveProcessorCount=2", "-Dbusker.bench.local=true", f"-Dol.libh2oclj.path={native}"],
            "native-mappings": [f"fixture {native}"], "garbage-collectors": ["fixture-gc"]}

if tool == "bb":
    if case == "hold" and sys.argv[1] == "qa":
        print("Fixture paused after reserving the initial test step", flush=True)
        print("Fixture stderr survives controller termination", file=sys.stderr, flush=True)
        Path(os.environ["TEMPO_FIXTURE_HOLD_READY"]).write_text(str(os.getpid()))
        signal.pause()
    if sys.argv[1] in ["qa", "build"]:
        native.parent.mkdir(parents=True, exist_ok=True)
        native.write_text("fixture native bytes")
        print("Fixed build/test output, not a Busker workload")
        sys.exit(7 if sys.argv[1] == "qa" and "test-failure" in marker else 0)
    assert sys.argv[1] == "bench:local"
    opts = dict(zip(sys.argv[2::2], sys.argv[3::2], strict=True))
    if case == "hold":
        Path(os.environ["TEMPO_FIXTURE_NEXT_PHASE"]).write_text("advanced")
    output = Path(opts["--output"])
    output.mkdir(parents=True)
    protocol, adapter = opts["--protocol"], opts["--adapter"]
    if case == "candidate-failure" and "initial" not in marker and opts["--protocol"] == "tls-h2":
        print("Fixture candidate benchmark failed", file=sys.stderr)
        sys.exit(7)
    rate = 200 if protocol == "tls-h2" else 100
    if adapter != "busker":
        rate = 150
    elif "initial" not in marker:
        rate = (180 if case == "reject" else 200) if protocol == "tls-h2" else (120 if "review-failure" in marker else 160)
        if case == "requal-higher" and protocol == "h1":
            rate = 167
        if case == "two-keeps" and protocol == "h1":
            rate = 160 if "good-1" in marker else 180
    params = {key[2:]: int(value) for key, value in opts.items() if key not in ["--output", "--protocol", "--adapter"]}
    affinity(output, [output / adapter / protocol / str(index + 1)
                      for index in range(params["repetitions"])], params["warmup"], params["duration"])
    env = environment()
    rates = [rate] * params["repetitions"]
    if case in ("noise", "noise-h2") and protocol == ("tls-h2" if case == "noise-h2" else "h1") and adapter == "busker":
        rates = [90, 100, 110]
    elif case == "candidate-noise" and "initial" not in marker and protocol == "tls-h2" and adapter == "busker":
        rates = [90, 100, 110]
    elif case == "noise-stabilizes" and protocol == "h1" and adapter == "busker":
        rates = [102, 102, 102] if output.name == "benchmark-repeat" else [100, 100, 105.01]
    sorted_rates = sorted(rates)
    median = sorted_rates[len(rates) // 2] if len(rates) % 2 else (sorted_rates[len(rates) // 2 - 1] + sorted_rates[len(rates) // 2]) / 2
    cell = {"status": "ok", "requests-per-second": median, "range": [sorted_rates[0], sorted_rates[-1]], "samples": [
        {"status": "ok", "requests-per-second": sample_rate, "requests": 1000, "environment": env,
         "response": {"status": 200, "body": "Hello World", "protocol": "HTTP_2" if protocol == "tls-h2" else "HTTP_1_1"}}
        for sample_rate in rates]}
    save(output / "results.json", {"environment": env, "parameters": {**params, "protocol": protocol, "adapter": adapter},
                                   "results": [{"adapter": adapter, "protocols": {protocol: cell}}]})
elif tool == "clojure":
    text = Path(sys.argv[-1]).read_text()
    directory = Path(json.loads(capture(r':directory ("[^"]+")', text)))
    event = capture(r":event :(\S+)", text)
    protocol = capture(r":protocol :(\S+)", text)
    params = {key: int(capture(r":" + key + r" (\d+)", text)) for key in ["warmup", "duration", "connections", "streams", "threads"]}
    fields = ["S"] + ["0"] * 49
    affinity(directory, [directory], params["warmup"], params["duration"])
    fields[11], fields[12], fields[19] = "10", "5", "100"
    stat = "123 (fixture) " + " ".join(fields)
    snapshot = {"captured-at-ms": 1000, "allocated-bytes": 100, "process": {"stat": stat, "status": None, "schedstat": None},
                "threads": {}, "host-pressure": None, "cgroup-path": "/fixture", "cgroup-stat": None, "cgroup-pressure": None}
    save(directory / "before.json", snapshot)
    save(directory / "after.json", {**snapshot, "captured-at-ms": 2000, "allocated-bytes": 200})
    save(directory / "client-cpu.json", {"userSeconds": 0.1, "systemSeconds": 0.1, "voluntarySwitches": 1, "involuntarySwitches": 0})
    (directory / "profile.collapsed").write_text("[h2o-evloop-0 tid=123];h2o_send 10\n")
    save(directory / "jfr-events.json", {"recording": {"events": [{"type": "jdk.ThreadAllocationStatistics", "values": {}}]}})
    save(directory / "profile.json", {"kind": "profile", "score": None, "event": event, "protocol": protocol, "parameters": params,
                                      "environment": environment(), "profiler-version": "1.6.2", "clock-ticks-per-second": 100,
                                      "load": {"requests": 1000, "requests-per-second": 1000}})
elif tool == "pi":
    policy_path = Path(sys.argv[sys.argv.index("--tempo-policy") + 1])
    policy = json.loads(policy_path.read_text())
    manifest = json.loads((policy_path.parent / "inputs/manifest.json").read_text())
    context = manifest["context"]
    evidence = context["inputs"]
    stage = context["stage"]
    data = [item["data"] for item in manifest["inputs"]]
    for entry in manifest["inputs"]:
        if entry["reference"]["name"] == context["runId"] + "-experiment-brief":
            saved = entry["data"]
            assert saved["source"] == "prepared experiment brief"
            assert saved["runId"] == context["runId"]
            assert hashlib.sha256(saved["text"].encode()).hexdigest() == saved["sha256"]
    history = next(item["history"] for item in data if "history" in item)
    if case == "hold-edit" and stage == "implement":
        source.write_text("; interrupted fixture edit\n")
        print(json.dumps({"type": "session"}), flush=True)
        print("Fixed fixture Pi stderr", file=sys.stderr, flush=True)
        Path(os.environ["TEMPO_FIXTURE_HOLD_READY"]).write_text(str(os.getpid()))
        signal.pause()
    if stage == "analyze":
        if case in ["analysis-stop", "requal-higher"]:
            response = {"action": "stop", "reason": "Fixed fixture stop", "evidence": evidence}
        elif case == "repair" and not any(item["phase"] == "research" for item in history):
            response = {"action": "research", "question": "Fixture research", "evidence": evidence}
        elif case == "repair" and sum(item["phase"] == "profile" for item in history) == 1:
            response = {"action": "profile", "event": "wall", "question": "Fixture extra recording", "evidence": evidence}
        else:
            response = {"action": "change", "hypothesis": "Fixture hypothesis", "plan": ["Fixed edit"], "evidence": evidence}
    elif stage == "research":
        response = {"findings": [], "recommendation": "Fixture recommendation", "evidence": evidence}
    elif stage in ["implement", "repair"]:
        if stage == "repair" and "test-failure" in marker:
            failed_test = next(item for item in data if item.get("passed") is False)
            assert "Fixed build/test output" in failed_test["failureOutput"]["stdout"]["text"]
        next_marker = "good"
        if case == "repair":
            next_marker = "test-failure" if stage == "implement" else "review-failure" if "test-failure" in marker else "good"
        if case == "two-keeps":
            next_marker = "good-1" if not any(item["phase"] == "implement" for item in history) else "good-2"
        source.write_text("; " + next_marker + "\n")
        response = {"summary": "Fixed fixture edit", "evidence": evidence}
    else:
        assert stage == "review"
        failed = "review-failure" in marker
        response = {"verdict": "fail" if failed else "pass", "findings": [{"severity": "blocking", "file": "src/main/clojure/ol/busker.clj", "description": "Fixture review repair"}] if failed else [], "evidence": evidence}
    text = "{" if case == "malformed" and stage == "implement" else json.dumps(response)
    for entry in [{"type": "session"}, {"type": "entry_appended", "entry": {"customType": "tempo-policy", "data": policy}},
                  {"type": "message_end", "message": {"role": "assistant", "provider": "openai-codex", "model": "gpt-6-astra", "stopReason": "stop", "content": [{"type": "text", "text": text}], "usage": {"totalTokens": 1}}},
                  {"type": "agent_settled"}]:
        print(json.dumps(entry))
else:
    raise RuntimeError("Unexpected fixture command: " + tool)
