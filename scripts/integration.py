#!/usr/bin/env python3
"""Exercise the running UBI Collector over OTLP HTTP; no third-party Python packages."""

import argparse
import copy
import json
import re
import shlex
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

PAIRS = [(app, "processing") for app in ("ingress", "puptoo", "hbi", "engine", "advisor", "vulnerability", "patch")]
PAIRS += [(app, "inventory_views") for app in ("advisor", "vulnerability", "patch")]
SECOND = 1_000_000_000


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read()


def attributes(values):
    return [{"key": k, "value": {"stringValue": v}} for k, v in values.items()]


class Suite:
    def __init__(self, args):
        self.args = args
        self.prefix = "integration-" + uuid.uuid4().hex
        self.cases = []
        self.expected = {}
        self.clock = time.time_ns() - 180 * SECOND
        self.wait_ready()
        _, self.expected, _ = self.read()
        self.initial = copy.deepcopy(self.expected)

    def wait_ready(self):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            try:
                self.read()
                return
            except (OSError, ValueError):
                time.sleep(0.25)
        raise AssertionError("Collector metrics did not become ready")

    def read(self):
        values, snapshot, quality = {}, 0, {}
        for line in get(self.args.metrics).decode().splitlines():
            if line.startswith("#"):
                continue
            match = re.match(r'([^ {]+)(?:\{(.*?)\})?\s+(\S+)', line)
            if not match:
                continue
            name, labels, value = match.groups()
            labels = dict(re.findall(r'(\w+)="([^"]*)"', labels or ""))
            value = float(value)
            if name == "ingestion_sli_snapshot_timestamp_seconds":
                snapshot = value
            elif name in ("ingestion_sli_window_completed", "ingestion_sli_window_within_target"):
                pair = (labels["application"], labels["milestone"])
                values.setdefault(pair, [0, 0])[int(name.endswith("within_target"))] = int(value)
            elif name == "ingestion_sli_correlation_quality":
                quality[labels["reason"]] = value
        if len(values) != 10 or snapshot <= 0:
            raise ValueError("Incomplete SLA metric snapshot")
        return snapshot, values, quality

    def send(self, spans):
        data = json.dumps({"resourceSpans": spans}).encode()
        req = urllib.request.Request(self.args.endpoint, data=data, headers={"Content-Type": "application/json"})
        started = time.monotonic()
        with urllib.request.urlopen(req, timeout=20) as response:
            body = json.loads(response.read() or b"{}")
            assert response.status == 200 and not body.get("partialSuccess"), body
        return time.monotonic() - started

    def span(self, name, service, start, end, pair=None, outcome="processed"):
        request = self.prefix + "-" + name
        tid = uuid.uuid5(uuid.NAMESPACE_DNS, request).hex
        span = {
            "traceId": tid, "spanId": uuid.uuid4().hex[:16], "name": "completion",
            "kind": 1, "startTimeUnixNano": str(start), "endTimeUnixNano": str(end),
            "attributes": attributes({"rh.org_id": "integration", "rh.request_id": request, "rh.host_id": "host-" + name}),
        }
        if pair is None:
            span.update(name="POST /api/ingress/v1/upload", kind=2)
            span["attributes"] += attributes({"rh.ingestion.route": "advisor"})
        else:
            span["events"] = [{"name": "rh.ingestion.completed", "timeUnixNano": str(end), "attributes": attributes({
                "rh.ingestion.application": pair[0], "rh.ingestion.milestone": pair[1], "rh.ingestion.outcome": outcome,
            })}]
        return {"resource": {"attributes": attributes({"service.name": service})}, "scopeSpans": [{"spans": [span]}]}

    def root(self, name, start=None):
        start = self.clock if start is None else start
        return self.span(name, "ingress", start, start + SECOND)

    def completion(self, name, pair, duration=5 * SECOND, start=None, outcome="processed"):
        start = self.clock if start is None else start
        return self.span(name, pair[0], start + duration - 1_000_000, start + duration, pair, outcome)

    def add(self, pair, duration):
        self.expected[pair][0] += 1
        self.expected[pair][1] += int(duration <= 60 * SECOND)

    def verify(self, name, previous):
        deadline = time.monotonic() + 25
        last = None
        while time.monotonic() < deadline:
            try:
                snapshot, last, _ = self.read()
                if snapshot > previous and last == self.expected:
                    self.cases.append(name)
                    print("PASS", name, flush=True)
                    return
            except (OSError, ValueError):
                pass
            time.sleep(0.25)
        raise AssertionError(f"{name}: expected {self.expected}, received {last}")

    def run(self):
        previous, _, _ = self.read()
        root = self.root("all")
        completions = [self.completion("all", p, (i + 5) * SECOND) for i, p in enumerate(PAIRS)]
        for span in reversed(completions):
            self.send([span])
        self.send([root])
        for i, p in enumerate(PAIRS):
            self.add(p, (i + 5) * SECOND)
        self.verify("all ten milestones; completions before Ingress", previous)

        previous, _, _ = self.read()
        self.send([root] + completions)
        self.verify("duplicate spans and completion events count once", previous)

        previous, _, _ = self.read()
        for i, duration in enumerate((60 * SECOND, 60 * SECOND + 1)):
            self.send([self.root("boundary" + str(i)), self.completion("boundary" + str(i), ("hbi", "processing"), duration)])
            self.add(("hbi", "processing"), duration)
        self.verify("inclusive 60-second target with nanosecond precision", previous)

        previous, _, _ = self.read()
        self.send([self.root("unchanged")] + [self.completion("unchanged", p, outcome="unchanged") for p in PAIRS if p[1] == "processing"])
        for p in PAIRS:
            if p[1] == "processing":
                self.add(p, 5 * SECOND)
        self.verify("successful no-work paths without host-apps messages", previous)

        previous, _, _ = self.read()
        self.send([self.root("earlier"), self.completion("earlier", ("hbi", "processing"), 90 * SECOND), self.completion("earlier", ("hbi", "processing"), 30 * SECOND)])
        self.add(("hbi", "processing"), 30 * SECOND)
        self.verify("earliest successful completion wins", previous)

        previous, _, _ = self.read()
        start = self.clock - 3 * 3600 * SECOND
        duration = 3 * 3600 * SECOND
        self.send([self.root("backlog", start), self.completion("backlog", ("vulnerability", "processing"), duration, start)])
        self.add(("vulnerability", "processing"), duration)
        self.verify("three-hour Kafka delay included without keeping a trace open", previous)

        previous, _, _ = self.read()
        old = self.clock - 2 * 3600 * SECOND
        self.send([self.root("old", old), self.completion("old", ("hbi", "processing"), start=old)])
        failed = self.completion("failed", ("hbi", "processing"), outcome="failed")
        self.send([self.root("failed"), failed])
        self.verify("old completions and failed outcomes do not enter the window", previous)

        previous, _, _ = self.read()
        conflict = self.completion("conflict", ("hbi", "processing"))
        self.send([self.root("conflict"), conflict])
        self.add(("hbi", "processing"), 5 * SECOND)
        self.verify("valid observation before late conflict", previous)
        previous, _, quality = self.read()
        bad = copy.deepcopy(conflict)
        bad["scopeSpans"][0]["spans"][0]["attributes"][-1]["value"]["stringValue"] = "different-host"
        self.send([bad])
        self.expected[("hbi", "processing")][0] -= 1
        self.expected[("hbi", "processing")][1] -= 1
        self.verify("late host conflict retracts an already counted measurement", previous)
        assert self.read()[2]["conflicting_host"] > quality["conflicting_host"]

        pending_root = self.root("restart")
        pending_completion = self.completion("restart", ("advisor", "processing"))
        previous, _, _ = self.read()
        self.send([pending_completion])
        self.verify("pending completion remains unknown before its start", previous)
        if not self.args.skip_restart:
            subprocess.run(shlex.split(self.args.compose) + ["restart", "collector"], cwd=Path(__file__).resolve().parents[1], check=True)
            self.wait_ready()
            previous = 0
            self.verify("completed measurements survive Collector restart", previous)
            previous, _, _ = self.read()
            self.send([pending_root])
            self.add(("advisor", "processing"), 5 * SECOND)
            self.verify("pending completion survives restart and joins its late start", previous)
            previous, _, _ = self.read()
            self.send([pending_root, pending_completion, root] + completions)
            self.verify("deduplication survives restart", previous)

        query = 'ingestion_sli:success_ratio_1h{environment="local"}'
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            result = json.loads(get(self.args.prometheus + "/api/v1/query?" + urllib.parse.urlencode({"query": query})))
            rows = result.get("data", {}).get("result", [])
            if len(rows) == 10 and all(abs(float(row["value"][1]) - self.expected[(row["metric"]["application"], row["metric"]["milestone"])][1] / self.expected[(row["metric"]["application"], row["metric"]["milestone"])][0]) < 1e-10 for row in rows):
                break
            time.sleep(0.5)
        else:
            raise AssertionError("Prometheus recording rules do not match the measured counts")
        self.cases.append("Prometheus one-hour SLA ratios match exact counts")
        report = {"passed": self.cases, "restart_tested": not self.args.skip_restart, "milestones": len(PAIRS), "counts": {"/".join(p): v for p, v in self.expected.items()}}
        print(json.dumps(report, indent=2), flush=True)
        if self.args.output:
            Path(self.args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:14348/v1/traces")
    parser.add_argument("--metrics", default="http://127.0.0.1:19464/metrics")
    parser.add_argument("--prometheus", default="http://127.0.0.1:19096")
    parser.add_argument("--compose", default="podman-compose")
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--output")
    Suite(parser.parse_args()).run()
