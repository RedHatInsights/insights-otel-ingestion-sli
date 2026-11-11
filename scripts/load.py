#!/usr/bin/env python3
"""Send controlled synthetic OTLP traffic through the local Collector."""

import argparse
import json
import statistics
import time
from pathlib import Path

from integration import PAIRS, SECOND, Suite

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--seconds", type=int, default=30)
parser.add_argument("--rate", type=int, default=100, help="ingestions per second")
parser.add_argument("--output")
args = parser.parse_args()
assert 1 <= args.seconds <= 300 and 1 <= args.rate <= 1000
args.endpoint = "http://127.0.0.1:14348/v1/traces"
args.metrics = "http://127.0.0.1:19464/metrics"
suite = Suite(args)
previous, _, _ = suite.read()
durations = []
begin = time.monotonic()
for i in range(args.seconds):
    payload = []
    for j in range(args.rate):
        name = f"load-{i}-{j}"
        payload.append(suite.root(name))
        for pair in PAIRS:
            payload.append(suite.completion(name, pair, 10 * SECOND))
            suite.add(pair, 10 * SECOND)
    durations.append(suite.send(payload))
    time.sleep(max(0, begin + i + 1 - time.monotonic()))
elapsed = time.monotonic() - begin
suite.verify("controlled load: exact counts for all ten milestones", previous)
report = {
    "ingestions": args.seconds * args.rate,
    "completion_events": args.seconds * args.rate * len(PAIRS),
    "elapsed_seconds": elapsed,
    "achieved_ingestions_per_second": args.seconds * args.rate / elapsed,
    "otlp_batch_ack_p50_seconds": statistics.median(durations),
    "otlp_batch_ack_max_seconds": max(durations),
    "all_counts_verified": True,
}
print(json.dumps(report, indent=2))
if args.output:
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
