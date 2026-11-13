# Insights OpenTelemetry ingestion SLI

An OpenTelemetry Collector distribution with an `ingestion_sli` traces-to-metrics
connector. It correlates Ingress upload starts with application completion events
to measure end-to-end ingestion latency, including queue delays and retries.

Correlation state is stored in an embedded bbolt database on a persistent volume.

## Collector integration

The SLI Collector receives an unsampled OTLP copy from an existing Collector,
which continues exporting traces to Tempo. Application export endpoints stay
unchanged.

```text
Applications --OTLP/Kafka--> Existing Collector --> Tempo
                                   |
                             Unsampled OTLP copy
                                   |
                                   v
                              SLI Collector --> Prometheus --> Grafana
                                   |
                              Persistent state
```

Forward evidence from both the existing Collector's OTLP and Kafka inputs before
sampling. The SLI Collector itself receives OTLP; it does not consume the business
Kafka topics.

The included [Compose stack](compose.yaml) is a standalone development setup:
synthetic traces go directly to the SLI Collector, which also exports them to a
local Tempo. For integration with an existing Collector, remove `traces/tempo`
and the `otlp_http/tempo` exporter from the sample configuration to avoid duplicate
Tempo exports. Configure the environment and a persistent state volume.

## Run locally

Requires Go 1.26 or later, Python 3, Podman and podman-compose. Run commands from
the repository root:

```bash
make up           # Build the UBI image and start the local stack
make integration  # Check OTLP ingestion, metrics, replay and restart recovery
make fixture      # Send synthetic successful completions
```

| Component | Address |
|---|---|
| Grafana dashboard | http://localhost:13013/d/ingestion-connector |
| Prometheus | http://localhost:19096 |
| Tempo | http://localhost:13270 |
| Collector OTLP HTTP | http://localhost:14348/v1/traces |
| Collector OTLP gRPC | localhost:14347 |
| Metrics | http://localhost:19464/metrics |
| Collector health | http://localhost:19333 |

Host ports bind to localhost. Grafana allows anonymous Viewer access. Integration
tests send synthetic traces and restart the local Collector to verify recovery.

Stop the stack with `make down`. Named volumes are preserved; deleting the
Collector state volume also deletes correlation and deduplication history.

## Measurement contract

- **Start:** the start timestamp of the `POST /api/ingress/v1/upload` `SERVER`
  span from service `ingress`. Matching uses the service, span name and kind;
  no ingestion route attribute is required.
- **Identity:** configured environment, `rh.org_id` and `rh.request_id`. Each
  ingestion represents one canonical host. Host aliases are `rh.host_id`,
  `inventory.host_id` and `vulnerability.inventory_id`.
- **Completion:** an explicit `rh.ingestion.completed` span event carrying
  `rh.ingestion.application`, `rh.ingestion.milestone` and
  `rh.ingestion.outcome` (`processed` or `unchanged`). Identity can be supplied
  on the event for batch spans. The event timestamp is the completion time;
  an optional `rh.ingestion.completed_at_ns` int64 overrides it.
- **Supported milestones:** `processing` for Ingress, Puptoo, HBI, Engine, Advisor,
  Vulnerability and Patch; separate `inventory_views` milestones for Advisor,
  Vulnerability and Patch.
- **Latency:** completion time minus the original Ingress start. The earliest
  successful completion counts once per ingestion, application and milestone.
  Conflicting origins or hosts invalidate affected observations.
- **Population:** observed successful completions. Successful no-work processing
  counts without a `host-apps` message; Inventory Views requires its own completion
  event. Missing evidence remains unknown. Application clocks must be synchronized.
- **Target:** at least 99% within 60 seconds, over the completion-time window
  `(now - 1 hour, now]`. Exactly 60 seconds passes. The example alert rules require
  five consecutive minutes below target. Empty or stale windows report no data.

Applications must emit completion events after the relevant transaction commits,
required publication is acknowledged, or a successful terminal no-work decision
is made. Each milestone needs its own explicit event; supporting a milestone in
this connector does not imply its application already emits it. The connector
does not infer completion from an arbitrary span ending.
A later optional error on the enclosing span does not invalidate an explicit
successful completion event. Failed or incomplete runs are outside this latency
population.

## Configuration and state

See [configs/collector.yaml](configs/collector.yaml) for the local configuration
and [internal/config/config.go](internal/config/config.go) for defaults and
validation. The measurement window is fixed at one hour and the latency threshold
at 60 seconds. State retention defaults to 24 hours and snapshots refresh every
five seconds. The default start selector is:

```yaml
start:
  service: ^ingress$
  name: ^POST /api/ingress/v1/upload$
  kind: SERVER
```

An optional `start.attributes` map adds filters. These attributes must actually
be emitted by Ingress; an unmet filter leaves completions without a matching
start. Ingestions without the selected Ingress start remain outside the measured
population.

Accepted evidence and deduplication state are committed before OTLP success is
returned. Storage or capacity errors propagate to the sender for retry. Delayed
and out-of-order evidence can be correlated within the retention period.

Run one active instance per environment and state file. Independent replicas do
not share correlation state. State files are tied to their environment and start
selector; use a new state file when changing either. Size the persistent volume
and `max_requests` for the expected ingestion rate and retention period.

## Metrics

The connector exports these rolling gauges:

- `ingestion_sli_window_completed`
- `ingestion_sli_window_within_target`
- `ingestion_sli_window_duration_seconds_sum`
- `ingestion_sli_window_duration_seconds_bucket`

Use them directly; do not apply `rate()` or `increase()`. Duration buckets include
an exact 60-second boundary. Attainment uses exact received completion counts;
histogram quantiles are interpolated estimates. Organization, request and host
IDs are not exported as metric labels.

The [local recording rules](deploy/local/rules.yaml) provide the attainment ratio:

```promql
ingestion_sli:success_ratio_1h
```

For P99 latency:

```promql
histogram_quantile(0.99,
  max by (environment, application, milestone, le)
    (ingestion_sli_window_duration_seconds_bucket))
```

Quality metrics expose unmatched completions, conflicting identities, invalid
intervals, rejected evidence, state size and snapshot freshness.

`ingestion_sli_ignored_evidence{reason="unsupported_pair"}` separately counts
well-formed completion markers for unsupported application/milestone pairs, such
as HBI's `compliance / inventory_views` events. They do not enter correlation
state or SLA counts. Missing fields, invalid timestamps, conflicting identities
and other malformed evidence still appear in `ingestion_sli_rejected_evidence`.
Both metrics are persisted cumulative delivery counts exposed as gauges; retries
can increase them. They are not counts of unique hosts or failed ingestions.
Existing rejected counts survive upgrades and are not retroactively reclassified.

`ingestion_sli_correlation_quality{reason="missing_start"}` counts retained
completion records without a matching selected Ingress start. It includes delayed
or missing start telemetry and workflows that bypass Ingress, such as direct RHSM
host updates. One ingestion can contribute multiple application/milestone records.
These records remain unknown and outside latency counts; a late matching start
can still correlate them within state retention. The marker contract does not
reliably identify the source reporter, so the connector cannot distinguish these
causes automatically. This diagnostic alone does not establish an ingestion
failure or SLA breach. Measuring RHSM paths requires an agreed start boundary.

## Development and testing

GitHub Actions runs pre-commit checks and race-enabled Go unit tests on pull
requests and pushes to `main`, using Go 1.26 to match the image build toolchain.

Konflux builds the UBI image through the PR and push pipelines in `.tekton/`.
PR builds target `main`, produce temporary images and disable automatic release.
Push builds run after changes merge into `main`. These checks require the
repository's Component and Pipelines as Code integration to be provisioned
in Konflux.

Install [pre-commit](https://pre-commit.com/#install), then enable the local hooks
and check the repository:

```bash
pre-commit install
pre-commit run --all-files
```

The hooks check whitespace, YAML, JSON, merge conflicts, oversized files, private
keys, Python debug statements and Go formatting. Go vet and unit tests run through
the Makefile:

```bash
make test check   # Go race tests, vet and formatting checks
make rules        # Prometheus recording and alert rule tests
make benchmark    # Embedded storage benchmark
```

With the local stack running, generate slow or successful no-work processing:

```bash
go run ./cmd/fixture -scenario slow
go run ./cmd/fixture -scenario unchanged
```

Run a synthetic load test and save its report in the ignored `reports/` directory:

```bash
mkdir -p reports
python3 scripts/load.py --seconds 30 --rate 100 --output reports/load.json
```

## Replay a trace

The replay tool accepts OTLP JSON or a Tempo trace response. It filters attributes
and shifts timestamps while preserving intervals. By default, it sends spans in
reverse order twice to exercise out-of-order delivery and deduplication.

```bash
mkdir -p replays
go run ./cmd/replay -input /path/to/trace.json -output replays/trace.json
go run ./cmd/replay -input replays/trace.json -shift-now=false
```

Reuse the saved timestamps when testing deduplication. Shifting the same request
to a different start creates an origin conflict. Explicit completion events are
required for milestone measurements.

Filtering does not anonymize identifiers or span names. Keep real trace captures,
replay outputs and state backups outside the repository or in the ignored
`captures/` and `replays/` directories. Use synthetic data for public fixtures.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
