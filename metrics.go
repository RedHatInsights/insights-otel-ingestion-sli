package ingestionconnector

import (
	"strconv"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/store"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// All window series are GAUGES. Their buckets are cumulative by duration but
// represent a rolling population, so dashboards must never apply rate/increase.
func buildMetrics(s store.Snapshot, environment string) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "insights-otel-ingestion-sli")
	scope := rm.ScopeMetrics().AppendEmpty()
	scope.Scope().SetName("github.com/RedHatInsights/insights-otel-ingestion-sli")
	newGauge := func(name, description string) pmetric.NumberDataPointSlice {
		m := scope.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetDescription(description)
		return m.SetEmptyGauge().DataPoints()
	}
	point := func(points pmetric.NumberDataPointSlice) pmetric.NumberDataPoint {
		p := points.AppendEmpty()
		p.SetTimestamp(pcommon.Timestamp(s.AsOfNS))
		p.Attributes().PutStr("environment", environment)
		return p
	}
	completed := newGauge("ingestion_sli_window_completed", "Observed successful completions in (now-1h,now].")
	within := newGauge("ingestion_sli_window_within_target", "Observed completions taking at most 60 seconds in (now-1h,now].")
	sum := newGauge("ingestion_sli_window_duration_seconds_sum", "Sum of end-to-end durations in the rolling window; gauge.")
	buckets := newGauge("ingestion_sli_window_duration_seconds_bucket", "Cumulative duration buckets over the rolling window; gauge, never rate().")
	for _, w := range s.Windows {
		label := func(points pmetric.NumberDataPointSlice) pmetric.NumberDataPoint {
			p := point(points)
			p.Attributes().PutStr("application", w.Application)
			p.Attributes().PutStr("milestone", w.Milestone)
			return p
		}
		label(completed).SetIntValue(int64(w.Completed))
		label(within).SetIntValue(int64(w.Within))
		label(sum).SetDoubleValue(w.Sum)
		for i, n := range w.Buckets {
			le := "+Inf"
			if i < len(model.Buckets) {
				le = strconv.FormatFloat(model.Buckets[i], 'g', -1, 64)
			}
			p := label(buckets)
			p.Attributes().PutStr("le", le)
			p.SetIntValue(int64(n))
		}
	}
	quality := newGauge("ingestion_sli_correlation_quality", "Counts of retained completion records without usable correlation, or oldest pending age in seconds; missing starts may belong to flows that bypass Ingress.")
	for reason, value := range map[string]float64{"missing_start": float64(s.PendingStarts), "conflicting_start": float64(s.ConflictingStarts), "conflicting_host": float64(s.ConflictingHosts), "invalid_interval": float64(s.InvalidIntervals), "pending_age_seconds": s.PendingAge} {
		p := point(quality)
		p.Attributes().PutStr("reason", reason)
		p.SetDoubleValue(value)
	}
	ignored := point(newGauge("ingestion_sli_ignored_evidence", "Persisted cumulative counts of well-formed completion markers outside the supported application/milestone scope; gauge."))
	ignored.Attributes().PutStr("reason", "unsupported_pair")
	ignored.SetIntValue(int64(s.Diagnostics["unsupported_pair"]))
	rejected := newGauge("ingestion_sli_rejected_evidence", "Persisted cumulative counts of rejected evidence, exposed as gauges.")
	for reason, n := range s.Diagnostics {
		if reason == "unsupported_pair" {
			continue
		}
		p := point(rejected)
		p.Attributes().PutStr("reason", reason)
		p.SetIntValue(int64(n))
	}
	point(newGauge("ingestion_sli_snapshot_timestamp_seconds", "Timestamp of the last successfully generated snapshot.")).SetDoubleValue(float64(s.AsOfNS) / 1e9)
	point(newGauge("ingestion_sli_state_requests", "Retained ingestion identities in persistent state.")).SetIntValue(s.Requests)
	point(newGauge("ingestion_sli_state_bytes", "Allocated embedded database file size.")).SetIntValue(s.FileBytes)
	return metrics
}
