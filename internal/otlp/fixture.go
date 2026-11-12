package otlp

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Fixture emits explicitly synthetic contract evidence, never production claims.
// unchanged produces seven terminal processing observations and no Views events.
func Fixture(scenario string, now time.Time) (ptrace.Traces, error) {
	if scenario != "happy" && scenario != "slow" && scenario != "unchanged" {
		return ptrace.NewTraces(), fmt.Errorf("scenario must be happy, slow, or unchanged")
	}
	traces := ptrace.NewTraces()
	var tid [16]byte
	_, err := rand.Read(tid[:])
	if err != nil {
		return traces, err
	}
	request := fmt.Sprintf("fixture-%x", tid)
	base := now.Add(-3 * time.Minute)
	makeSpan := func(service, name string, start, end time.Time) ptrace.Span {
		resource := traces.ResourceSpans().AppendEmpty()
		resource.Resource().Attributes().PutStr("service.name", service)
		span := resource.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName(name)
		span.SetTraceID(pcommon.TraceID(tid))
		var id [8]byte
		_, _ = rand.Read(id[:])
		span.SetSpanID(pcommon.SpanID(id))
		span.SetKind(ptrace.SpanKindInternal)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(end))
		span.Attributes().PutStr("rh.request_id", request)
		span.Attributes().PutStr("rh.org_id", "synthetic-fixture")
		return span
	}
	root := makeSpan("ingress", "POST /api/ingress/v1/upload", base, base.Add(500*time.Millisecond))
	root.SetKind(ptrace.SpanKindServer)
	root.Attributes().PutStr("rh.ingestion.route", "advisor")
	for i, pair := range model.Pairs() {
		if scenario == "unchanged" && pair.Milestone == "inventory_views" {
			continue
		}
		duration := time.Duration(i+1) * time.Second
		if scenario == "slow" {
			duration += 65 * time.Second
		}
		service := pair.Application
		span := makeSpan(service, "synthetic terminal boundary", base.Add(duration-time.Millisecond), base.Add(duration))
		span.SetParentSpanID(root.SpanID())
		span.Attributes().PutStr("rh.host_id", "synthetic-host")
		e := span.Events().AppendEmpty()
		e.SetName(CompletionEvent)
		e.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(duration)))
		e.Attributes().PutStr("rh.ingestion.application", pair.Application)
		e.Attributes().PutStr("rh.ingestion.milestone", pair.Milestone)
		outcome := "processed"
		if scenario == "unchanged" {
			outcome = "unchanged"
		}
		e.Attributes().PutStr("rh.ingestion.outcome", outcome)
	}
	return traces, nil
}
