package otlp

import (
	"bytes"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"testing"
	"time"
)

func TestReplaySanitizesAndPreservesIntervals(t *testing.T) {
	now := time.Now()
	traces, _ := Fixture("happy", now)
	root := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	root.Attributes().PutStr("http.url", "https://storage.invalid/archive?secret=must-not-replay")
	data, err := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	clean, err := Normalize(data)
	if err != nil {
		t.Fatal(err)
	}
	original, err := testParser(t).Parse(clean, now)
	if err != nil {
		t.Fatal(err)
	}
	Shift(clean, now.Add(-time.Second))
	shifted, err := testParser(t).Parse(clean, now)
	if err != nil {
		t.Fatal(err)
	}
	if original.Completions[0].CompletedAtNS-original.Starts[0].StartedAtNS != shifted.Completions[0].CompletedAtNS-shifted.Starts[0].StartedAtNS {
		t.Fatal("replay changed latency")
	}
	encoded, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(clean)
	if bytes.Contains(encoded, []byte("must-not-replay")) || len(Split(clean)) != 11 {
		t.Fatal("unsafe or incomplete replay")
	}
}

func testParser(t *testing.T) Parser {
	t.Helper()
	c := config.Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	return Parser{Config: *c}
}
func TestMarkersAllApplicationsAndNoWork(t *testing.T) {
	p := testParser(t)
	now := time.Now()
	for scenario, count := range map[string]int{"happy": 10, "slow": 10, "unchanged": 7} {
		traces, err := Fixture(scenario, now)
		if err != nil {
			t.Fatal(err)
		}
		// Live Ingress spans identify the upload without an ingestion route attribute.
		traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().Remove("rh.ingestion.route")
		for _, encoding := range []string{"otlp_json", "otlp_proto"} {
			var data []byte
			if encoding == "otlp_json" {
				data, err = (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
			} else {
				data, err = (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
			}
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Decode(data, encoding)
			if err != nil {
				t.Fatal(err)
			}
			b, err := p.Parse(decoded, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(b.Starts) != 1 || len(b.Completions) != count {
				t.Fatalf("%s/%s: starts=%d completions=%d diagnostics=%v", scenario, encoding, len(b.Starts), len(b.Completions), b.Diagnostics)
			}
		}
	}
}
func TestExcludedEvidenceIsClassified(t *testing.T) {
	p := testParser(t)
	now := time.Now()
	for _, test := range []struct {
		name   string
		change func(ptrace.Span)
		reason string
	}{
		{"different request ID", func(s ptrace.Span) { s.Events().At(0).Attributes().PutStr("rh.request_id", "wrong") }, "conflicting_identity"},
		{"different event host", func(s ptrace.Span) { s.Events().At(0).Attributes().PutStr("rh.host_id", "wrong") }, "conflicting_identity"},
		{"different host aliases", func(s ptrace.Span) { s.Attributes().PutStr("inventory.host_id", "wrong") }, "invalid_marker"},
		{"missing identity", func(s ptrace.Span) { s.Attributes().Remove("rh.org_id") }, "missing_identity"},
		{"unsupported application", func(s ptrace.Span) {
			s.Events().At(0).Attributes().PutStr("rh.ingestion.application", "compliance")
			s.Events().At(0).Attributes().PutStr("rh.ingestion.milestone", "inventory_views")
		}, "unsupported_pair"},
		{"unsupported milestone", func(s ptrace.Span) {
			s.Events().At(0).Attributes().PutStr("rh.ingestion.milestone", "future_milestone")
		}, "unsupported_pair"},
		{"missing application", func(s ptrace.Span) { s.Events().At(0).Attributes().Remove("rh.ingestion.application") }, "invalid_marker"},
		{"missing milestone", func(s ptrace.Span) { s.Events().At(0).Attributes().Remove("rh.ingestion.milestone") }, "invalid_marker"},
		{"malformed application", func(s ptrace.Span) { s.Events().At(0).Attributes().PutStr("rh.ingestion.application", "compliance\n") }, "invalid_marker"},
		{"unsupported application with invalid outcome", func(s ptrace.Span) {
			s.Events().At(0).Attributes().PutStr("rh.ingestion.application", "compliance")
			s.Events().At(0).Attributes().PutStr("rh.ingestion.outcome", "failed")
		}, "invalid_marker"},
		{"failure marker", func(s ptrace.Span) { s.Events().At(0).Attributes().PutStr("rh.ingestion.outcome", "failed") }, "invalid_marker"},
		{"lossy ns", func(s ptrace.Span) {
			s.Events().At(0).Attributes().PutDouble("rh.ingestion.completed_at_ns", float64(now.UnixNano()))
		}, "invalid_marker"},
		{"future event", func(s ptrace.Span) { s.Events().At(0).SetTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Hour))) }, "invalid_marker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			traces, _ := Fixture("happy", now)
			test.change(traces.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0))
			b, err := p.Parse(traces, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(b.Completions) != 9 || len(b.Diagnostics) != 1 || b.Diagnostics[test.reason] != 1 {
				t.Fatalf("unexpected evidence classification: %+v", b.Diagnostics)
			}
		})
	}
}
func TestMarkerPrecisionAndPostProcessingError(t *testing.T) {
	p := testParser(t)
	now := time.Now()
	traces, _ := Fixture("happy", now)
	s := traces.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0)
	s.Status().SetCode(ptrace.StatusCodeError)
	ns := int64(s.Events().At(0).Timestamp()) + 123
	s.Events().At(0).Attributes().PutInt("rh.ingestion.completed_at_ns", ns)
	data, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	decoded, err := Decode(data, "otlp_json")
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Parse(decoded, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Completions) != 10 || b.Completions[0].CompletedAtNS != ns {
		t.Fatal("lost authoritative marker or nanosecond precision")
	}
}
