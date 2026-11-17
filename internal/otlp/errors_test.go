package otlp

import (
	"testing"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestOnlyReviewedProcessingErrorsAreAccepted(t *testing.T) {
	now := time.Now()
	traces, err := Fixture("processing-errors", now)
	if err != nil {
		t.Fatal(err)
	}
	p := testParser(t)
	b, err := p.Parse(traces, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Errors) != 5 || len(b.Completions) != 0 || len(b.Diagnostics) != 0 {
		t.Fatalf("child errors were counted or reviewed errors lost: %+v", b)
	}
	seen := map[string]bool{}
	for _, e := range b.Errors {
		if e.Milestone != "processing" || e.OrgID != "synthetic-fixture" || e.RequestID == "" || e.SpanID == "" {
			t.Fatalf("lost identity/boundary: %+v", e)
		}
		seen[e.Application] = true
	}
	for _, name := range []string{"puptoo", "hbi", "advisor", "patch", "vulnerability"} {
		if !seen[name] {
			t.Fatal("missing boundary", name)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(ptrace.Span)
		reason string
	}{
		{"missing identity", func(s ptrace.Span) { s.Attributes().Remove("rh.org_id") }, "error_missing_identity"},
		{"conflicting boundary", func(s ptrace.Span) { s.Attributes().PutStr("rh.ingestion.application", "patch") }, "conflicting_error_boundary"},
		{"conflicting host aliases", func(s ptrace.Span) {
			s.Attributes().PutStr("rh.host_id", "one")
			s.Attributes().PutStr("inventory.host_id", "two")
		}, "error_invalid_host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, _ := Fixture("processing-errors", now)
			tc.change(x.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0))
			b, err := p.Parse(x, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(b.Errors) != 4 || b.Diagnostics[tc.reason] != 1 {
				t.Fatalf("bad error accepted: %+v", b)
			}
		})
	}
	// An overlapping configuration must not assign an error to an arbitrary app.
	cfg := config.Default()
	cfg.ErrorBoundaries = append(cfg.ErrorBoundaries, cfg.ErrorBoundaries[0])
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	b, err = (Parser{Config: *cfg}).Parse(traces, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Errors) != 4 || b.Diagnostics["ambiguous_error_boundary"] != 1 {
		t.Fatal("ambiguous error was accepted")
	}
	// Unset/OK boundaries are never proof of failed processing.
	eachSpan(traces, func(s ptrace.Span) { s.Status().SetCode(ptrace.StatusCodeUnset) })
	b, err = p.Parse(traces, now)
	if err != nil || len(b.Errors) != 0 {
		t.Fatal("non-error counted")
	}
}

func TestErrorReplayRetainsStatusButRemovesExceptionData(t *testing.T) {
	now := time.Now()
	traces, _ := Fixture("error-recovery", now)
	s := traces.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0)
	s.Status().SetMessage("private database details")
	e := s.Events().AppendEmpty()
	e.SetName("exception")
	e.Attributes().PutStr("exception.message", "private archive URL")
	encoded, err := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	clean, err := Normalize(encoded)
	if err != nil {
		t.Fatal(err)
	}
	s = clean.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0)
	if s.Status().Code() != ptrace.StatusCodeError || s.Status().Message() != "" || s.Events().Len() != 0 {
		t.Fatal("error replay leaked details or lost status")
	}
	b, err := testParser(t).Parse(clean, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Errors) != 5 || len(b.Completions) != 5 {
		t.Fatal("replay lost error/recovery evidence")
	}
}
