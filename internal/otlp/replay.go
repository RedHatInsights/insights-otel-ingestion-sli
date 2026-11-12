package otlp

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Normalize accepts OTLP JSON and Tempo's JSON trace response. Tempo encodes
// IDs as base64; standard OTLP JSON uses hex. Unknown attributes are discarded
// before replay so signed archive URLs and user data never leave the source.
func Normalize(payload []byte) (ptrace.Traces, error) {
	var object map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return ptrace.NewTraces(), err
	}
	if batches, ok := object["batches"]; ok {
		object["resourceSpans"] = batches
		delete(object, "batches")
	}
	var walk func(any) error
	walk = func(value any) error {
		switch v := value.(type) {
		case map[string]any:
			if old, ok := v["instrumentationLibrarySpans"]; ok {
				v["scopeSpans"] = old
				delete(v, "instrumentationLibrarySpans")
			}
			if old, ok := v["instrumentationLibrary"]; ok {
				v["scope"] = old
				delete(v, "instrumentationLibrary")
			}
			for _, key := range []string{"traceId", "spanId", "parentSpanId"} {
				id, ok := v[key].(string)
				if !ok || id == "" {
					continue
				}
				length := 8
				if key == "traceId" {
					length = 16
				}
				if decoded, err := hex.DecodeString(id); err == nil && len(decoded) == length {
					continue
				}
				decoded, err := base64.StdEncoding.DecodeString(id)
				if err != nil || len(decoded) != length {
					return fmt.Errorf("invalid trace/span ID")
				}
				v[key] = hex.EncodeToString(decoded)
			}
			for _, child := range v {
				if err := walk(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range v {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(object); err != nil {
		return ptrace.NewTraces(), err
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return ptrace.NewTraces(), err
	}
	traces, err := Decode(encoded, "otlp_json")
	if err != nil {
		return traces, err
	}
	Sanitize(traces)
	return traces, nil
}

func allowed(key string) bool {
	switch key {
	case "service.name", "deployment.environment", "deployment.environment.name", "rh.request_id", "rh.org_id", "rh.host_id", "inventory.host_id", "vulnerability.inventory_id", "rh.ingestion.application", "rh.ingestion.milestone", "rh.ingestion.outcome", "rh.ingestion.completed_at_ns", "rh.ingestion.route", "messaging.batch.message_count":
		return true
	}
	return false
}
func Sanitize(traces ptrace.Traces) {
	keep := func(attrs pcommon.Map) {
		attrs.RemoveIf(func(key string, _ pcommon.Value) bool { return !allowed(key) })
	}
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		r := traces.ResourceSpans().At(i)
		keep(r.Resource().Attributes())
		for j := 0; j < r.ScopeSpans().Len(); j++ {
			s := r.ScopeSpans().At(j)
			s.Scope().Attributes().Clear()
			for k := 0; k < s.Spans().Len(); k++ {
				span := s.Spans().At(k)
				keep(span.Attributes())
				span.Status().SetMessage("")
				span.Links().RemoveIf(func(ptrace.SpanLink) bool { return true })
				span.Events().RemoveIf(func(e ptrace.SpanEvent) bool { return e.Name() != CompletionEvent })
				for n := 0; n < span.Events().Len(); n++ {
					keep(span.Events().At(n).Attributes())
				}
			}
		}
	}
}

// Shift preserves durations, ordering, and integer nanosecond precision.
func Shift(traces ptrace.Traces, end time.Time) {
	var latest pcommon.Timestamp
	eachSpan(traces, func(s ptrace.Span) {
		if s.EndTimestamp() > latest {
			latest = s.EndTimestamp()
		}
	})
	if latest == 0 {
		return
	}
	delta := end.UnixNano() - int64(latest)
	shift := func(t pcommon.Timestamp) pcommon.Timestamp {
		if t == 0 {
			return t
		}
		return pcommon.Timestamp(int64(t) + delta)
	}
	eachSpan(traces, func(s ptrace.Span) {
		s.SetStartTimestamp(shift(s.StartTimestamp()))
		s.SetEndTimestamp(shift(s.EndTimestamp()))
		for i := 0; i < s.Events().Len(); i++ {
			e := s.Events().At(i)
			e.SetTimestamp(shift(e.Timestamp()))
			if value, ok := e.Attributes().Get("rh.ingestion.completed_at_ns"); ok && value.Type() == pcommon.ValueTypeInt {
				e.Attributes().PutInt("rh.ingestion.completed_at_ns", value.Int()+delta)
			}
		}
	})
}
func eachSpan(traces ptrace.Traces, fn func(ptrace.Span)) {
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		scopes := traces.ResourceSpans().At(i).ScopeSpans()
		for j := 0; j < scopes.Len(); j++ {
			spans := scopes.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				fn(spans.At(k))
			}
		}
	}
}

// Split deliberately removes ordering assumptions for integration exercises.
func Split(traces ptrace.Traces) []ptrace.Traces {
	var out []ptrace.Traces
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		resource := traces.ResourceSpans().At(i)
		for j := 0; j < resource.ScopeSpans().Len(); j++ {
			scope := resource.ScopeSpans().At(j)
			for k := 0; k < scope.Spans().Len(); k++ {
				part := ptrace.NewTraces()
				r := part.ResourceSpans().AppendEmpty()
				resource.Resource().CopyTo(r.Resource())
				s := r.ScopeSpans().AppendEmpty()
				scope.Scope().CopyTo(s.Scope())
				scope.Spans().At(k).CopyTo(s.Spans().AppendEmpty())
				out = append(out, part)
			}
		}
	}
	return out
}
