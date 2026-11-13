// Package otlp extracts correlation evidence, never whole-trace duration.
package otlp

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const CompletionEvent = "rh.ingestion.completed"

type Parser struct{ Config config.Config }

func Decode(payload []byte, encoding string) (ptrace.Traces, error) {
	switch encoding {
	case "otlp_proto":
		return (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(payload)
	case "otlp_json":
		return (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(payload)
	default:
		return ptrace.NewTraces(), fmt.Errorf("unsupported OTLP encoding")
	}
}

func Attributes(values pcommon.Map) map[string]string {
	out := make(map[string]string, values.Len())
	values.Range(func(key string, value pcommon.Value) bool {
		switch value.Type() {
		case pcommon.ValueTypeStr:
			out[key] = value.Str()
		case pcommon.ValueTypeInt:
			out[key] = strconv.FormatInt(value.Int(), 10)
		case pcommon.ValueTypeBool:
			out[key] = strconv.FormatBool(value.Bool())
		}
		return true
	})
	return out
}

func overlay(parent, child map[string]string) map[string]string {
	result := make(map[string]string, len(parent)+len(child))
	for key, value := range parent {
		result[key] = value
	}
	for key, value := range child {
		result[key] = value
	}
	return result
}

func (p Parser) Parse(traces ptrace.Traces, now time.Time) (model.Batch, error) {
	b := model.Batch{Diagnostics: make(map[string]int)}
	if traces.SpanCount() > p.Config.MaxSpans {
		return b, fmt.Errorf("OTLP span limit exceeded")
	}
	validTime := func(timestamp uint64) bool {
		return timestamp > 0 && timestamp <= math.MaxInt64 && int64(timestamp) <= now.Add(p.Config.FutureTolerance).UnixNano()
	}
	resources := traces.ResourceSpans()
	for i := 0; i < resources.Len(); i++ {
		resource := resources.At(i)
		resourceAttrs := Attributes(resource.Resource().Attributes())
		service := resourceAttrs["service.name"]
		scopes := resource.ScopeSpans()
		for j := 0; j < scopes.Len(); j++ {
			spans := scopes.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				attrs := overlay(resourceAttrs, Attributes(span.Attributes()))
				kind := strings.ToUpper(span.Kind().String())
				if span.TraceID().IsEmpty() || span.SpanID().IsEmpty() || !validTime(uint64(span.StartTimestamp())) || !validTime(uint64(span.EndTimestamp())) || span.EndTimestamp() < span.StartTimestamp() {
					b.Diagnostics["invalid_span"]++
					continue
				}
				traceID, spanID := span.TraceID().String(), span.SpanID().String()
				if conflict(resourceAttrs, Attributes(span.Attributes()), "rh.request_id") || conflict(resourceAttrs, Attributes(span.Attributes()), "rh.org_id") {
					b.Diagnostics["conflicting_identity"]++
					continue
				}
				if p.Config.Start.Matches(service, span.Name(), kind, attrs) {
					id := identity(p.Config.Environment, attrs)
					if id.Validate() != nil {
						b.Diagnostics["missing_identity"]++
					} else {
						b.Starts = append(b.Starts, model.Start{Identity: id, StartedAtNS: int64(span.StartTimestamp()), TraceID: traceID, SpanID: spanID})
					}
				}
				for n := 0; n < span.Events().Len(); n++ {
					event := span.Events().At(n)
					if event.Name() != CompletionEvent {
						continue
					}
					eventAttrs := Attributes(event.Attributes())
					if conflict(attrs, eventAttrs, "rh.request_id") || conflict(attrs, eventAttrs, "rh.org_id") {
						b.Diagnostics["conflicting_identity"]++
						continue
					}
					if parentHost, eventHost := hostID(attrs), hostID(eventAttrs); parentHost != "" && eventHost != "" && parentHost != eventHost {
						b.Diagnostics["conflicting_identity"]++
						continue
					}
					combined := overlay(attrs, eventAttrs)
					application, milestone, outcome := combined["rh.ingestion.application"], combined["rh.ingestion.milestone"], combined["rh.ingestion.outcome"]
					finished := uint64(event.Timestamp())
					if value, present := event.Attributes().Get("rh.ingestion.completed_at_ns"); present {
						// Nanoseconds must never pass through a lossy floating point value.
						if value.Type() == pcommon.ValueTypeInt && value.Int() > 0 {
							finished = uint64(value.Int())
						} else {
							b.Diagnostics["invalid_marker"]++
							continue
						}
					}
					id := identity(p.Config.Environment, combined)
					if id.Validate() != nil {
						b.Diagnostics["missing_identity"]++
						continue
					}
					if !model.ValidIdentifier(application, 128) || !model.ValidIdentifier(milestone, 128) || (outcome != "processed" && outcome != "unchanged") || !validTime(finished) {
						b.Diagnostics["invalid_marker"]++
						continue
					}
					if host := hostID(combined); !consistentHost(combined) || (host != "" && !model.ValidIdentifier(host, 128)) {
						b.Diagnostics["invalid_marker"]++
						continue
					}
					if !model.ValidPair(application, milestone) {
						// HBI also reports completions for applications outside this SLI.
						// Keep scope exclusions separate from malformed evidence.
						b.Diagnostics["unsupported_pair"]++
						continue
					}
					b.Completions = append(b.Completions, model.Completion{Identity: id, Pair: model.Pair{Application: application, Milestone: milestone}, HostID: hostID(combined), Outcome: outcome, CompletedAtNS: int64(finished), TraceID: traceID, SpanID: spanID, Evidence: "marker"})
				}

			}
		}
	}
	return b, nil
}

func identity(environment string, attrs map[string]string) model.Identity {
	return model.Identity{Environment: environment, OrgID: attrs["rh.org_id"], RequestID: attrs["rh.request_id"]}
}

func hostID(attrs map[string]string) string {
	for _, key := range []string{"rh.host_id", "inventory.host_id", "vulnerability.inventory_id"} {
		if value := attrs[key]; value != "" {
			return value
		}
	}
	return ""
}

func consistentHost(attrs map[string]string) bool {
	canonical := hostID(attrs)
	for _, key := range []string{"rh.host_id", "inventory.host_id", "vulnerability.inventory_id"} {
		if value := attrs[key]; value != "" && value != canonical {
			return false
		}
	}
	return true
}

func conflict(parent, child map[string]string, key string) bool {
	return parent[key] != "" && child[key] != "" && parent[key] != child[key]
}
