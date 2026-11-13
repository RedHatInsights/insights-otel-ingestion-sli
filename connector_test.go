package ingestionconnector

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/otlp"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestCollectorLifecycleMetricsAndRecovery(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "connector.db")
	var mu sync.Mutex
	var frames []pmetric.Metrics
	firstFrame := make(chan struct{}, 1)
	sink, err := consumer.NewMetrics(func(_ context.Context, m pmetric.Metrics) error {
		mu.Lock()
		defer mu.Unlock()
		frames = append(frames, m)
		select {
		case firstFrame <- struct{}{}:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	factory := NewFactory()
	instance, err := factory.CreateTracesToMetrics(ctx, connector.Settings{ID: component.NewID(Type), TelemetrySettings: structTelemetry()}, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	c := instance.(*ingestionConnector)
	now := time.Now()
	c.now = func() time.Time { return now }
	if c.Capabilities().MutatesData {
		t.Fatal("must preserve the Tempo trace copy")
	}
	if err := c.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	// Finish the initial empty snapshot before asserting later measurements.
	select {
	case <-firstFrame:
	case <-time.After(time.Second):
		t.Fatal("initial snapshot not published")
	}
	traces, _ := otlp.Fixture("happy", now)
	// Generic HBI completion reporting also includes applications outside the SLI.
	events := traces.ResourceSpans().At(1).ScopeSpans().At(0).Spans().At(0).Events()
	ignored := events.AppendEmpty()
	events.At(0).CopyTo(ignored)
	ignored.Attributes().PutStr("rh.ingestion.application", "compliance")
	ignored.Attributes().PutStr("rh.ingestion.milestone", "inventory_views")
	before, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	for range 2 {
		if err := c.ConsumeTraces(ctx, traces); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if string(before) != string(after) {
		t.Fatal("mutated the application's traces")
	}
	c.publish(ctx)
	mu.Lock()
	latest := frames[len(frames)-1]
	mu.Unlock()
	verifyMetrics(t, latest, 10, 2)
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ConsumeTraces(ctx, traces); err == nil {
		t.Fatal("accepted traces after shutdown")
	}
	instance, err = factory.CreateTracesToMetrics(ctx, connector.Settings{ID: component.NewID(Type), TelemetrySettings: structTelemetry()}, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	c = instance.(*ingestionConnector)
	c.now = func() time.Time { return now }
	if err := c.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(ctx)
	if err := c.ConsumeTraces(ctx, traces); err != nil {
		t.Fatal(err)
	}
	c.publish(ctx)
	mu.Lock()
	latest = frames[len(frames)-1]
	mu.Unlock()
	// Diagnostic counts track deliveries, including replays; observations deduplicate.
	verifyMetrics(t, latest, 10, 3)
}

func TestMetricExportFailureDoesNotLoseAcceptedEvidence(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "connector.db")
	sink, _ := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return errors.New("metrics backend unavailable") })
	c := newConnector(*cfg, sink, zap.NewNop())
	ctx := context.Background()
	if err := c.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(ctx)
	traces, _ := otlp.Fixture("unchanged", time.Now())
	if err := c.ConsumeTraces(ctx, traces); err != nil {
		t.Fatal(err)
	}
	c.publish(ctx)
	s, err := c.state.Snapshot(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, w := range s.Windows {
		n += w.Completed
		if w.Milestone == "inventory_views" && w.Completed != 0 {
			t.Fatal("invented Views completion")
		}
	}
	if n != 7 {
		t.Fatalf("lost evidence during metrics outage: %d", n)
	}
}

func verifyMetrics(t *testing.T, m pmetric.Metrics, expected, expectedIgnored int64) {
	t.Helper()
	var total, ignored int64
	var seenBucket, seenSnapshot, seenIgnored bool
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		scopes := m.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < scopes.Len(); j++ {
			metrics := scopes.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.Type() != pmetric.MetricTypeGauge {
					t.Fatal("rolling measurements must be gauges")
				}
				points := metric.Gauge().DataPoints()
				for n := 0; n < points.Len(); n++ {
					p := points.At(n)
					if _, ok := p.Attributes().Get("rh.request_id"); ok {
						t.Fatal("request ID became a metric label")
					}
					switch metric.Name() {
					case "ingestion_sli_window_completed":
						total += p.IntValue()
					case "ingestion_sli_ignored_evidence":
						reason, _ := p.Attributes().Get("reason")
						if reason.Str() != "unsupported_pair" {
							t.Fatalf("unexpected ignored evidence reason: %s", reason.Str())
						}
						ignored += p.IntValue()
						seenIgnored = true
					case "ingestion_sli_rejected_evidence":
						if p.IntValue() != 0 {
							t.Fatal("well-formed out-of-scope marker counted as rejected evidence")
						}
					case "ingestion_sli_window_duration_seconds_bucket":
						if v, ok := p.Attributes().Get("le"); ok && v.Str() == "60" {
							seenBucket = true
						}
					case "ingestion_sli_snapshot_timestamp_seconds":
						seenSnapshot = p.DoubleValue() > 0
					}
				}
			}
		}
	}
	if total != expected || ignored != expectedIgnored || !seenBucket || !seenSnapshot || !seenIgnored {
		t.Fatalf("invalid SLA metrics: total=%d ignored=%d bucket=%t snapshot=%t", total, ignored, seenBucket, seenSnapshot)
	}
}

func structTelemetry() component.TelemetrySettings {
	return component.TelemetrySettings{Logger: zap.NewNop()}
}
