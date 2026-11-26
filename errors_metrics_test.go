package ingestionconnector

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/otlp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestErrorMetricsUseReviewedBoundariesAndDurableState(t *testing.T) {
	for _, scenario := range []string{"processing-errors", "error-recovery", "post-completion-errors"} {
		t.Run(scenario, func(t *testing.T) {
			cfg := config.Default()
			cfg.StoragePath = filepath.Join(t.TempDir(), "state.db")
			sink, _ := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return nil })
			c := newConnector(*cfg, sink, zap.NewNop())
			ctx := context.Background()
			now := time.Now()
			c.now = func() time.Time { return now }
			if err := c.Start(ctx, nil); err != nil {
				t.Fatal(err)
			}
			traces, err := otlp.Fixture(scenario, now)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
			for range 2 {
				if err := c.ConsumeTraces(ctx, traces); err != nil {
					t.Fatal(err)
				}
			}
			after, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
			if string(before) != string(after) {
				t.Fatal("modified original spans")
			}
			if err := c.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			c = newConnector(*cfg, sink, zap.NewNop())
			c.now = func() time.Time { return now }
			if err := c.Start(ctx, nil); err != nil {
				t.Fatal(err)
			}
			defer c.Shutdown(ctx)
			if err := c.ConsumeTraces(ctx, traces); err != nil {
				t.Fatal(err)
			}
			snapshot, err := c.state.Snapshot(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			m := buildMetrics(snapshot, c.cfg)
			values := map[string]int64{}
			metrics := m.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			for i := 0; i < metrics.Len(); i++ {
				metric := metrics.At(i)
				if metric.Type() != pmetric.MetricTypeGauge {
					t.Fatal("window metric became counter")
				}
				points := metric.Gauge().DataPoints()
				for j := 0; j < points.Len(); j++ {
					point := points.At(j)
					for _, name := range []string{"rh.org_id", "rh.request_id", "rh.host_id", "trace_id", "span_id"} {
						if _, ok := point.Attributes().Get(name); ok {
							t.Fatal("high-cardinality identifier exported")
						}
					}
					values[metric.Name()] += point.IntValue()
				}
			}
			if values["ingestion_sli_error_boundary_enabled"] != 5 {
				t.Fatal("invented Engine, Ingress, or Views error coverage")
			}
			wantAttempts, wantCompleted, wantRecovered, wantPost := int64(5), int64(0), int64(0), int64(0)
			if scenario != "processing-errors" {
				wantCompleted = 5
			}
			if scenario == "error-recovery" {
				wantRecovered = 5
			}
			if scenario == "post-completion-errors" {
				wantAttempts = 0
				wantPost = 5
			}
			for name, want := range map[string]int64{
				"ingestion_sli_window_processing_error_attempts":      wantAttempts,
				"ingestion_sli_window_processing_error_paths":         wantAttempts,
				"ingestion_sli_window_recovered_error_paths":          wantRecovered,
				"ingestion_sli_window_error_paths_without_success":    wantAttempts - wantRecovered,
				"ingestion_sli_window_post_completion_error_attempts": wantPost,
				"ingestion_sli_window_post_completion_error_paths":    wantPost,
				"ingestion_sli_window_completed":                      wantCompleted,
			} {
				if values[name] != want {
					t.Fatalf("%s: got %d want %d", name, values[name], want)
				}
			}
		})
	}
}
