package ingestionconnector

import (
	"context"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

var Type = component.MustNewType("ingestion_sli")

func NewFactory() connector.Factory {
	return connector.NewFactory(Type, func() component.Config { return config.Default() },
		connector.WithTracesToMetrics(createTracesToMetrics, component.StabilityLevelAlpha))
}

func createTracesToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Traces, error) {
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return newConnector(*c, next, set.Logger), nil
}
