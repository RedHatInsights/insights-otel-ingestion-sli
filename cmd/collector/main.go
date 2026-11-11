package main

import (
	"fmt"
	"os"

	ingestion "github.com/RedHatInsights/insights-otel-ingestion-sli"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

func factories() (otelcol.Factories, error) {
	return otelcol.Factories{
		Telemetry:  otelconftelemetry.NewFactory(),
		Receivers:  map[component.Type]receiver.Factory{component.MustNewType("otlp"): otlpreceiver.NewFactory()},
		Processors: map[component.Type]processor.Factory{component.MustNewType("memory_limiter"): memorylimiterprocessor.NewFactory()},
		Connectors: map[component.Type]connector.Factory{ingestion.Type: ingestion.NewFactory()},
		Exporters: map[component.Type]exporter.Factory{
			component.MustNewType("prometheus"): prometheusexporter.NewFactory(),
			component.MustNewType("debug"):      debugexporter.NewFactory(),
			component.MustNewType("otlp_http"):  otlphttpexporter.NewFactory(),
		},
		Extensions: map[component.Type]extension.Factory{component.MustNewType("health_check"): healthcheckextension.NewFactory()},
	}, nil
}

func main() {
	command := otelcol.NewCommand(otelcol.CollectorSettings{
		BuildInfo:              component.BuildInfo{Command: "insights-otel-ingestion-sli", Description: "Insights OpenTelemetry ingestion SLI Collector", Version: "0.1.0"},
		Factories:              factories,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{DefaultScheme: "file", ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), envprovider.NewFactory()}}},
	})
	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
