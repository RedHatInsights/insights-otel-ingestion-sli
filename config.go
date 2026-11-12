// Package ingestionconnector correlates Ingress starts with explicit application
// completion events and emits rolling, completion-time SLA gauges.
package ingestionconnector

import "github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"

type Config = config.Config
