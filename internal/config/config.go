package config

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
)

type Selector struct {
	Service           string            `mapstructure:"service"`
	Name              string            `mapstructure:"name"`
	Kind              string            `mapstructure:"kind"`
	Attributes        map[string]string `mapstructure:"attributes"`
	serviceRE, nameRE *regexp.Regexp
}

func (s *Selector) Compile() error {
	if s.Service == "" || s.Name == "" {
		return fmt.Errorf("start requires service and name expressions")
	}
	var err error
	if s.serviceRE, err = regexp.Compile(s.Service); err != nil {
		return err
	}
	if s.nameRE, err = regexp.Compile(s.Name); err != nil {
		return err
	}
	return nil
}

func (s Selector) Matches(service, name, kind string, attrs map[string]string) bool {
	if kind != s.Kind || s.serviceRE == nil || s.nameRE == nil || !s.serviceRE.MatchString(service) || !s.nameRE.MatchString(name) {
		return false
	}
	for key, value := range s.Attributes {
		if attrs[key] != value {
			return false
		}
	}
	return true
}

type ErrorBoundary struct {
	Selector    `mapstructure:",squash"`
	Application string `mapstructure:"application"`
	Milestone   string `mapstructure:"milestone"`
}

type Config struct {
	Environment                string          `mapstructure:"environment"`
	StoragePath                string          `mapstructure:"storage_path"`
	Window                     time.Duration   `mapstructure:"window"`
	Target                     time.Duration   `mapstructure:"target"`
	StateRetention             time.Duration   `mapstructure:"state_retention"`
	SnapshotInterval           time.Duration   `mapstructure:"snapshot_interval"`
	MaintenanceInterval        time.Duration   `mapstructure:"maintenance_interval"`
	MaintenanceBatchSize       int             `mapstructure:"maintenance_batch_size"`
	MaxRequests                int             `mapstructure:"max_requests"`
	MaxSpans                   int             `mapstructure:"max_spans"`
	FutureTolerance            time.Duration   `mapstructure:"future_tolerance"`
	SingleHost                 bool            `mapstructure:"single_host_per_ingestion"`
	Start                      Selector        `mapstructure:"start"`
	ErrorBoundaries            []ErrorBoundary `mapstructure:"error_boundaries"`
	MaxErrorAttemptsPerRequest int             `mapstructure:"max_error_attempts_per_request"`
}

func Default() *Config {
	return &Config{
		Environment: "local", StoragePath: "/var/lib/otelcol/ingestion.db",
		Window: time.Hour, Target: time.Minute, StateRetention: 24 * time.Hour,
		SnapshotInterval: 5 * time.Second, MaintenanceInterval: 30 * time.Second,
		MaintenanceBatchSize: 10000, MaxRequests: 10000000, MaxSpans: 50000,
		FutureTolerance: time.Minute, SingleHost: true,
		Start:                      Selector{Service: "^ingress$", Name: "^POST /api/ingress/v1/upload$", Kind: "SERVER"},
		MaxErrorAttemptsPerRequest: 256,
		ErrorBoundaries: []ErrorBoundary{
			{Selector: Selector{Service: "^insights-puptoo$", Name: "^puptoo\\.handle_message$", Kind: "CONSUMER"}, Application: "puptoo", Milestone: "processing"},
			{Selector: Selector{Service: "^host-inventory-mq$", Name: "^mq\\.process platform\\.inventory\\.host-ingress-p1$", Kind: "CONSUMER"}, Application: "hbi", Milestone: "processing"},
			{Selector: Selector{Service: "^insights-advisor-service$", Name: "^process platform\\.engine\\.results$", Kind: "CONSUMER"}, Application: "advisor", Milestone: "processing"},
			{Selector: Selector{Service: "^patchman-evaluator-upload$", Name: "^evaluate upload$", Kind: "INTERNAL"}, Application: "patch", Milestone: "processing"},
			{Selector: Selector{Service: "^vulnerability-engine-evaluator$", Name: "^evaluator _evaluate_system$", Kind: "INTERNAL"}, Application: "vulnerability", Milestone: "processing"},
		},
	}
}

func (c *Config) Validate() error {
	if !model.ValidIdentifier(c.Environment, 128) || !filepath.IsAbs(c.StoragePath) {
		return fmt.Errorf("environment and an absolute storage_path are required")
	}
	if c.Window != time.Hour || c.Target != time.Minute {
		return fmt.Errorf("the agreed SLI requires window: 1h and target: 60s")
	}
	if !c.SingleHost {
		return fmt.Errorf("this contract requires single_host_per_ingestion: true")
	}
	if c.StateRetention < 2*time.Hour || c.StateRetention > 30*24*time.Hour {
		return fmt.Errorf("state_retention must be between 2h and 720h")
	}
	if c.SnapshotInterval < time.Second || c.SnapshotInterval > time.Minute || c.MaintenanceInterval < time.Second || c.MaintenanceInterval > time.Minute {
		return fmt.Errorf("snapshot and maintenance intervals must be between 1s and 1m")
	}
	if c.MaintenanceBatchSize < 1 || c.MaintenanceBatchSize > 100000 || c.MaxRequests < 1 || c.MaxSpans < 1 || c.MaxSpans > 100000 {
		return fmt.Errorf("invalid capacity limits")
	}
	if c.FutureTolerance < 0 || c.FutureTolerance > 5*time.Minute {
		return fmt.Errorf("future_tolerance must be between 0 and 5m")
	}
	if c.Start.Kind != "SERVER" {
		return fmt.Errorf("Ingress start must be a SERVER span")
	}
	if err := c.Start.Compile(); err != nil {
		return err
	}
	if c.MaxErrorAttemptsPerRequest < 1 || c.MaxErrorAttemptsPerRequest > 4096 || len(c.ErrorBoundaries) > 64 {
		return fmt.Errorf("invalid processing error capacity limits")
	}
	for i := range c.ErrorBoundaries {
		rule := &c.ErrorBoundaries[i]
		if !model.ValidPair(rule.Application, rule.Milestone) || (rule.Kind != "CONSUMER" && rule.Kind != "INTERNAL" && rule.Kind != "SERVER") {
			return fmt.Errorf("invalid processing error boundary")
		}
		if err := rule.Compile(); err != nil {
			return err
		}
	}
	return nil
}
