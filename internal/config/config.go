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
	if s.Kind != "SERVER" {
		return fmt.Errorf("Ingress start must be a SERVER span")
	}
	return nil
}

func (s Selector) Matches(service, name, kind string, attrs map[string]string) bool {
	if s.serviceRE == nil || s.nameRE == nil || !s.serviceRE.MatchString(service) || !s.nameRE.MatchString(name) || kind != s.Kind {
		return false
	}
	for key, value := range s.Attributes {
		if attrs[key] != value {
			return false
		}
	}
	return true
}

type Config struct {
	Environment          string        `mapstructure:"environment"`
	StoragePath          string        `mapstructure:"storage_path"`
	Window               time.Duration `mapstructure:"window"`
	Target               time.Duration `mapstructure:"target"`
	StateRetention       time.Duration `mapstructure:"state_retention"`
	SnapshotInterval     time.Duration `mapstructure:"snapshot_interval"`
	MaintenanceInterval  time.Duration `mapstructure:"maintenance_interval"`
	MaintenanceBatchSize int           `mapstructure:"maintenance_batch_size"`
	MaxRequests          int           `mapstructure:"max_requests"`
	MaxSpans             int           `mapstructure:"max_spans"`
	FutureTolerance      time.Duration `mapstructure:"future_tolerance"`
	SingleHost           bool          `mapstructure:"single_host_per_ingestion"`
	Start                Selector      `mapstructure:"start"`
}

func Default() *Config {
	return &Config{
		Environment: "local", StoragePath: "/var/lib/otelcol/ingestion.db",
		Window: time.Hour, Target: time.Minute, StateRetention: 24 * time.Hour,
		SnapshotInterval: 5 * time.Second, MaintenanceInterval: 30 * time.Second,
		MaintenanceBatchSize: 10000, MaxRequests: 10000000, MaxSpans: 50000,
		FutureTolerance: time.Minute, SingleHost: true,
		Start: Selector{Service: "^ingress$", Name: "^POST /api/ingress/v1/upload$", Kind: "SERVER"},
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
	return c.Start.Compile()
}
