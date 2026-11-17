package config

import "testing"

func TestErrorBoundaryConfigurationRejectsUnsafeRules(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"zero attempt limit", func(c *Config) { c.MaxErrorAttemptsPerRequest = 0 }},
		{"unbounded attempt limit", func(c *Config) { c.MaxErrorAttemptsPerRequest = 4097 }},
		{"client child boundary", func(c *Config) { c.ErrorBoundaries[0].Kind = "CLIENT" }},
		{"unknown application", func(c *Config) { c.ErrorBoundaries[0].Application = "unknown" }},
		{"invalid expression", func(c *Config) { c.ErrorBoundaries[0].Name = "[" }},
		{"nonserver ingress", func(c *Config) { c.Start.Kind = "CONSUMER" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.change(c)
			if c.Validate() == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	c := Default()
	c.ErrorBoundaries = nil
	if err := c.Validate(); err != nil {
		t.Fatal("cannot explicitly disable error inference", err)
	}
}
