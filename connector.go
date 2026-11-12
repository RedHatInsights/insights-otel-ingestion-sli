package ingestionconnector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/otlp"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/store"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type ingestionConnector struct {
	cfg    Config
	next   consumer.Metrics
	logger *zap.Logger
	mu     sync.RWMutex
	state  *store.Store
	cancel context.CancelFunc
	done   chan struct{}
	now    func() time.Time
}

func newConnector(cfg Config, next consumer.Metrics, logger *zap.Logger) *ingestionConnector {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ingestionConnector{cfg: cfg, next: next, logger: logger, now: time.Now}
}

func (c *ingestionConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *ingestionConnector) Start(_ context.Context, _ component.Host) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != nil {
		return fmt.Errorf("connector already started")
	}
	if err := c.cfg.Validate(); err != nil {
		return err
	}
	s, err := store.Open(c.cfg)
	if err != nil {
		return err
	}
	c.state = s
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.done = make(chan struct{})
	go c.run(ctx)
	return nil
}

func (c *ingestionConnector) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state == nil {
		return fmt.Errorf("connector is not running")
	}
	now := c.now()
	batch, err := (otlp.Parser{Config: c.cfg}).Parse(traces, now)
	if err != nil {
		return err
	}
	return c.state.Apply(ctx, batch, now)
}

func (c *ingestionConnector) run(ctx context.Context) {
	defer close(c.done)
	flush := time.NewTicker(c.cfg.SnapshotInterval)
	defer flush.Stop()
	maintenance := time.NewTicker(c.cfg.MaintenanceInterval)
	defer maintenance.Stop()
	c.publish(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-flush.C:
			c.publish(ctx)
		case <-maintenance.C:
			c.mu.RLock()
			err := c.state.Prune(ctx, c.now(), c.cfg.MaintenanceBatchSize)
			c.mu.RUnlock()
			if err != nil && ctx.Err() == nil {
				c.logger.Error("Unable to prune correlation state", zap.Error(err))
			}
		}
	}
}

func (c *ingestionConnector) publish(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, c.cfg.SnapshotInterval)
	defer cancel()
	c.mu.RLock()
	defer c.mu.RUnlock()
	snapshot, err := c.state.Snapshot(ctx, c.now())
	if err == nil {
		err = c.next.ConsumeMetrics(ctx, buildMetrics(snapshot, c.cfg.Environment))
	}
	if err != nil && parent.Err() == nil {
		c.logger.Error("Unable to publish ingestion SLI snapshot", zap.Error(err))
	}
}

func (c *ingestionConnector) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if c.state == nil {
		c.mu.Unlock()
		return nil
	}
	c.cancel()
	done := c.done
	c.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.state.Close()
	c.state = nil
	return err
}
