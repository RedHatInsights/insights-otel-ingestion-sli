package store

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
)

var testNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func testConfig(t testing.TB) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "state.db")
	cfg.Environment = "test"
	return *cfg
}
func openTest(t testing.TB, cfg config.Config) *Store {
	t.Helper()
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func id(name string) model.Identity {
	return model.Identity{Environment: "test", OrgID: "org", RequestID: name}
}
func start(name string, at time.Time) model.Start {
	return model.Start{Identity: id(name), StartedAtNS: at.UnixNano(), TraceID: "trace-" + name, SpanID: "root"}
}
func finish(name string, pair model.Pair, at time.Time) model.Completion {
	return model.Completion{Identity: id(name), Pair: pair, HostID: "host-" + name, Outcome: "processed", CompletedAtNS: at.UnixNano(), Evidence: "marker"}
}
func apply(t testing.TB, s *Store, b model.Batch, now time.Time) {
	t.Helper()
	if err := s.Apply(context.Background(), b, now); err != nil {
		t.Fatal(err)
	}
}
func snapshot(t testing.TB, s *Store, now time.Time) Snapshot {
	t.Helper()
	v, err := s.Snapshot(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func total(s Snapshot) uint64 {
	var n uint64
	for _, w := range s.Windows {
		n += w.Completed
	}
	return n
}

func TestOutOfOrderDedupAndRestartAllMilestones(t *testing.T) {
	cfg := testConfig(t)
	s := openTest(t, cfg)
	b := model.Batch{}
	for i, p := range pairs {
		b.Completions = append(b.Completions, finish("a", p, testNow.Add(time.Duration(i-20)*time.Second)))
	}
	apply(t, s, b, testNow)
	if got := snapshot(t, s, testNow); total(got) != 0 || got.PendingStarts != 10 {
		t.Fatalf("pending: %+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, cfg)
	apply(t, s, model.Batch{Starts: []model.Start{start("a", testNow.Add(-time.Minute))}}, testNow)
	apply(t, s, b, testNow)
	got := snapshot(t, s, testNow)
	if total(got) != 10 || got.PendingStarts != 0 {
		t.Fatalf("dedup/restart: %+v", got)
	}
	for _, w := range got.Windows {
		if w.Completed != 1 || w.Within != 1 || w.Buckets[len(w.Buckets)-1] != 1 {
			t.Fatalf("invalid histogram: %+v", w)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, cfg)
	if got = snapshot(t, s, testNow); total(got) != 10 {
		t.Fatal("lost measurements after restart")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExactTargetWindowAndBacklog(t *testing.T) {
	s := openTest(t, testConfig(t))
	defer s.Close()
	for _, tc := range []struct {
		name     string
		end      time.Time
		duration time.Duration
	}{
		{"exact", testNow.Add(-2 * time.Minute), time.Minute},
		{"late", testNow.Add(-time.Minute), time.Minute + time.Nanosecond},
		{"lower_excluded", testNow.Add(-time.Hour), time.Second},
		{"lower_inside", testNow.Add(-time.Hour + time.Nanosecond), time.Second},
		{"upper_included", testNow, time.Second},
		{"future_excluded", testNow.Add(time.Nanosecond), time.Second},
		{"hours_in_kafka", testNow.Add(-3 * time.Minute), 3 * time.Hour},
	} {
		apply(t, s, model.Batch{Starts: []model.Start{start(tc.name, tc.end.Add(-tc.duration))}, Completions: []model.Completion{finish(tc.name, pairs[0], tc.end)}}, testNow)
	}
	w := snapshot(t, s, testNow).Windows[0]
	if w.Completed != 5 || w.Within != 3 {
		t.Fatalf("target/window: %+v", w)
	}
	// A completion exported after its measurement window must not enter today's window.
	apply(t, s, model.Batch{Starts: []model.Start{start("old", testNow.Add(-3*time.Hour))}, Completions: []model.Completion{finish("old", pairs[0], testNow.Add(-2*time.Hour))}}, testNow)
	if snapshot(t, s, testNow).Windows[0].Completed != 5 {
		t.Fatal("delivery time changed measurement population")
	}
}

func TestLateEarlierCompletionAndConflictsRetract(t *testing.T) {
	s := openTest(t, testConfig(t))
	defer s.Close()
	root := start("a", testNow.Add(-2*time.Minute))
	apply(t, s, model.Batch{Starts: []model.Start{root}, Completions: []model.Completion{finish("a", pairs[0], testNow.Add(-time.Second))}}, testNow)
	if snapshot(t, s, testNow).Windows[0].Within != 0 {
		t.Fatal("expected slow measurement")
	}
	apply(t, s, model.Batch{Completions: []model.Completion{finish("a", pairs[0], testNow.Add(-time.Minute))}}, testNow)
	if w := snapshot(t, s, testNow).Windows[0]; w.Completed != 1 || w.Within != 1 || w.Sum != 60 {
		t.Fatalf("earliest completion was not substituted: %+v", w)
	}
	bad := finish("a", pairs[1], testNow.Add(-time.Minute))
	bad.HostID = "another-host"
	apply(t, s, model.Batch{Completions: []model.Completion{bad}}, testNow)
	if got := snapshot(t, s, testNow); total(got) != 0 || got.ConflictingHosts != 2 {
		t.Fatalf("host conflict: %+v", got)
	}
	root.SpanID = "another-root"
	apply(t, s, model.Batch{Starts: []model.Start{root}}, testNow)
	if snapshot(t, s, testNow).ConflictingStarts != 2 {
		t.Fatal("different root accepted")
	}
	negative := finish("negative", pairs[0], testNow.Add(-time.Minute))
	apply(t, s, model.Batch{Starts: []model.Start{start("negative", testNow)}, Completions: []model.Completion{negative}}, testNow)
	if snapshot(t, s, testNow).InvalidIntervals != 1 {
		t.Fatal("negative interval accepted")
	}
}

func TestRetentionCapacityAndRollback(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxRequests = 1
	cfg.StateRetention = 2 * time.Hour
	s := openTest(t, cfg)
	b := model.Batch{Starts: []model.Start{start("a", testNow.Add(-time.Minute))}, Completions: []model.Completion{finish("a", pairs[0], testNow)}}
	apply(t, s, b, testNow)
	if err := s.Apply(context.Background(), model.Batch{Starts: []model.Start{start("b", testNow)}, Completions: []model.Completion{finish("a", pairs[1], testNow)}}, testNow); err == nil {
		t.Fatal("capacity silently discarded state")
	}
	if total(snapshot(t, s, testNow)) != 1 {
		t.Fatal("failed batch partially committed")
	}
	later := testNow.Add(3 * time.Hour)
	if err := s.Prune(context.Background(), later, 100); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, later).Requests != 0 {
		t.Fatal("retention did not release state")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.StateRetention = 24 * time.Hour
	s = openTest(t, cfg)
	defer s.Close()
	apply(t, s, b, later)
	if got := snapshot(t, s, later); got.Requests != 0 || got.Diagnostics["expired_evidence"] != 2 {
		t.Fatalf("expired replay resurrected after retention change: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Apply(ctx, model.Batch{Starts: []model.Start{start("c", later)}}, later); err == nil {
		t.Fatal("cancelled transaction accepted")
	}
	other := cfg
	other.Environment = "another"
	if _, err := Open(other); err == nil {
		t.Fatal("same file was concurrently opened")
	}
}

func TestSnapshotMatchesRawOracleAndConcurrentReplay(t *testing.T) {
	s := openTest(t, testConfig(t))
	defer s.Close()
	rng := rand.New(rand.NewSource(42))
	b := model.Batch{}
	type expected struct {
		p        int
		end      int64
		duration int64
	}
	var values []expected
	for i := range 1200 {
		name := fmt.Sprintf("request-%d", i)
		end := testNow.Add(-time.Duration(rng.Intn(7200))*time.Second + time.Duration(i)*time.Nanosecond)
		duration := time.Duration(rng.Intn(120000)) * time.Millisecond
		b.Starts = append(b.Starts, start(name, end.Add(-duration)))
		p := i % len(pairs)
		b.Completions = append(b.Completions, finish(name, pairs[p], end))
		values = append(values, expected{p, end.UnixNano(), int64(duration)})
	}
	apply(t, s, b, testNow)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 3 {
				if err := s.Apply(context.Background(), b, testNow); err != nil {
					t.Error(err)
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 3 {
				if _, err := s.Snapshot(context.Background(), testNow); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	for _, at := range []time.Time{testNow, testNow.Add(17 * time.Second), testNow.Add(90 * time.Minute)} {
		got := snapshot(t, s, at)
		for p := range pairs {
			var count, within uint64
			bins := make([]uint64, len(model.Buckets)+1)
			for _, v := range values {
				if v.p != p || v.end <= at.Add(-time.Hour).UnixNano() || v.end > at.UnixNano() {
					continue
				}
				count++
				if v.duration <= int64(time.Minute) {
					within++
				}
				for j := bucket(v.duration); j < len(bins); j++ {
					bins[j]++
				}
			}
			w := got.Windows[p]
			if w.Completed != count || w.Within != within {
				t.Fatalf("oracle disagrees: %+v expected %d/%d", w, count, within)
			}
			for j := range bins {
				if w.Buckets[j] != bins[j] {
					t.Fatalf("histogram bucket %d: %d != %d", j, w.Buckets[j], bins[j])
				}
			}
		}
	}
}

func BenchmarkBatch(b *testing.B) {
	s := openTest(b, testConfig(b))
	defer s.Close()
	b.ReportAllocs()
	for i := range b.N {
		batch := model.Batch{}
		for j := range 100 {
			name := fmt.Sprintf("%d-%d", i, j)
			batch.Starts = append(batch.Starts, start(name, testNow.Add(-time.Minute)))
			for _, p := range pairs {
				batch.Completions = append(batch.Completions, finish(name, p, testNow))
			}
		}
		apply(b, s, batch, testNow)
	}
}
