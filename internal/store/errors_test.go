package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	bolt "go.etcd.io/bbolt"
)

func failed(name string, p model.Pair, at time.Time, attempt int) model.ProcessingError {
	return model.ProcessingError{Identity: id(name), Pair: p, HostID: "host-" + name, EndedAtNS: at.UnixNano(), TraceID: "trace-" + name, SpanID: fmt.Sprint(attempt)}
}

func TestProcessingErrorsDeduplicateRecoverAndSurviveRestart(t *testing.T) {
	cfg := testConfig(t)
	s := openTest(t, cfg)
	p := model.Pair{Application: "patch", Milestone: "processing"}
	pi := pairIndex(p)
	err1 := failed("a", p, testNow.Add(-time.Minute), 1)
	err2 := failed("a", p, testNow.Add(-30*time.Second), 2)
	b := model.Batch{Errors: []model.ProcessingError{err1, err2, err1}}
	apply(t, s, b, testNow)
	if got := snapshot(t, s, testNow); got.ErrorQuality[0] != 2 || got.Windows[pi].Errors != (model.ErrorCounts{}) {
		t.Fatalf("missing start became a failure: %+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, cfg)
	apply(t, s, model.Batch{Starts: []model.Start{start("a", testNow.Add(-2*time.Minute))}}, testNow)
	assertCounts := func(want model.ErrorCounts) {
		t.Helper()
		if got := snapshot(t, s, testNow).Windows[pi].Errors; got != want {
			t.Fatalf("error counts: got %v want %v", got, want)
		}
	}
	assertCounts(model.ErrorCounts{2, 1, 0, 1, 0, 0})
	apply(t, s, b, testNow)
	// Another application and another milestone cannot clear this failure.
	apply(t, s, model.Batch{Completions: []model.Completion{
		finish("a", model.Pair{Application: "advisor", Milestone: "processing"}, testNow),
		finish("a", model.Pair{Application: "patch", Milestone: "inventory_views"}, testNow),
	}}, testNow)
	assertCounts(model.ErrorCounts{2, 1, 0, 1, 0, 0})
	apply(t, s, model.Batch{Completions: []model.Completion{finish("a", p, testNow.Add(-10*time.Second))}}, testNow)
	assertCounts(model.ErrorCounts{2, 1, 1, 0, 0, 0})
	apply(t, s, model.Batch{Errors: []model.ProcessingError{failed("a", p, testNow, 3)}}, testNow)
	assertCounts(model.ErrorCounts{2, 1, 1, 0, 1, 1})
	// A late earlier completion reclassifies errors; it never revokes success.
	apply(t, s, model.Batch{Completions: []model.Completion{finish("a", p, time.Unix(0, err1.EndedAtNS))}}, testNow)
	assertCounts(model.ErrorCounts{0, 0, 0, 0, 3, 1})
	if snapshot(t, s, testNow).Windows[pi].Completed != 1 {
		t.Fatal("error revoked successful completion")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, cfg)
	defer s.Close()
	apply(t, s, b, testNow)
	assertCounts(model.ErrorCounts{0, 0, 0, 0, 3, 1})
	badHost := failed("a", p, testNow, 4)
	badHost.HostID = "different-host"
	apply(t, s, model.Batch{Errors: []model.ProcessingError{badHost}}, testNow)
	if got := snapshot(t, s, testNow); got.Windows[pi].Errors != (model.ErrorCounts{}) || got.ErrorQuality[2] != 4 || got.Windows[pi].Completed != 0 {
		t.Fatal("late host conflict did not retract both latency and error measurements")
	}
}

func TestProcessingErrorWindowMatchesRawOracle(t *testing.T) {
	s := openTest(t, testConfig(t))
	defer s.Close()
	rng := rand.New(rand.NewSource(51))
	p := model.Pair{Application: "advisor", Milestone: "processing"}
	type path struct {
		ends      []time.Time
		completed time.Time
	}
	var paths []path
	b := model.Batch{}
	for i := range 180 {
		name := fmt.Sprint(i)
		entry := path{}
		b.Starts = append(b.Starts, start(name, testNow.Add(-3*time.Hour)))
		for j := range 4 {
			end := testNow.Add(-time.Duration(rng.Intn(7300))*time.Second + time.Duration(j)*time.Nanosecond)
			if i == 0 {
				end = testNow.Add(time.Duration(j-1) * time.Nanosecond)
			} // clock-ahead retry cannot hide a current error
			entry.ends = append(entry.ends, end)
			b.Errors = append(b.Errors, failed(name, p, end, j))
		}
		if i%3 == 0 {
			entry.completed = testNow.Add(-time.Duration(rng.Intn(6000)) * time.Second)
			b.Completions = append(b.Completions, finish(name, p, entry.completed))
		}
		paths = append(paths, entry)
	}
	b.Starts = append(b.Starts, start("invalid-interval", testNow))
	b.Errors = append(b.Errors, failed("invalid-interval", p, testNow.Add(-time.Second), 1))
	apply(t, s, b, testNow)
	apply(t, s, b, testNow)
	if snapshot(t, s, testNow).ErrorQuality[3] != 1 {
		t.Fatal("negative processing-error interval was counted")
	}
	for _, at := range []time.Time{testNow, testNow.Add(time.Nanosecond), testNow.Add(17 * time.Second), testNow.Add(61 * time.Minute)} {
		var want model.ErrorCounts
		for _, entry := range paths {
			var affected, post bool
			for _, end := range entry.ends {
				if !end.After(at.Add(-time.Hour)) || end.After(at) {
					continue
				}
				if !entry.completed.IsZero() && !end.Before(entry.completed) {
					want[model.PostCompletionErrorAttempts]++
					post = true
				} else {
					want[model.ErrorAttempts]++
					affected = true
				}
			}
			if affected {
				want[model.AffectedErrorPaths]++
				if entry.completed.IsZero() {
					want[model.ErrorPathsWithoutSuccess]++
				} else {
					want[model.RecoveredErrorPaths]++
				}
			}
			if post {
				want[model.PostCompletionErrorPaths]++
			}
		}
		if got := snapshot(t, s, at).Windows[pairIndex(p)].Errors; got != want {
			t.Fatalf("at %s: got %v want %v", at, got, want)
		}
	}
}

func TestProcessingErrorConflictsCapacityAndPruning(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxErrorAttemptsPerRequest = 1
	cfg.StateRetention = 2 * time.Hour
	s := openTest(t, cfg)
	defer s.Close()
	p := model.Pair{Application: "hbi", Milestone: "processing"}
	pi := pairIndex(p)
	f := failed("a", p, testNow.Add(-time.Second), 1)
	apply(t, s, model.Batch{Starts: []model.Start{start("a", testNow.Add(-time.Minute))}, Errors: []model.ProcessingError{f}}, testNow)
	if err := s.Apply(context.Background(), model.Batch{Errors: []model.ProcessingError{failed("a", p, testNow, 2)}}, testNow); err == nil {
		t.Fatal("capacity overflow acknowledged")
	}
	if got := snapshot(t, s, testNow).Windows[pi].Errors; got != (model.ErrorCounts{1, 1, 0, 1, 0, 0}) {
		t.Fatal("overflow changed accepted evidence")
	}
	conflict := f
	conflict.EndedAtNS++
	apply(t, s, model.Batch{Errors: []model.ProcessingError{conflict}}, testNow)
	if got := snapshot(t, s, testNow); got.Windows[pi].Errors != (model.ErrorCounts{}) || got.ErrorQuality[4] != 1 {
		t.Fatal("conflicting replay was counted")
	}
	apply(t, s, model.Batch{Errors: []model.ProcessingError{f}}, testNow)
	if snapshot(t, s, testNow).ErrorQuality[4] != 1 {
		t.Fatal("duplicate resurrected conflicting evidence")
	}
	later := testNow.Add(3 * time.Hour)
	for _, name := range []string{"b", "c"} {
		apply(t, s, model.Batch{Starts: []model.Start{start(name, testNow.Add(-time.Minute))}, Errors: []model.ProcessingError{failed(name, p, testNow.Add(-time.Second), 1)}}, testNow)
	}
	if err := s.Prune(context.Background(), later, 2); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, later).Requests != 2 {
		t.Fatal("cleanup did not budget retained error attempts")
	}
	if err := s.Prune(context.Background(), later, 100); err != nil {
		t.Fatal(err)
	}
	apply(t, s, model.Batch{Errors: []model.ProcessingError{f}}, later)
	if got := snapshot(t, s, later); got.Requests != 0 || got.ErrorQuality[4] != 0 || got.Diagnostics["expired_evidence"] != 1 {
		t.Fatalf("prune/replay: %+v", got)
	}
}

func TestVersionOneStateUpgradesWithoutLosingMeasurements(t *testing.T) {
	cfg := testConfig(t)
	s := openTest(t, cfg)
	p := model.Pair{Application: "advisor", Milestone: "processing"}
	apply(t, s, model.Batch{Starts: []model.Start{start("a", testNow.Add(-time.Minute))}, Completions: []model.Completion{finish("a", p, testNow)}}, testNow)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		// Preserve the original field order of the version 1 contract.
		data := tx.Bucket(metaBucket).Get([]byte("contract"))
		var legacy struct {
			Version        int
			Environment    string
			Window, Target int64
			Start          json.RawMessage
		}
		if err := json.Unmarshal(data, &legacy); err != nil {
			return err
		}
		legacy.Version = 1
		encoded, err := json.Marshal(legacy)
		if err != nil {
			return err
		}
		if err = tx.Bucket(metaBucket).Put([]byte("contract"), encoded); err != nil {
			return err
		}
		if err = tx.DeleteBucket(errorObservationBucket); err != nil {
			return err
		}
		return tx.DeleteBucket(errorRollupBucket)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, cfg)
	defer s.Close()
	if total(snapshot(t, s, testNow)) != 1 {
		t.Fatal("upgrade lost latency measurements")
	}
	apply(t, s, model.Batch{Errors: []model.ProcessingError{failed("a", p, testNow.Add(-time.Second), 1)}}, testNow)
	if got := snapshot(t, s, testNow).Windows[pairIndex(p)]; got.Completed != 1 || got.Errors != (model.ErrorCounts{1, 1, 1, 0, 0, 0}) {
		t.Fatal("upgraded state did not correlate error")
	}
}

func BenchmarkProcessingErrors(b *testing.B) {
	s := openTest(b, testConfig(b))
	defer s.Close()
	b.ReportAllocs()
	p := model.Pair{Application: "advisor", Milestone: "processing"}
	for i := range b.N {
		batch := model.Batch{}
		for j := range 100 {
			name := fmt.Sprintf("%d-%d", i, j)
			batch.Starts = append(batch.Starts, start(name, testNow.Add(-time.Minute)))
			batch.Errors = append(batch.Errors, failed(name, p, testNow.Add(-time.Second), 1))
			if j%2 == 0 {
				batch.Completions = append(batch.Completions, finish(name, p, testNow))
			}
		}
		apply(b, s, batch, testNow)
		if _, err := s.Snapshot(context.Background(), testNow); err != nil {
			b.Fatal(err)
		}
	}
}
