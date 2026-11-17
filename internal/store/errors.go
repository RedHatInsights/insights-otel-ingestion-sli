package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	bolt "go.etcd.io/bbolt"
)

type processingError struct {
	Pair     int
	NS       int64
	Host     string `json:",omitempty"`
	Conflict bool   `json:",omitempty"`
}

const (
	errorAttempt = iota
	errorPath
	postCompletionPath
)

type errorObservation struct {
	Pair   int
	NS     int64
	Kind   int
	Counts model.ErrorCounts
}

// Each attempt contributes once at its end time. Each affected path contributes
// once at its latest qualifying error, making path counts exact over the same
// rolling window while retries remain distinct attempts. Explicit completion is
// authoritative: errors ending at/after it are separate operational diagnostics.
func errorEvidence(r request, upper int64) (map[string]errorObservation, [5]int64) {
	obs := make(map[string]errorObservation)
	var quality [5]int64
	var latest, post [10]int64
	completed := func(p int) *terminal {
		c := r.Completions[p]
		if c != nil && r.Start != nil && c.NS >= r.Start.NS {
			return c
		}
		return nil
	}
	for token, e := range r.Errors {
		if e.Conflict {
			quality[4]++
			continue
		}
		if r.Start == nil {
			quality[0]++
		}
		if r.StartConflict {
			quality[1]++
		}
		if r.HostConflict {
			quality[2]++
		}
		if r.Start == nil || r.StartConflict || r.HostConflict {
			continue
		}
		if e.NS < r.Start.NS {
			quality[3]++
			continue
		}
		if e.NS > upper {
			continue
		}
		o := errorObservation{Pair: e.Pair, NS: e.NS, Kind: errorAttempt}
		if c := completed(e.Pair); c != nil && c.NS <= e.NS {
			o.Counts[model.PostCompletionErrorAttempts] = 1
			post[e.Pair] = max(post[e.Pair], e.NS)
		} else {
			o.Counts[model.ErrorAttempts] = 1
			latest[e.Pair] = max(latest[e.Pair], e.NS)
		}
		obs["attempt/"+token] = o
	}
	for p := range pairs {
		if latest[p] != 0 {
			o := errorObservation{Pair: p, NS: latest[p], Kind: errorPath}
			o.Counts[model.AffectedErrorPaths] = 1
			if completed(p) != nil {
				o.Counts[model.RecoveredErrorPaths] = 1
			} else {
				o.Counts[model.ErrorPathsWithoutSuccess] = 1
			}
			obs[fmt.Sprintf("path/%d", p)] = o
		}
		if post[p] != 0 {
			o := errorObservation{Pair: p, NS: post[p], Kind: postCompletionPath}
			o.Counts[model.PostCompletionErrorPaths] = 1
			obs[fmt.Sprintf("post/%d", p)] = o
		}
	}
	return obs, quality
}

func errorObservationKey(key []byte, token string, o errorObservation) []byte {
	return append(indexKey(o.NS, key), []byte(token)...)
}

func updateErrorObservation(tx *bolt.Tx, key []byte, token string, o errorObservation, sign int64) error {
	b := tx.Bucket(errorObservationBucket)
	if sign > 0 {
		if err := save(b, errorObservationKey(key, token, o), o); err != nil {
			return err
		}
	} else if err := b.Delete(errorObservationKey(key, token, o)); err != nil {
		return err
	}
	rk := append(timestamp(minute(o.NS)), byte(o.Pair))
	rb := tx.Bucket(errorRollupBucket)
	var counts model.ErrorCounts
	if err := load(rb, rk, &counts); err != nil {
		return err
	}
	var nonzero bool
	for i, n := range o.Counts {
		counts[i] += sign * n
		if counts[i] < 0 {
			return fmt.Errorf("corrupt processing error accounting")
		}
		nonzero = nonzero || counts[i] != 0
	}
	if !nonzero {
		return rb.Delete(rk)
	}
	return save(rb, rk, counts)
}

func reconcileErrors(tx *bolt.Tx, m *metadata, key []byte, before, after request) error {
	if len(before.Errors) == 0 && len(after.Errors) == 0 {
		return nil
	}
	old, oq := errorEvidence(before, math.MaxInt64)
	next, nq := errorEvidence(after, math.MaxInt64)
	for id, o := range old {
		if n, ok := next[id]; !ok || n != o {
			if err := updateErrorObservation(tx, key, id, o, -1); err != nil {
				return err
			}
		}
	}
	for id, o := range next {
		if previous, ok := old[id]; !ok || previous != o {
			if err := updateErrorObservation(tx, key, id, o, 1); err != nil {
				return err
			}
		}
	}
	for i := range m.ErrorQuality {
		m.ErrorQuality[i] += nq[i] - oq[i]
		if m.ErrorQuality[i] < 0 {
			return fmt.Errorf("corrupt processing error quality accounting")
		}
	}
	return nil
}

// Snapshots scan minute rollups and the two boundary minutes, not all retained
// identities or attempts. Rare clock-ahead path anchors are corrected separately
// so a future retry cannot hide an earlier error in the current window.
func snapshotErrors(ctx context.Context, tx *bolt.Tx, result *Snapshot, lower, upper int64) error {
	add := func(p int, counts model.ErrorCounts) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p < 0 || p >= len(result.Windows) {
			return fmt.Errorf("invalid processing error pair")
		}
		for i, n := range counts {
			result.Windows[p].Errors[i] += n
		}
		return nil
	}
	firstFull, lastFull := minute(lower)+int64(time.Minute), minute(upper)
	c := tx.Bucket(errorRollupBucket).Cursor()
	for k, v := c.Seek(timestamp(firstFull)); k != nil && int64(binary.BigEndian.Uint64(k[:8])) < lastFull; k, v = c.Next() {
		var counts model.ErrorCounts
		if err := json.Unmarshal(v, &counts); err != nil {
			return err
		}
		if err := add(int(k[8]), counts); err != nil {
			return err
		}
	}
	boundary := func(start, end int64) error {
		c := tx.Bucket(errorObservationBucket).Cursor()
		for k, v := c.Seek(timestamp(start)); k != nil && int64(binary.BigEndian.Uint64(k[:8])) <= end; k, v = c.Next() {
			var o errorObservation
			if err := json.Unmarshal(v, &o); err != nil {
				return err
			}
			if err := add(o.Pair, o.Counts); err != nil {
				return err
			}
		}
		return nil
	}
	if err := boundary(lower+1, firstFull-1); err != nil {
		return err
	}
	if err := boundary(lastFull, upper); err != nil {
		return err
	}
	c = tx.Bucket(errorObservationBucket).Cursor()
	for k, v := c.Seek(timestamp(upper + 1)); k != nil; k, v = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var o errorObservation
		if err := json.Unmarshal(v, &o); err != nil {
			return err
		}
		if o.Kind == errorAttempt {
			continue
		}
		var r request
		if err := load(tx.Bucket(requestBucket), k[8:40], &r); err != nil {
			return err
		}
		observations, _ := errorEvidence(r, upper)
		if replacement, ok := observations[string(k[40:])]; ok && replacement.NS > lower {
			if err := add(replacement.Pair, replacement.Counts); err != nil {
				return err
			}
		}
	}
	return nil
}
