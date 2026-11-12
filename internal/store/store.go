// Package store keeps correlation, deduplication and rolling measurements in one
// transactional embedded database. No trace payloads are retained.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/config"
	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/model"
	bolt "go.etcd.io/bbolt"
)

var (
	metaBucket        = []byte("meta")
	requestBucket     = []byte("requests")
	expiryBucket      = []byte("expiry")
	observationBucket = []byte("observations")
	rollupBucket      = []byte("rollups")
	pendingBucket     = []byte("pending")
	pairs             = model.Pairs()
)

type origin struct {
	NS          int64
	Trace, Span string
}
type terminal struct {
	NS, ReceivedNS int64
	Host, Outcome  string
}
type request struct {
	Identity                    model.Identity
	Start                       *origin
	Completions                 [10]*terminal
	StartConflict, HostConflict bool
	Host                        string
	LastEventNS                 int64
}
type observation struct {
	Pair                                int
	CompletedNS, DurationNS, ReceivedNS int64
}
type rollup struct {
	Count int64
	Sum   float64
	Bins  []int64
}
type metadata struct {
	CutoffNS    int64
	Requests    int64
	Quality     [4]int64 // missing start, conflicting start, conflicting host, invalid interval
	Diagnostics map[string]uint64
}
type Snapshot struct {
	model.Snapshot
	Diagnostics map[string]uint64
	Requests    int64
	FileBytes   int64
}
type Store struct {
	db  *bolt.DB
	cfg config.Config
}

func Open(cfg config.Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.StoragePath), 0750); err != nil {
		return nil, err
	}
	db, err := bolt.Open(cfg.StoragePath, 0660, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open correlation state: %w", err)
	}
	s := &Store{db: db, cfg: cfg}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{metaBucket, requestBucket, expiryBucket, observationBucket, rollupBucket, pendingBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		// Refuse reuse of a volume for another environment or measurement contract.
		header, err := json.Marshal(struct {
			Version        int
			Environment    string
			Window, Target int64
			Start          config.Selector
		}{1, cfg.Environment, int64(cfg.Window), int64(cfg.Target), cfg.Start})
		if err != nil {
			return err
		}
		b := tx.Bucket(metaBucket)
		if previous := b.Get([]byte("contract")); previous != nil && !bytes.Equal(header, previous) {
			return fmt.Errorf("state volume belongs to a different environment or evidence contract")
		}
		return b.Put([]byte("contract"), header)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func encode(value any) ([]byte, error) { return json.Marshal(value) }
func save(b *bolt.Bucket, key []byte, value any) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}
func load(b *bolt.Bucket, key []byte, value any) error {
	if data := b.Get(key); data != nil {
		return json.Unmarshal(data, value)
	}
	return nil
}
func readMeta(tx *bolt.Tx) (metadata, error) {
	m := metadata{Diagnostics: make(map[string]uint64)}
	err := load(tx.Bucket(metaBucket), []byte("state"), &m)
	return m, err
}
func timestamp(ns int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ns))
	return b
}
func indexKey(ns int64, key []byte) []byte { return append(timestamp(ns), key...) }
func requestKey(id model.Identity) []byte  { b, _ := encode(id); h := sha256.Sum256(b); return h[:] }
func minute(ns int64) int64                { return ns / int64(time.Minute) * int64(time.Minute) }
func observationKey(key []byte, o observation) []byte {
	return append(indexKey(o.CompletedNS, key), byte(o.Pair))
}
func rollupKey(o observation) []byte { return append(timestamp(minute(o.CompletedNS)), byte(o.Pair)) }
func pairIndex(p model.Pair) int {
	for i, pair := range pairs {
		if p == pair {
			return i
		}
	}
	return -1
}
func bucket(ns int64) int {
	return sort.Search(len(model.Buckets), func(i int) bool { return float64(ns)/1e9 <= model.Buckets[i] })
}

func evidence(r request) (map[int]observation, [4]int64, int64) {
	obs := make(map[int]observation)
	var quality [4]int64
	var pending int64
	for i, c := range r.Completions {
		if c == nil {
			continue
		}
		if r.Start == nil {
			quality[0]++
			if pending == 0 || c.ReceivedNS < pending {
				pending = c.ReceivedNS
			}
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
		if c.NS < r.Start.NS {
			quality[3]++
			continue
		}
		obs[i] = observation{Pair: i, CompletedNS: c.NS, DurationNS: c.NS - r.Start.NS, ReceivedNS: c.ReceivedNS}
	}
	return obs, quality, pending
}

func updateObservation(tx *bolt.Tx, key []byte, o observation, sign int64) error {
	b := tx.Bucket(observationBucket)
	if sign > 0 {
		if err := save(b, observationKey(key, o), o); err != nil {
			return err
		}
	} else if err := b.Delete(observationKey(key, o)); err != nil {
		return err
	}
	r := rollup{Bins: make([]int64, len(model.Buckets)+1)}
	rb := tx.Bucket(rollupBucket)
	rk := rollupKey(o)
	if err := load(rb, rk, &r); err != nil {
		return err
	}
	r.Count += sign
	r.Sum += float64(sign) * float64(o.DurationNS) / 1e9
	r.Bins[bucket(o.DurationNS)] += sign
	if r.Count < 0 || r.Bins[bucket(o.DurationNS)] < 0 {
		return fmt.Errorf("corrupt measurement accounting")
	}
	if r.Count == 0 {
		return rb.Delete(rk)
	}
	return save(rb, rk, r)
}

func reconcile(tx *bolt.Tx, m *metadata, key []byte, before, after request) error {
	old, oq, op := evidence(before)
	newObs, nq, np := evidence(after)
	for p, o := range old {
		if n, ok := newObs[p]; !ok || n != o {
			if err := updateObservation(tx, key, o, -1); err != nil {
				return err
			}
		}
	}
	for p, o := range newObs {
		if oldValue, ok := old[p]; !ok || oldValue != o {
			if err := updateObservation(tx, key, o, 1); err != nil {
				return err
			}
		}
	}
	for i := range m.Quality {
		m.Quality[i] += nq[i] - oq[i]
		if m.Quality[i] < 0 {
			return fmt.Errorf("corrupt quality accounting")
		}
	}
	pb := tx.Bucket(pendingBucket)
	if op != 0 {
		if err := pb.Delete(indexKey(op, key)); err != nil {
			return err
		}
	}
	if np != 0 {
		return pb.Put(indexKey(np, key), []byte{1})
	}
	return nil
}

func removeRequest(tx *bolt.Tx, m *metadata, key []byte, r request) error {
	if err := reconcile(tx, m, key, r, request{}); err != nil {
		return err
	}
	if err := tx.Bucket(expiryBucket).Delete(indexKey(r.LastEventNS, key)); err != nil {
		return err
	}
	if err := tx.Bucket(requestBucket).Delete(key); err != nil {
		return err
	}
	m.Requests--
	return nil
}

// Apply commits evidence, retractions, deduplication state and rollups atomically
// before the receiver acknowledges this batch. bbolt uses fsync by default.
func (s *Store) Apply(ctx context.Context, b model.Batch, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(b.Starts) == 0 && len(b.Completions) == 0 && len(b.Diagnostics) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		m, err := readMeta(tx)
		if err != nil {
			return err
		}
		m.CutoffNS = max(m.CutoffNS, now.Add(-s.cfg.StateRetention).UnixNano())
		for reason, n := range b.Diagnostics {
			m.Diagnostics[reason] += uint64(n)
		}
		type update struct {
			id          model.Identity
			starts      []model.Start
			completions []model.Completion
		}
		updates := make(map[string]*update)
		get := func(id model.Identity) *update {
			key := string(requestKey(id))
			u := updates[key]
			if u == nil {
				u = &update{id: id}
				updates[key] = u
			}
			return u
		}
		for _, v := range b.Starts {
			if v.StartedAtNS <= m.CutoffNS {
				m.Diagnostics["expired_evidence"]++
				continue
			}
			get(v.Identity).starts = append(get(v.Identity).starts, v)
		}
		for _, v := range b.Completions {
			if v.CompletedAtNS <= m.CutoffNS {
				m.Diagnostics["expired_evidence"]++
				continue
			}
			get(v.Identity).completions = append(get(v.Identity).completions, v)
		}
		for raw, u := range updates {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := []byte(raw)
			rb := tx.Bucket(requestBucket)
			previous := rb.Get(key)
			var before request
			if previous != nil {
				if err := json.Unmarshal(previous, &before); err != nil {
					return err
				}
				if before.Identity != u.id {
					return fmt.Errorf("correlation key collision")
				}
			}
			if previous != nil && before.LastEventNS <= m.CutoffNS {
				if err := removeRequest(tx, &m, key, before); err != nil {
					return err
				}
				before = request{}
				previous = nil
			}
			if previous == nil && m.Requests >= int64(s.cfg.MaxRequests) {
				return fmt.Errorf("max_requests reached; refusing to discard correlation state")
			}
			// Deep copy the fixed completion array; pointers are updated by replacement.
			after := before
			after.Identity = u.id
			for _, v := range u.starts {
				o := origin{NS: v.StartedAtNS, Trace: v.TraceID, Span: v.SpanID}
				if after.Start == nil {
					after.Start = &o
				} else if *after.Start != o {
					after.StartConflict = true
				}
				after.LastEventNS = max(after.LastEventNS, v.StartedAtNS)
			}
			for _, v := range u.completions {
				p := pairIndex(v.Pair)
				if p < 0 {
					return fmt.Errorf("invalid application/milestone")
				}
				if v.HostID != "" {
					if after.Host != "" && after.Host != v.HostID {
						after.HostConflict = true
					}
					if after.Host == "" {
						after.Host = v.HostID
					}
				}
				old := after.Completions[p]
				if old == nil || v.CompletedAtNS < old.NS {
					after.Completions[p] = &terminal{NS: v.CompletedAtNS, ReceivedNS: now.UnixNano(), Host: v.HostID, Outcome: v.Outcome}
				}
				after.LastEventNS = max(after.LastEventNS, v.CompletedAtNS)
			}
			if reflect.DeepEqual(before, after) {
				continue
			}
			if err := reconcile(tx, &m, key, before, after); err != nil {
				return err
			}
			if previous != nil {
				if err := tx.Bucket(expiryBucket).Delete(indexKey(before.LastEventNS, key)); err != nil {
					return err
				}
			} else {
				m.Requests++
			}
			if err := save(rb, key, after); err != nil {
				return err
			}
			if err := tx.Bucket(expiryBucket).Put(indexKey(after.LastEventNS, key), []byte{1}); err != nil {
				return err
			}
		}
		return save(tx.Bucket(metaBucket), []byte("state"), m)
	})
}

// Prune bounds each cleanup transaction. The persisted admission cutoff prevents
// old replayed evidence from reappearing after eviction or a retention increase.
func (s *Store) Prune(ctx context.Context, now time.Time, limit int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		m, err := readMeta(tx)
		if err != nil {
			return err
		}
		m.CutoffNS = max(m.CutoffNS, now.Add(-s.cfg.StateRetention).UnixNano())
		cursor := tx.Bucket(expiryBucket).Cursor()
		var keys [][]byte
		for k, _ := cursor.First(); k != nil && len(keys) < limit; k, _ = cursor.Next() {
			if int64(binary.BigEndian.Uint64(k[:8])) > m.CutoffNS {
				break
			}
			keys = append(keys, bytes.Clone(k[8:]))
		}
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			var r request
			if err := load(tx.Bucket(requestBucket), key, &r); err != nil {
				return err
			}
			if err := removeRequest(tx, &m, key, r); err != nil {
				return err
			}
		}
		return save(tx.Bucket(metaBucket), []byte("state"), m)
	})
}

// Snapshot combines minute rollups and the two partial boundary minutes, giving
// exactly (now-1h,now] by completion time without scanning retained history.
func (s *Store) Snapshot(ctx context.Context, now time.Time) (Snapshot, error) {
	result := Snapshot{Snapshot: model.Snapshot{AsOfNS: now.UnixNano()}}
	for _, p := range pairs {
		result.Windows = append(result.Windows, model.Window{Pair: p, Buckets: make([]uint64, len(model.Buckets)+1)})
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		m, err := readMeta(tx)
		if err != nil {
			return err
		}
		result.Diagnostics = m.Diagnostics
		result.Requests = m.Requests
		result.PendingStarts = uint64(m.Quality[0])
		result.ConflictingStarts = uint64(m.Quality[1])
		result.ConflictingHosts = uint64(m.Quality[2])
		result.InvalidIntervals = uint64(m.Quality[3])
		if key, _ := tx.Bucket(pendingBucket).Cursor().First(); key != nil {
			result.PendingAge = max(0, now.Sub(time.Unix(0, int64(binary.BigEndian.Uint64(key[:8])))).Seconds())
		}
		lower, upper := now.Add(-s.cfg.Window).UnixNano(), now.UnixNano()
		firstFull, lastFull := minute(lower)+int64(time.Minute), minute(upper)
		cursor := tx.Bucket(rollupBucket).Cursor()
		for k, v := cursor.Seek(timestamp(firstFull)); k != nil && int64(binary.BigEndian.Uint64(k[:8])) < lastFull; k, v = cursor.Next() {
			var r rollup
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			p := int(k[8])
			if p >= len(result.Windows) {
				return fmt.Errorf("invalid rollup pair")
			}
			w := &result.Windows[p]
			w.Completed += uint64(r.Count)
			w.Sum += r.Sum
			for i, n := range r.Bins {
				w.Buckets[i] += uint64(n)
			}
		}
		boundary := func(start, end int64) error {
			c := tx.Bucket(observationBucket).Cursor()
			for k, v := c.Seek(timestamp(start)); k != nil && int64(binary.BigEndian.Uint64(k[:8])) <= end; k, v = c.Next() {
				var o observation
				if err := json.Unmarshal(v, &o); err != nil {
					return err
				}
				w := &result.Windows[o.Pair]
				w.Completed++
				w.Sum += float64(o.DurationNS) / 1e9
				w.Buckets[bucket(o.DurationNS)]++
			}
			return nil
		}
		if err := boundary(lower+1, firstFull-1); err != nil {
			return err
		}
		if err := boundary(lastFull, upper); err != nil {
			return err
		}
		for i := range result.Windows {
			w := &result.Windows[i]
			for j := 1; j < len(w.Buckets); j++ {
				w.Buckets[j] += w.Buckets[j-1]
			}
			w.Within = w.Buckets[bucket(int64(s.cfg.Target))]
		}
		return nil
	})
	if info, statErr := os.Stat(s.cfg.StoragePath); statErr == nil {
		result.FileBytes = info.Size()
	}
	return result, err
}
