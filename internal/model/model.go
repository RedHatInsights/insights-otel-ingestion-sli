package model

import (
	"fmt"
	"strings"
	"unicode"
)

var Applications = []string{"ingress", "puptoo", "hbi", "engine", "advisor", "vulnerability", "patch"}
var Buckets = []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 5, 6, 8, 10, 15, 20, 25, 30, 40, 45, 50, 55, 60, 65, 75, 90, 120, 180, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400}

type Pair struct {
	Application string `json:"application"`
	Milestone   string `json:"milestone"`
}

func ValidPair(application, milestone string) bool {
	known := false
	for _, candidate := range Applications {
		known = known || candidate == application
	}
	return known && (milestone == "processing" || (milestone == "inventory_views" && (application == "advisor" || application == "vulnerability" || application == "patch")))
}

func Pairs() []Pair {
	var pairs []Pair
	for _, application := range Applications {
		pairs = append(pairs, Pair{application, "processing"})
		if ValidPair(application, "inventory_views") {
			pairs = append(pairs, Pair{application, "inventory_views"})
		}
	}
	return pairs
}

type Identity struct {
	Environment string `json:"environment"`
	OrgID       string `json:"org_id"`
	RequestID   string `json:"request_id"`
}

func ValidIdentifier(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) == -1
}

func (i Identity) Validate() error {
	if !ValidIdentifier(i.Environment, 128) || !ValidIdentifier(i.OrgID, 128) || !ValidIdentifier(i.RequestID, 1024) {
		return fmt.Errorf("invalid correlation identity")
	}
	return nil
}

type Start struct {
	Identity
	StartedAtNS int64
	TraceID     string
	SpanID      string
}

type Completion struct {
	Identity
	Pair
	HostID        string
	Outcome       string
	CompletedAtNS int64
	TraceID       string
	SpanID        string
	Evidence      string // marker or configured span_rule; marker has precedence.
}

type Batch struct {
	Starts      []Start
	Completions []Completion
	Diagnostics map[string]int
}

type Window struct {
	Pair
	Completed uint64   `json:"completed"`
	Within    uint64   `json:"within_60s"`
	Sum       float64  `json:"duration_sum_seconds"`
	Buckets   []uint64 `json:"buckets"`
}

type Snapshot struct {
	AsOfNS            int64    `json:"as_of_ns"`
	Windows           []Window `json:"windows"`
	PendingStarts     uint64   `json:"completions_missing_start"`
	ConflictingStarts uint64   `json:"completions_with_conflicting_start"`
	ConflictingHosts  uint64   `json:"completions_with_conflicting_host"`
	InvalidIntervals  uint64   `json:"invalid_intervals"`
	DeliveryDelay     float64  `json:"maximum_delivery_delay_seconds"`
	PendingAge        float64  `json:"oldest_pending_age_seconds"`
}
