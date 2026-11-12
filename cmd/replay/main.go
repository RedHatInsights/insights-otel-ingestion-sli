// replay sends synthetic contract fixtures or sanitized existing Tempo traces to
// the local Collector. It never produces application Kafka messages.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/otlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func run() error {
	input := flag.String("input", "", "OTLP JSON file or Tempo trace response URL")
	scenario := flag.String("scenario", "happy", "Synthetic fixture: happy, slow, unchanged (ignored with input)")
	endpoint := flag.String("endpoint", "http://localhost:14348/v1/traces", "Collector OTLP HTTP endpoint")
	output := flag.String("output", "", "Write sanitized, shifted OTLP JSON instead of sending")
	shift := flag.Bool("shift-now", true, "Shift timestamps to end two seconds ago, preserving intervals")
	split := flag.Bool("split", true, "Send each span separately")
	reverse := flag.Bool("reverse", true, "Send spans in reverse order")
	duplicates := flag.Int("duplicates", 2, "Number of identical deliveries")
	flag.Parse()
	var traces ptrace.Traces
	var err error
	client := &http.Client{Timeout: 30 * time.Second}
	if *input == "" {
		traces, err = otlp.Fixture(*scenario, time.Now())
	} else {
		var payload []byte
		if strings.HasPrefix(*input, "http://") || strings.HasPrefix(*input, "https://") {
			response, e := client.Get(*input)
			if e != nil {
				return e
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				return fmt.Errorf("input returned %d", response.StatusCode)
			}
			payload, err = io.ReadAll(io.LimitReader(response.Body, 64<<20+1))
		} else {
			payload, err = os.ReadFile(*input)
		}
		if err == nil {
			if len(payload) > 64<<20 {
				return fmt.Errorf("input too large")
			}
			traces, err = otlp.Normalize(payload)
		}
	}
	if err != nil {
		return err
	}
	if *shift {
		otlp.Shift(traces, time.Now().Add(-2*time.Second))
	}
	if *output != "" {
		data, e := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
		if e != nil {
			return e
		}
		return os.WriteFile(*output, data, 0600)
	}
	if *duplicates < 1 || *duplicates > 100 {
		return fmt.Errorf("duplicates must be 1..100")
	}
	parts := []ptrace.Traces{traces}
	if *split {
		parts = otlp.Split(traces)
	}
	if *reverse {
		slices.Reverse(parts)
	}
	for d := 0; d < *duplicates; d++ {
		for _, part := range parts {
			data, e := (&ptrace.JSONMarshaler{}).MarshalTraces(part)
			if e != nil {
				return e
			}
			req, e := http.NewRequest(http.MethodPost, *endpoint, bytes.NewReader(data))
			if e != nil {
				return e
			}
			req.Header.Set("Content-Type", "application/json")
			if token := os.Getenv("OTLP_BEARER_TOKEN"); token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			response, e := client.Do(req)
			if e != nil {
				return e
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			if response.StatusCode != 200 {
				return fmt.Errorf("intake returned %d", response.StatusCode)
			}
		}
	}
	fmt.Printf("Replayed %d spans, %d deliveries each; split=%t reverse=%t\n", traces.SpanCount(), *duplicates, *split, *reverse)
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
