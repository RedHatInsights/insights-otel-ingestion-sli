// fixture sends synthetic completion evidence through the real OTLP receiver.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/RedHatInsights/insights-otel-ingestion-sli/internal/otlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:14348/v1/traces", "OTLP HTTP traces URL")
	scenario := flag.String("scenario", "happy", "happy, slow, unchanged")
	flag.Parse()
	traces, err := otlp.Fixture(*scenario, time.Now())
	if err != nil {
		fatal(err)
	}
	data, err := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	if err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint, bytes.NewReader(data))
	if err != nil {
		fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fatal(fmt.Errorf("OTLP %s: %s", resp.Status, body))
	}
	fmt.Printf("Sent %s synthetic evidence: %d spans\n", *scenario, traces.SpanCount())
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
