// Package stroppy is the pure half of running stroppy: it renders the config
// of a `simple` workload and reads the bench summary back. It knows nothing
// about Graphene or docker — the pipeline decides where and how stroppy runs.
package stroppy

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// Image is the stroppy release the demo runs.
const Image = "docker.stroppy.io/stroppy-io/stroppy:v6.0.0.62"

// Request is one `simple` workload run against a database.
type Request struct {
	RunId    string        `json:"runId"`
	URL      string        `json:"url"`
	VUs      int           `json:"vus"`
	Duration time.Duration `json:"duration"`
}

// Report is the bench summary stroppy printed.
type Report struct {
	Iterations float64 `json:"iterations"`
	PerSecond  float64 `json:"iterationsPerSecond"`
	P99Ms      float64 `json:"p99Ms"`
	Summary    string  `json:"summary"`
}

// Config renders stroppy-config.json. Deterministic: safe in workflow code.
func Config(req Request) []byte {
	raw, _ := json.MarshalIndent(map[string]any{
		"version": "1",
		"script":  "simple",
		"global": map[string]any{
			"runId":  req.RunId,
			"logger": map[string]any{"logLevel": "info", "logMode": "LOG_MODE_PRODUCTION"},
		},
		"drivers": map[string]any{"0": map[string]any{"driverType": "postgres", "url": req.URL}},
		"run":     map[string]any{"executor": "constant-vus", "vus": req.VUs, "duration": req.Duration.String()},
	}, "", "  ")
	return raw
}

// Args is the container command; the image's entrypoint is stroppy itself.
func Args(configPath string) []string {
	return []string{"run", "-f", configPath, "--log-mode", "production", "--log-level", "info"}
}

// Parse cuts the block stroppy prints at the end of a run — from
// "=== bench summary ===" to the end — and reads the headline numbers out of
// it. The block itself is the report: readable as it is.
func Parse(output string, duration time.Duration) Report {
	_, block, found := strings.Cut(output, "=== bench summary ===")
	if !found {
		return Report{}
	}
	r := Report{Summary: strings.TrimSpace(block)}
	for _, line := range strings.Split(block, "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) == 2 && f[0] == "iterations_total":
			r.Iterations, _ = strconv.ParseFloat(f[1], 64)
		case len(f) > 2 && f[0] == "iteration_duration":
			for _, kv := range f[1:] {
				if v, ok := strings.CutPrefix(kv, "p(99)~="); ok {
					r.P99Ms, _ = strconv.ParseFloat(v, 64)
				}
			}
		}
	}
	if duration > 0 {
		r.PerSecond = r.Iterations / duration.Seconds()
	}
	return r
}

// ErrorLine is stroppy's own "Error: …" line, or the end of the output when
// there is none (the usage text printed after an error is noise).
func ErrorLine(output string) string {
	for _, line := range strings.Split(output, "\n") {
		if t := strings.TrimSpace(line); strings.HasPrefix(t, "Error:") {
			return t
		}
	}
	output = strings.TrimSpace(output)
	if len(output) > 400 {
		return "…" + output[len(output)-400:]
	}
	return output
}

// Publish is an activity body: it makes the headline numbers METRICS of the
// run, stamped with the machine it runs on. Flushed right here — the
// machine's container is torn down when the run ends, before the periodic
// exporter would get to them.
func Publish(ctx context.Context, r Report) (bool, error) {
	obs.Gauge(ctx, "stroppy.iterations_per_second", r.PerSecond)
	obs.Gauge(ctx, "stroppy.iteration_duration_p99_ms", r.P99Ms)
	if mp, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider); ok {
		_ = mp.ForceFlush(ctx)
	}
	return true, nil
}
