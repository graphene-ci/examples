package stroppy

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A realistic tail of `stroppy run` output (pkg/bench/runtime.go summary).
const sampleLog = `2026-09-16T10:00:00Z INFO bench: warmup done
=== bench summary ===
  iterations_total                         6000.000
  iteration_duration                       count=6000 avg=40.100 p(50)~=38.000 p(90)~=55.000 p(95)~=61.000 p(99)~=80.000
`

func TestParse(t *testing.T) {
	t.Parallel()
	r := Parse(sampleLog, time.Minute)
	require.InDelta(t, 6000, r.Iterations, 0.001)
	require.InDelta(t, 100, r.PerSecond, 0.001)
	require.InDelta(t, 80, r.P99Ms, 0.001)
	require.True(t, strings.HasPrefix(r.Summary, "iterations_total"))
	require.Empty(t, Parse("nothing here", time.Minute).Summary)
}

func TestErrorLine(t *testing.T) {
	t.Parallel()
	out := "noise\nError: failed to run go workload: driver dispatch: context deadline exceeded\nUsage:\n  stroppy run …\n"
	require.Equal(t, "Error: failed to run go workload: driver dispatch: context deadline exceeded", ErrorLine(out))
	require.Equal(t, "just a tail", ErrorLine("just a tail\n"))
}

func TestConfigIsDeterministic(t *testing.T) {
	t.Parallel()
	req := Request{RunId: "r1", URL: "postgresql://x", VUs: 4, Duration: time.Minute}
	require.Equal(t, Config(req), Config(req))
	require.Contains(t, string(Config(req)), `"duration": "1m0s"`)
}
