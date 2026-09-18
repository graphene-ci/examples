// Package infra carries the infrastructure test suite INSIDE the pipeline
// binary and reads its answer back. The suite is plain pytest — see
// tests/test_infra.py; nothing here knows about Graphene or docker.
package infra

import (
	"embed"
	"encoding/json"
	"strings"
)

// Tests is the pytest suite, compiled into the pipeline with go:embed: the
// code travels with the binary, no repository to clone on the machine.
//
//go:embed tests/test_infra.py
var Tests embed.FS

// SuitePath is the suite's path inside Tests.
const SuitePath = "tests/test_infra.py"

// Report is what one machine's suite said.
type Report struct {
	Passed bool `json:"passed"`
	// Summary is pytest's own last line: "3 passed in 2.10s".
	Summary string `json:"summary"`
	// The measured numbers, from the suite's infra.json.
	CPUs         int     `json:"cpus,omitempty"`
	DiskMBps     float64 `json:"diskMBps,omitempty"`
	ConnectP99Ms float64 `json:"connectP99Ms,omitempty"`
	JUnitDigest  string  `json:"junitDigest,omitempty"`
}

// Parse reads the job's stdout: pytest's summary line, then the JSON the
// job's command printed from infra.json.
func Parse(exitCode int, stdout string) Report {
	r := Report{Passed: exitCode == 0}
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "{"):
			_ = json.Unmarshal([]byte(line), &r)
		case strings.Contains(line, " passed") || strings.Contains(line, " failed") || strings.Contains(line, " error"):
			r.Summary = strings.Trim(line, "= ")
		}
	}
	r.Passed = exitCode == 0 // the JSON must not overrule the exit status
	return r
}
