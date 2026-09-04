// Command suite is a PARENT pipeline: it fans out across N cells of the
// childcell pipeline with pipeline.RunAll, bounded to a concurrency, and
// aggregates their typed results. It demonstrates child runs — each cell
// is a full run of childcell, owned by this run, visible in this run's
// tree, and torn down if this run is cancelled.
package main

import (
	"fmt"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params drives the fan-out.
type Params struct {
	// Count is how many cells to run (childcell of n = 1..Count).
	Count int `json:"count"`
	// Concurrency bounds how many cells run at once; 0 means all.
	Concurrency int `json:"concurrency"`
	// SleepSeconds is passed to each cell — a slow fan-out to catch and
	// cancel mid-flight.
	SleepSeconds int `json:"sleepSeconds"`
}

// cellResult mirrors childcell's Result — the child returns its result as
// JSON and RunAll decodes it into this type.
type cellResult struct {
	N       int `json:"n"`
	Squared int `json:"squared"`
}

// Result is the suite's aggregate.
type Result struct {
	Cells    int `json:"cells"`
	Sum      int `json:"sum"`
	Failures int `json:"failures"`
}

func run(ctx pipeline.Context, p Params) (Result, error) {
	if p.Count <= 0 {
		p.Count = 3
	}
	cells := make([]pipeline.Cell, p.Count)
	for i := range cells {
		n := i + 1
		cells[i] = pipeline.Cell{ID: fmt.Sprintf("cell-%d", n), Params: childParams(n, p.SleepSeconds)}
	}
	handles := pipeline.RunAll[cellResult](ctx, "childcell", cells, p.Concurrency)

	out := Result{Cells: len(handles)}
	for _, h := range handles {
		// TryReady, not Ready: a failed cell must not abort the suite — the
		// parent decides what a partial result means.
		res, err := h.TryReady(ctx)
		if err != nil {
			out.Failures++
			continue
		}
		out.Sum += res.Squared
	}
	return out, nil
}

// childParams builds one childcell's params.
func childParams(n, sleepSeconds int) map[string]int {
	return map[string]int{"n": n, "sleepSeconds": sleepSeconds}
}

func main() {
	pipeline.Main("suite", run)
}
