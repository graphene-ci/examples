// Command childcell is the smallest pipeline meant to be run as a CHILD:
// it takes a number, squares it, and returns — no infrastructure, so a
// fan-out of many cells is fast to watch. The suite pipeline starts N of
// these with pipeline.RunAll and collects their results.
package main

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params is one cell's input.
type Params struct {
	N int `json:"n"`
	// SleepSeconds keeps the cell running for a while — so a fan-out can be
	// caught mid-flight and cancelled, proving the cascade.
	SleepSeconds int `json:"sleepSeconds"`
	// FailAt makes the cell fail when N == FailAt (0 disables) — to prove a
	// failed child fails only its own handle, not its siblings, and never
	// loops the await.
	FailAt int `json:"failAt"`
}

// Result is what a cell returns.
type Result struct {
	N       int `json:"n"`
	Squared int `json:"squared"`
}

func run(ctx pipeline.Context, p Params) (Result, error) {
	if p.FailAt != 0 && p.N == p.FailAt {
		return Result{}, fmt.Errorf("cell n=%d deliberately failed", p.N)
	}
	if p.SleepSeconds > 0 {
		if err := workflow.Sleep(ctx, time.Duration(p.SleepSeconds)*time.Second); err != nil {
			return Result{}, err
		}
	}
	return Result{N: p.N, Squared: p.N * p.N}, nil
}

func main() {
	pipeline.Main("childcell", run)
}
