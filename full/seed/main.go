// Command seed is the "another pipeline" of the example: it publishes
// the baseline-report artifact the perf-nightly pipeline ATTACHES as a
// foreign resource. Run it once per installation.
package main

import (
	"fmt"

	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params is empty on purpose: the baseline is fixed content.
type Params struct{}

// Result reports where the baseline landed.
type Result struct {
	Digest string `json:"digest"`
}

func main() {
	pipeline.Main("baseline", func(ctx pipeline.Context, _ Params) (Result, error) {
		art := pipeline.NewArtifact(ctx, "baseline-report",
			artifact.FromBytes([]byte("baseline: perf reference v1\n")))
		state := art.Ready(ctx)
		if !state.Verified {
			return Result{}, fmt.Errorf("baseline artifact not verified")
		}
		// The artifact outlives the run on the pipeline's stand — no
		// TTL: a baseline stays until an explicit delete.
		pipeline.ToStand(ctx, art)
		return Result{Digest: state.Blob.Digest}, nil
	})
}
