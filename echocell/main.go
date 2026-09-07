// Command echocell is a fresh never-pushed pipeline, used to prove the
// source-first flow stands up a NEW pipeline with one token: materialize
// (which now births the pipeline record) then `revision activate`.
package main

import "github.com/graphene-ci/pipeline/pkg/pipeline"

type Params struct {
	Msg string `json:"msg"`
}
type Result struct {
	Echo string `json:"echo"`
}

func run(_ pipeline.Context, p Params) (Result, error) {
	return Result{Echo: "echo: " + p.Msg}, nil
}

func main() { pipeline.Main("echocell", run) }
