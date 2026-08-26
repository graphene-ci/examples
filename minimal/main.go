// Command minimal is the smallest complete pipeline: typed params, an
// agent installed on a machine that already exists, one-shot work on
// it, teardown when the run ends. Everything ../full has beyond this —
// crossplane resources through k8slib, provider libraries, triggers —
// is the same surface used more.
package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"

	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Params is the typed input of the run: the UI form and the submit
// validation both derive from this type.
type Params struct {
	// Host and User locate the existing machine the agent is installed
	// on over ssh (../full creates a VM with crossplane instead and
	// feeds it the agent's CloudInit through user-data).
	Host string `json:"host"`
	User string `json:"user"`
	// HostKey is the machine's public key; required — no
	// trust-on-first-use.
	HostKey string `json:"hostKey"`
	// Key names the secret holding the ssh private key. Only the NAME
	// travels; the value resolves on the server at the moment of the
	// install.
	Key ref.SecretRef `json:"key"`

	// Work is the command whose output becomes the report.
	Work string `json:"work"`

	// Keep leaves the agent record standing after the work for this
	// long — for the morning when somebody wants to look at what
	// failed.
	Keep time.Duration `json:"keep"`
}

// run is the pipeline: an ordinary workflow written with the library.
func run(ctx pipeline.Context, params Params) (string, error) {
	// Declare the agent: a machine that ALREADY exists, whose only
	// touch by the system is the ssh install. The handle returns
	// immediately; several declarations in a row converge in parallel.
	agent := pipeline.NewAgentViaSSH(ctx, "bare-"+string(ctx.RunId()), pipeline.SSHInstall{
		Address: params.Host,
		User:    params.User,
		KeyRef:  params.Key,
		HostKey: params.HostKey,
	})

	// Outputs exist only behind Ready: the first read blocks until the
	// agent has connected.
	if _, err := agent.TryReady(ctx); err != nil {
		return "", err
	}

	// One-shot work ON the machine. AtMostOnce: an undeterminable
	// outcome is an error, never a silent second execution.
	report, err := pipelineactivity.Activity(ctx, agent,
		pipelineactivity.ActivityFn(
			"run-work",
			func(ctx context.Context, work string) (string, error) {
				// machine.Command chroots into the host's filesystem —
				// the executor image itself has no shell.
				out, err := machine.Command(ctx, "/bin/sh", "-c", work).CombinedOutput()
				return string(out), err
			},
			params.Work,
		),
		pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce),
	)
	if err != nil {
		return "", err
	}

	// Keep window: the record (and the agent link) stands for a while.
	if params.Keep > 0 {
		if err := workflow.Sleep(ctx, params.Keep); err != nil {
			return "", err
		}
	}
	return report, nil
}

func main() {
	// One main == one pipeline. The role (run worker / machine
	// container) and the wiring come from the environment the server,
	// the agent or the CLI sets.
	pipeline.Main("minimal", run)
}
