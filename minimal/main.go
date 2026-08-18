// Command minimal is the story of ../full told with TODAY'S surface of
// the pipeline library: pipeline.Main as the entry point, typed params, a
// machine record linked to a real machine, one-shot work on it, teardown
// by the run's end. Everything the full sketch has beyond this — chained
// crossplane resources through k8slib, provider libraries, agent
// libraries — is future surface.
package main

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Params is the typed input of the run: the UI form and submit validation
// derive from this type.
type Params struct {
	// Host and User locate the existing machine the agent is installed
	// on over ssh (the full sketch creates a VM with crossplane instead
	// and feeds it pipeline.AgentUserData through user-data).
	Host string `json:"host"`
	User string `json:"user"`
	// HostKey is the machine's public key; required — no trust-on-first-use.
	HostKey string `json:"hostKey"`

	// Work is the command whose output becomes the report.
	Work string `json:"work"`

	// Keep leaves the machine record standing after the work for this
	// long — for the morning when somebody wants to look at what failed.
	Keep time.Duration `json:"keep"`
}

// RunWork executes the work on the machine: a named registered function
// running inside the per-(machine × run) container hosted by the agent —
// ordinary local code with the machine under its feet.
func RunWork(work string) (string, error) {
	// exec.Command(...) here in real life; the sketch keeps it visible.
	return "report of: " + work, nil
}

// PerfPipeline is the run: an ordinary Temporal workflow using the
// pipeline library.
func PerfPipeline(ctx workflow.Context, params Params) (string, error) {
	mid := id.MachineId("vm-" + string(pipeline.RunId(ctx)))

	// Declare the machine record: a LINK to an existing machine, with the
	// agent installed over ssh. The handle returns immediately; several
	// declarations in a row would converge in parallel.
	machine := pipeline.MachineViaSSH(ctx, mid, pipeline.SSHInstall{
		Address: params.Host,
		User:    params.User,
		KeyRef:  ref.SecretRef{Name: "ssh-key"},
		HostKey: params.HostKey,
	})

	// Outputs exist only behind Ready: the first read blocks until the
	// agent has connected.
	if _, err := machine.Ready(ctx); err != nil {
		return "", err
	}

	// One-shot work on the machine: at most once; an undeterminable
	// outcome is ErrUnknown — never a silent second execution.
	var report string
	if err := pipeline.Action(ctx, mid, pipeline.ExecOptions{}, RunWork, params.Work).Get(ctx, &report); err != nil {
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
	// One main == one pipeline. The role (run worker / machine container)
	// and the wiring come from the environment set by the server, the
	// agent, or the CLI.
	pipeline.Main("perf", PerfPipeline,
		pipeline.WithMachineFunctions(RunWork),
	)
}
