// Command minimal is the story of ../full told with TODAY'S surface of
// the pipeline library: typed params, a cloud machine owned by the run,
// work executed on it, teardown by the run's end (or a keep window
// first). Everything the full sketch has beyond this — chained resource
// outputs, provider libraries, agent libraries — is future surface.
package main

import (
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Params is the typed input of the run: the UI form and submit validation
// derive from this type.
type Params struct {
	Zone     string `json:"zone"`
	Image    string `json:"image"`
	Cores    int    `json:"cores"`
	MemoryGB int    `json:"memoryGB"`

	// Work is the command whose output becomes the report.
	Work string `json:"work"`

	// Keep leaves the machine standing after the work for this long —
	// for the morning when somebody wants to look at what failed.
	Keep time.Duration `json:"keep"`
}

// RunWork executes the work command on the machine. It is a registered
// named function — it runs inside the per-(machine × run) container
// hosted by the agent, so this is ordinary local code with the machine
// under its feet.
func RunWork(work string) (string, error) {
	// exec.Command(...) here in real life; the sketch keeps it visible.
	return "report of: " + work, nil
}

// PerfPipeline is the run: an ordinary Temporal workflow using the
// pipeline library.
func PerfPipeline(ctx workflow.Context, params Params) (string, error) {
	mid := id.MachineId("vm-" + string(pipeline.RunId(ctx)))

	// Declare the machine; the run owns it — the run's end tears it down
	// even if everything below fails.
	_, err := pipeline.Machine(ctx, mid, pipeline.MachineSpec{
		Cloud: &pipeline.CloudSource{
			Provider: "yandex",
			Params: map[string]string{
				"zone":     params.Zone,
				"image":    params.Image,
				"cores":    fmt.Sprint(params.Cores),
				"memoryGB": fmt.Sprint(params.MemoryGB),
			},
		},
	})
	if err != nil {
		return "", err
	}

	// One-shot work on the machine: at most once, an undeterminable
	// outcome is ErrUnknown — never a silent second execution.
	var report string
	if err := pipeline.Action(ctx, mid, pipeline.ExecOptions{}, RunWork, params.Work).Get(ctx, &report); err != nil {
		return "", err
	}

	// Keep window: the machine stands for a while after the work.
	if params.Keep > 0 {
		if err := workflow.Sleep(ctx, params.Keep); err != nil {
			return "", err
		}
	}
	return report, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	c, err := client.Dial(client.Options{})
	if err != nil {
		return err
	}
	defer c.Close()

	// The run worker side of the same binary. The machine-side container
	// registers RunWork on the machine's run queue; the wiring of roles
	// (managed / inplace / on-machine) is the Serve() surface to come.
	w := worker.New(c, wire.RunQueue("dev"), worker.Options{})
	w.RegisterWorkflow(PerfPipeline)
	w.RegisterActivity(RunWork)
	return w.Run(worker.InterruptCh())
}
