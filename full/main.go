// Command target is the groomed user surface, compiling: user code is a
// mix of Resource, Activity, and plain control flow; everything else —
// registration, containers, delivery — is wiring the libraries hide.
//
// This is the observability chain on ONE existing machine: an ssh-installed
// agent, docker on it, postgres, its exporter, and the declared flow
// between them. The exporter's metrics reach obs through the container's
// observation beat (Р-Н27) and the pg←exporter edge is a declared flow
// (Р-Н25) — no crossplane, no cloud VM, so the cycle is deterministic and
// fast: the machine already exists.
//
// Groomed decisions living in this file:
//   - The primitive is the HANDLE (pipeline.Resource[...]), not the verb:
//     libraries bring their own constructors (dockerlib.Container,
//     pipeline.NewAgentViaSSH), all returning the same handle; the tree
//     (Parent/Children), cascade, and visibility are properties of it.
//   - Machine already exists: the only touch is the ssh install of the
//     agent — the same script a fresh VM would get through user-data.
//   - Long life is a TRANSFER: the pipeline's Stand always exists; KeepFor
//     bounds the stay under it, so the exporter keeps being scraped past
//     the run that built it.
package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/docker/docker/api/types/container"

	dockerlib "github.com/graphene-ci/library/docker"
	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/trigger"
)

// Params is the typed input of the run: the UI form and submit validation
// derive from this type.
type Params struct {
	// Event receives a webhook trigger's request body (reserved name).
	Event json.RawMessage `json:"event,omitempty"`

	Work string        `json:"work"`
	Keep time.Duration `json:"keep"`

	// The bare machine: it already exists (a rack, somebody's VM) — the
	// system only puts the agent on it over ssh. The host key is required
	// deliberately: a control plane opening a root shell does not
	// trust-on-first-use.
	BareHost    string `json:"bareHost"`
	BareUser    string `json:"bareUser"`
	BareHostKey string `json:"bareHostKey"`
	// A secret-typed param: the NAME of a secret in the installation's
	// store travels; the schema marks the field, the door checks the name
	// exists before the run starts, the value resolves on the server at the
	// point of use.
	BareKey pipeline.SecretRef `json:"bareKey"`
}

// Result is the run's output: what the UI/CLI shows for the finished run.
type Result struct {
	Report        string `json:"report"`
	DockerVersion string `json:"dockerVersion"`
	ExporterId    string `json:"exporterId"`
}

func main() {
	pipeline.Main("perf-nightly", run,
		pipeline.WithTriggers(
			// Trigger params are the pipeline's OWN Params type —
			// pipeline.Main refuses a drifted declaration at startup.
			// Environment values are REFERENCES (pipeline.Var,
			// pipeline.UseSecret), assigned on the installation.
			trigger.Cron("0 3 * * *", trigger.Params(Params{
				Work:        "uname -a > /var/log/perf/nightly.txt",
				Keep:        10 * time.Minute,
				BareHost:    pipeline.Var("bare-host"),
				BareUser:    pipeline.Var("bare-user"),
				BareHostKey: pipeline.Var("bare-host-key"),
				BareKey:     pipeline.UseSecret("bare-ssh-key"),
			})),
			trigger.Webhook("push", trigger.HookSecret("gh-hook"),
				trigger.Params(Params{
					Work:        "uname -a > /var/log/perf/hook.txt",
					Keep:        10 * time.Minute,
					BareHost:    pipeline.Var("bare-host"),
					BareUser:    pipeline.Var("bare-user"),
					BareHostKey: pipeline.Var("bare-host-key"),
					BareKey:     pipeline.UseSecret("bare-ssh-key"),
				})),
		),
		pipeline.WithConcurrency(pipeline.Queue),
	)
}

func run(ctx pipeline.Context, params Params) (Result, error) {
	// The OTHER road to a machine: it already exists, the system's only
	// touch is the ssh install of the agent. The key is a secret NAME; the
	// value resolves on the server at the moment of the install.
	bareAgent := pipeline.NewAgentViaSSH(ctx, "bare-1", pipeline.SSHInstall{
		Address: params.BareHost,
		User:    params.BareUser,
		KeyRef:  params.BareKey,
		HostKey: params.BareHostKey,
	}, pipeline.WithLabels(map[string]string{"role": "edge"}))

	// Docker on the machine: the install body publishes capability "docker"
	// onto the agent's record — written down where it happened.
	install, err := pipelineactivity.Activity(ctx, bareAgent, dockerlib.Install())
	if err != nil {
		return Result{}, err
	}

	// Inline user activity: name + body right where they are needed;
	// arguments travel only through the binding — explicit and
	// serializable. AtMostOnce: one-shot, an undeterminable outcome is
	// ErrUnknown, never a silent second execution.
	report, err := pipelineactivity.Activity(ctx, bareAgent,
		pipelineactivity.ActivityFn(
			"run-work",
			func(ctx context.Context, work string) (string, error) {
				// Work runs ON THE MACHINE: machine.Command chroots into the
				// host's filesystem — the distroless executor image itself
				// has no shell.
				out, err := machine.Command(ctx, "/bin/sh", "-c", work).CombinedOutput()
				return string(out), err
			},
			params.Work,
		),
		pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce),
	)
	if err != nil {
		return Result{}, err
	}

	// A real observability chain on the machine: postgres, its exporter,
	// and the flow between them — the exporter's metrics reach obs through
	// the container's observation beat (no sidecar collector, no token on
	// the container).
	pg := dockerlib.Container(ctx, bareAgent, dockerlib.Spec{
		Name: "pg",
		Config: &container.Config{
			Image: "mirror.gcr.io/library/postgres:16-alpine",
			Env:   []string{"POSTGRES_PASSWORD=graphene", "POSTGRES_HOST_AUTH_METHOD=trust"},
		},
		Host: &container.HostConfig{
			NetworkMode:   "host",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways},
		},
	})
	_ = pg.Ready(ctx)

	// The exporter declares its FLOW to postgres (Р-Н25) and its own
	// prometheus endpoint to scrape (Р-Н27); the beat ships pg_up and the
	// rest under the record's reference docker/pg-exporter.
	pgExporter := dockerlib.Container(ctx, bareAgent, dockerlib.Spec{
		Name: "pg-exporter",
		Config: &container.Config{
			Image: "quay.io/prometheuscommunity/postgres-exporter:latest",
			Env:   []string{"DATA_SOURCE_NAME=postgresql://postgres@localhost:5432/postgres?sslmode=disable"},
		},
		Host:   &container.HostConfig{NetworkMode: "host", RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways}},
		Scrape: "http://localhost:9187/metrics",
	},
		pipeline.WithFlowTo(pg, pipeline.TCP, "postgres", pipeline.FlowPort(5432)),
		pipeline.Children(pg),
	)
	_ = pgExporter.Ready(ctx)

	// The postgres chain stays on the stand past the run, so its exporter
	// keeps being scraped — the metrics chain outlives the run that built
	// it (its beat ships pg_up on the stand's executor). KeepFor bounds it.
	pipeline.ToStand(ctx, pgExporter, pipeline.KeepFor(params.Keep))

	return Result{
		Report:        report,
		DockerVersion: install.Version,
		ExporterId:    pgExporter.Ready(ctx).Id,
	}, nil
}
