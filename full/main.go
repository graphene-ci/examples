// Command target is the groomed user surface, compiling: user code is a
// mix of Resource, Activity, and plain control flow; everything else —
// registration, containers, delivery — is wiring the libraries hide.
//
// Groomed decisions living in this file:
//   - The primitive is the HANDLE (pipeline.Resource[...]), not the verb:
//     libraries bring their own constructors (k8sClient.Resource,
//     dockerlib.Container, pipeline.NewAgent), all returning the same
//     handle; the tree (Parent/Children), cascade, and visibility are
//     properties of the handle.
//   - Specs are FOREIGN structs (crossplane-provider-yc here): plain
//     fields their authors made — dependencies are explicit Ready calls,
//     never wrapped Output types.
//   - Library resources (their activities) run on the run's worker: the
//     user's kubeconfig never leaves the run's contour. The managed run
//     container lives until the LAST resource of the run is deleted; the
//     system keeps the resource tree to know that.
//   - Machine and Agent are two different resources: Machine is the real
//     hardware we do not operate (here it is the crossplane vm), Agent
//     is our process and record; its CloudInit identity goes into the
//     vm's user-data. (MachineIntent — later.)
//   - Long life is a TRANSFER: the pipeline's Stand always exists;
//     KeepFor bounds the stay under it.
package main

import (
	"context"
	"encoding/json"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/docker/docker/api/types/container"
	ycapis "github.com/yandex-cloud/crossplane-provider-yc/apis"
	compute "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/compute/v1alpha1"
	vpc "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/vpc/v1alpha1"
	corev1 "k8s.io/api/core/v1"

	dockerlib "github.com/graphene-ci/library/docker"
	k8slib "github.com/graphene-ci/library/k8s"
	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/machine"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/trigger"
)

// Params is the typed input of the run: the UI form and submit
// validation derive from this type.
type Params struct {
	// Event receives a webhook trigger's request body (reserved name).
	Event json.RawMessage `json:"event,omitempty"`

	FolderId string        `json:"folderId"`
	Zone     string        `json:"zone"`
	ImageId  string        `json:"imageId"`
	Work     string        `json:"work"`
	Keep     time.Duration `json:"keep"`

	// The bare machine: it already exists (a rack, somebody's VM) — the
	// system only puts the agent on it over ssh. The host key is
	// required deliberately: a control plane opening a root shell does
	// not trust-on-first-use.
	BareHost    string `json:"bareHost"`
	BareUser    string `json:"bareUser"`
	BareHostKey string `json:"bareHostKey"`
	// A secret-typed param: the NAME of a secret in the installation's
	// store travels; the schema marks the field, the door checks the
	// name exists before the run starts, the value resolves on the
	// server at the point of use.
	BareKey pipeline.SecretRef `json:"bareKey"`
}

// Result is the run's output: what the UI/CLI shows for the finished
// run. Small values only — big data goes through artifacts.
type Result struct {
	Report         string `json:"report"`
	DockerVersion  string `json:"dockerVersion"`
	ContainerId    string `json:"containerId"`
	VmId           string `json:"vmId"`
	BaselineDigest string `json:"baselineDigest"`
}

func main() {
	pipeline.Main("perf-nightly", run,
		pipeline.WithTriggers(
			// Trigger params are the pipeline's OWN Params type —
			// pipeline.Main refuses a drifted declaration at startup.
			// Environment values are REFERENCES (pipeline.Var,
			// pipeline.UseSecret), assigned on the installation — the
			// declaration carries no cloud ids and no key material.
			trigger.Cron("0 3 * * *", trigger.Params(Params{
				FolderId:    pipeline.Var("yc-folder"),
				Zone:        pipeline.Var("yc-zone"),
				ImageId:     pipeline.Var("yc-image"),
				Work:        "uname -a > /var/log/perf/nightly.txt",
				Keep:        10 * time.Minute,
				BareHost:    pipeline.Var("bare-host"),
				BareUser:    pipeline.Var("bare-user"),
				BareHostKey: pipeline.Var("bare-host-key"),
				BareKey:     pipeline.UseSecret("bare-ssh-key"),
			})),
			trigger.Webhook("push", trigger.HookSecret("gh-hook"),
				trigger.Params(Params{
					FolderId:    pipeline.Var("yc-folder"),
					Zone:        pipeline.Var("yc-zone"),
					ImageId:     pipeline.Var("yc-image"),
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
	return runBody(ctx, params)
}

func runBody(ctx pipeline.Context, params Params) (Result, error) {
	return func(ctx pipeline.Context, params Params) (Result, error) {

		// ctx carries ONLY what does not exist outside a run (RunId,
		// Logger); everything that acts is a free function taking ctx
		// first. Secret builds a REF into this pipeline's secret set (the
		// values are assigned to the pipeline on the server,
		// GitHub-style); only the name travels, the value resolves inside
		// activities at the point of use.
		// Crossplane and its provider are the USER'S cluster setup — the
		// pipeline assumes them, it does not declare them. WithScheme
		// teaches the client the provider's types: no hand-written
		// TypeMeta anywhere below.
		k8sClient := k8slib.NewClientFromSecret(pipeline.Secret(ctx, "kubeconfig"),
			k8slib.WithScheme(ycapis.AddToScheme))

		// Every k8s resource is a temporal-entity typed by OUR OWN type:
		// apply + converge, drift healing on a tick, delete on teardown.
		// Ready(ctx) returns the LIVE typed object — real ids come from
		// Status, put there by the provider, never invented by us.
		net := k8slib.Resource(ctx, k8sClient, "net", &vpc.Network{
			Spec: vpc.NetworkSpec{
				ForProvider: vpc.NetworkParameters{FolderID: &params.FolderId},
			},
		}, k8slib.WithReady(func(live *vpc.Network) bool {
			return xpReady(live.Status.GetCondition(xpv1.TypeReady))
		}))

		// Cross-resource wiring uses the provider's NATIVE Ref fields:
		// the cluster resolves the reference itself when the network is
		// ready — the real id never travels through workflow history and
		// cannot be wrong. Explicit Ready stays for WAITING and for
		// reading live status.
		sub := k8slib.Resource(ctx, k8sClient, "sub", &vpc.Subnet{
			Spec: vpc.SubnetSpec{
				ForProvider: vpc.SubnetParameters{
					NetworkIDRef: &xpv1.Reference{Name: "net"},
					Zone:         &params.Zone,
					V4CidrBlocks: []*string{ptr("10.0.0.0/24")},
				},
			},
		}, k8slib.WithReady(func(live *vpc.Subnet) bool {
			return xpReady(live.Status.GetCondition(xpv1.TypeReady))
		}), k8slib.WithResourceOption[vpc.Subnet](pipeline.Parent(net)))

		// Agent — OUR resource: record + identity of the process we run.
		// Machine (the real hardware) is a DIFFERENT resource we do not
		// operate; here the real machine is the crossplane vm below, so
		// no Machine record is declared.
		vmAgent := pipeline.NewAgent(ctx, "edge-1",
			pipeline.WithLabels(map[string]string{"role": "edge"}))

		// The OTHER road to a machine: it already exists, the system's
		// only touch is the ssh install of the agent — the same script a
		// fresh VM gets through user-data. The key is a secret NAME; the
		// value resolves on the server at the moment of the install.
		bareAgent := pipeline.NewAgentViaSSH(ctx, "bare-1", pipeline.SSHInstall{
			Address: params.BareHost,
			User:    params.BareUser,
			KeyRef:  params.BareKey,
			HostKey: params.BareHostKey,
		}, pipeline.WithLabels(map[string]string{"role": "edge"}))

		// The agent proves itself with what the machine IS: CloudInit
		// carries the identity into user-data, no secret leaks through
		// metadata. The tree link is declared from whichever side exists
		// later — the agent record had to exist before the vm, so the vm
		// claims it as a child.
		// The kind's knowledge is the USER'S, typed: readiness beyond the
		// default Ready-condition convention when needed — symmetric to a
		// temporal-entity kind definition.
		vm := k8slib.Resource(ctx, k8sClient, "vm-1", &compute.Instance{
			Spec: compute.InstanceSpec{
				ForProvider: compute.InstanceParameters{
					Zone:      &params.Zone,
					Resources: []compute.ResourcesParameters{{Cores: ptr(2.0), Memory: ptr(4.0)}},
					BootDisk: []compute.BootDiskParameters{{
						InitializeParams: []compute.InitializeParamsParameters{{ImageID: &params.ImageId, Size: ptr(20.0)}},
					}},
					NetworkInterface: []compute.NetworkInterfaceParameters{{
						SubnetIDRef: &xpv1.Reference{Name: "sub"},
						NAT:         ptr(true),
					}},
					Metadata: map[string]*string{"user-data": ptr(vmAgent.CloudInit())},
				},
			},
		},
			k8slib.WithReady(func(live *compute.Instance) bool {
				return live.Status.AtProvider.Status != nil && *live.Status.AtProvider.Status == "running"
			}),
			// Parent(sub) declares the DEPENDENCY in the tree: the vm,
			// its subnet, and the network are one chain — handing the vm
			// to the stand moves the chain's root, so a finishing run
			// never strands the subnet under itself.
			k8slib.WithResourceOption[compute.Instance](pipeline.Parent(sub), pipeline.Children(vmAgent)),
		)

		// Waiting is explicit — and the wait pays off in TYPED status:
		// the provider's own field, not our guess.
		_ = sub.Ready(ctx)
		vmId := vm.Ready(ctx).Status.AtProvider.ID

		// Inline user activity: name + body right where they are needed;
		// arguments travel only through the binding — explicit and
		// serializable. AtMostOnce: one-shot, an undeterminable outcome is
		// ErrUnknown, never a silent second execution.
		// The one-shot work runs on the bare machine; the cloud vm below
		// hosts the docker demo — one run, two machines, one binary.
		report, err := pipelineactivity.Activity(ctx, bareAgent,
			pipelineactivity.ActivityFn(
				"run-work",
				func(ctx context.Context, work string) (string, error) {
					// Work runs ON THE MACHINE: machine.Command chroots
					// into the host's filesystem — the distroless
					// executor image itself has no shell.
					out, err := machine.Command(ctx, "/bin/sh", "-c", work).CombinedOutput()
					return string(out), err
				},
				params.Work,
			),
			pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce), // default is AtLeastOnce
		)
		if err != nil {
			return Result{}, err
		}

		// The pipeline's OWN agent must be ready before a label snapshot
		// can see it: the selection below reads READY records — racing
		// it against the vm agent's connect handed the docker install
		// to the bare machine only.
		_ = vmAgent.Ready(ctx)

		// "Run it on all who are marked": select by record labels (a
		// snapshot; the selection is FOREIGN — no ownership taken), then
		// one call on every agent in parallel. The library verb is a
		// Call — the same value serves Activity and ActivityAll. The
		// install body itself publishes capability "docker" onto each
		// machine's record — written down where it happened.
		edges, err := pipeline.SelectAgents(ctx,
			pipeline.WithLabels(map[string]string{"role": "edge"}))
		if err != nil {
			return Result{}, err
		}
		installReports, err := pipelineactivity.ActivityAll(ctx, edges, dockerlib.Install())
		if err != nil {
			return Result{}, err
		}

		// A resource made by ANOTHER pipeline: recognized, never created,
		// never owned — and its blob is right there to fetch.
		baseline := pipeline.AttachArtifact(ctx, "baseline-report")

		// The library sets Parent(vmAgent) itself — the container is an
		// ORDINARY resource in the tree (visible in CLI/UI), only the
		// sugar hides it from this code. The spec is docker's own types.
		dockerContainer := dockerlib.Container(ctx, vmAgent, dockerlib.Spec{
			Name:   "hello",
			Config: &container.Config{Image: "mirror.gcr.io/library/nginx:alpine"},
			Host:   &container.HostConfig{RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways}},
		})

		// A real observability chain on the bare machine: postgres, its
		// exporter, and the flow between them — the exporter's metrics
		// reach obs through the container's observation beat (no sidecar
		// collector, no token on the container).
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
		// prometheus endpoint to scrape (Р-Н27); the beat ships pg_up and
		// the rest under the record's reference docker/pg-exporter.
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

		// An Artifact is declared with its SOURCE: where the bytes are is
		// part of the declaration, the upload is the wrapper's business
		// (an activity on the right site under the hood — the agent here;
		// artifact.FromBytes for bytes the run computed itself).
		reportArtifact := pipeline.NewArtifact(ctx, "perf-report",
			artifact.FromAgentFile(bareAgent, "/var/log/perf/report.tgz"),
		)

		// Long life is a TRANSFER, not a sleep: the pipeline's Stand
		// always exists. The vm (with its subtree) goes there — the
		// workflow returns immediately, the machine stays up; KeepFor
		// bounds the stay, the artifact stays until an explicit delete.
		pipeline.ToStand(ctx, vm, pipeline.KeepFor(params.Keep))
		pipeline.ToStand(ctx, reportArtifact)
		// The postgres chain stays on the stand too, so its exporter keeps
		// being scraped past the run — the metrics chain outlives the run
		// that built it (its beat ships pg_up on the stand's executor).
		pipeline.ToStand(ctx, pgExporter, pipeline.KeepFor(params.Keep))

		result := Result{
			Report:         report,
			DockerVersion:  installReports[0].Version,
			ContainerId:    dockerContainer.Ready(ctx).Id,
			BaselineDigest: baseline.Ready(ctx).Blob.Digest,
		}
		if vmId != nil {
			result.VmId = *vmId
		}
		return result, nil
	}(ctx, params)
}

// ptr is the small price of foreign specs made of pointers.
func ptr[T any](v T) *T { return &v }

// xpReady is the readiness of a crossplane managed resource: its Ready
// condition is True. This is the USER'S knowledge, typed and kept here —
// NOT in the k8s library, which knows only vanilla kstatus. A crossplane
// MR is reconciled asynchronously and for a beat after apply carries no
// conditions at all; the library's default (no conditions → ready by
// existing) would latch such a record ready before the cloud object even
// exists — before the provider authenticates. Every crossplane resource
// must carry this override so the tree tells the truth.
func xpReady(c xpv1.Condition) bool { return c.Status == corev1.ConditionTrue }
