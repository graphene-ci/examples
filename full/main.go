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
	"os/exec"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/docker/docker/api/types/container"
	ycapis "github.com/yandex-cloud/crossplane-provider-yc/apis"
	compute "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/compute/v1alpha1"
	vpc "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/vpc/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dockerlib "github.com/graphene-ci/library/docker"
	k8slib "github.com/graphene-ci/library/k8s"
	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params is the typed input of the run: the UI form and submit
// validation derive from this type.
type Params struct {
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
	pipeline.Main("perf-nightly", func(ctx pipeline.Context, params Params) (Result, error) {

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
		net := k8slib.Resource(ctx, k8sClient, &vpc.Network{
			ObjectMeta: metav1.ObjectMeta{Name: "net"},
			Spec: vpc.NetworkSpec{
				ForProvider: vpc.NetworkParameters{FolderID: &params.FolderId},
			},
		})

		// Cross-resource wiring uses the provider's NATIVE Ref fields:
		// the cluster resolves the reference itself when the network is
		// ready — the real id never travels through workflow history and
		// cannot be wrong. Explicit Ready stays for WAITING and for
		// reading live status.
		sub := k8slib.Resource(ctx, k8sClient, &vpc.Subnet{
			ObjectMeta: metav1.ObjectMeta{Name: "sub"},
			Spec: vpc.SubnetSpec{
				ForProvider: vpc.SubnetParameters{
					NetworkIDRef: &xpv1.Reference{Name: "net"},
					Zone:         &params.Zone,
					V4CidrBlocks: []*string{ptr("10.0.0.0/24")},
				},
			},
		}, k8slib.WithResourceOption[vpc.Subnet](pipeline.Parent(net)))

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
			KeyRef:  pipeline.Secret(ctx, "bare-ssh-key"),
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
		vm := k8slib.Resource(ctx, k8sClient, &compute.Instance{
			ObjectMeta: metav1.ObjectMeta{Name: "vm-1"},
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
			k8slib.WithResourceOption[compute.Instance](pipeline.Children(vmAgent)),
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
					out, err := exec.CommandContext(ctx, "sh", "-c", work).CombinedOutput()
					return string(out), err
				},
				params.Work,
			),
			pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce), // default is AtLeastOnce
		)
		if err != nil {
			return Result{}, err
		}

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
		targets := make([]pipelineactivity.Target, len(edges))
		for i, a := range edges {
			targets[i] = a
		}
		installReports, err := pipelineactivity.ActivityAll(ctx, targets, dockerlib.Install())
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
			Config: &container.Config{Image: "nginx:alpine"},
			Host:   &container.HostConfig{RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways}},
		})

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
	})
}

// ptr is the small price of foreign specs made of pointers.
func ptr[T any](v T) *T { return &v }
