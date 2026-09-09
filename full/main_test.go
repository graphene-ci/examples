package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	dockerlib "github.com/graphene-ci/library/docker"
	"github.com/graphene-ci/library/docker/dockertest"
	"github.com/graphene-ci/library/k8s/k8stest"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	compute "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/compute/v1alpha1"
	vpc "github.com/yandex-cloud/crossplane-provider-yc/apis/cluster/vpc/v1alpha1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

const (
	networkRef ref.OwnerRef = "k8s.vpc.yandex-cloud.jet.crossplane.io.v1alpha1.Network/net"
	subnetRef  ref.OwnerRef = "k8s.vpc.yandex-cloud.jet.crossplane.io.v1alpha1.Subnet/sub"
	vmRef      ref.OwnerRef = "k8s.compute.yandex-cloud.jet.crossplane.io.v1alpha1.Instance/vm-1"
)

func fullWorld(t *testing.T, seed bool, vmDelay time.Duration) (*pipelinetest.World, Params, any) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	dockertest.Install(w, "28.5.2")
	objects := k8stest.Install(w)
	net := &vpc.Network{}
	net.Status.SetConditions(xpv1.Available())
	sub := &vpc.Subnet{}
	sub.Status.SetConditions(xpv1.Available())
	vm := &compute.Instance{}
	vm.Status.AtProvider.ID = ptr("vm-fixture")
	vm.Status.AtProvider.Status = ptr("running")
	require.NoError(t, objects.Set(networkRef, net))
	require.NoError(t, objects.Set(subnetRef, sub))
	require.NoError(t, objects.After(vmDelay, vmRef, vm))
	w.ConnectAfter("edge-1", 2*time.Second)
	w.ConnectAfter("bare-1", time.Second)
	w.File("bare-1", "/var/log/perf/report.tgz", []byte("report content"))
	if seed {
		w.SeedArtifact("baseline-report", []byte("baseline content"))
	}
	params := Params{FolderId: "folder", Zone: "zone", ImageId: "image", Work: "benchmark", Keep: 10 * time.Minute,
		BareHost: "fixture.invalid", BareUser: "test", BareHostKey: "fixture-host-key", BareKey: pipeline.UseSecret("fixture-key")}
	wf := pipelinetest.Workflow(w, "perf-nightly", run)
	return w, params, wf
}

func TestFullLocal(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, true, 3*time.Second)
	w.OnAgentActivity("bare-1", "run-work", mock.Anything, params.Work).Return("benchmark passed", nil).Once()
	w.Env.ExecuteWorkflow(wf, params)
	require.NoError(t, w.Env.GetWorkflowError())
	var result Result
	require.NoError(t, w.Env.GetWorkflowResult(&result))
	require.Equal(t, "benchmark passed", result.Report)
	require.Equal(t, "vm-fixture", result.VMId)
	require.Equal(t, "28.5.2", result.DockerVersion)
	require.Equal(t, "test-hello", result.ContainerId)
	require.NotEmpty(t, result.BaselineDigest)
	w.Env.AssertExpectations(t)
	w.AssertOwner(t, networkRef, "stand/perf-nightly")
	w.AssertOwner(t, subnetRef, networkRef)
	w.AssertOwner(t, vmRef, subnetRef)
	w.AssertOwner(t, "agent/edge-1", vmRef)
	w.AssertOwner(t, "docker/hello", "agent/edge-1")
	w.AssertOwner(t, "agent/bare-1", "stand/perf-nightly")
	w.AssertOwner(t, "docker/pg", "docker/pg-exporter")
	w.AssertOwner(t, "artifact/perf-report", "stand/perf-nightly")
	w.AssertNoLeaks(t)
	exporter, ok := w.Resource("docker/pg-exporter")
	require.True(t, ok)
	require.Equal(t, []pipeline.Flow{{To: "docker/pg", Protocol: pipeline.TCP, Label: "postgres", Port: 5432}}, exporter.Flows)
	require.Equal(t, "success", w.Outcome("run/test-perf-nightly"))
	for _, agent := range []string{"edge-1", "bare-1"} {
		var found bool
		for _, call := range w.Calls() {
			if call.Name == "docker.install" && call.TaskQueue == "agent/"+agent+"/run/test-perf-nightly" {
				found = true
			}
		}
		require.True(t, found, "docker must be installed on %s", agent)
	}
	artifactRecord, _ := w.Resource("artifact/perf-report")
	var artifactState pipeline.ArtifactState
	require.NoError(t, json.Unmarshal(artifactRecord.State, &artifactState))
	content, exists := w.Blob(artifactState.Blob)
	require.True(t, exists)
	require.Equal(t, []byte("report content"), content)
	w.Advance(11 * time.Minute)
	for _, name := range []ref.OwnerRef{networkRef, subnetRef, vmRef, "agent/edge-1", "agent/bare-1", "docker/hello", "docker/pg", "docker/pg-exporter"} {
		r, exists := w.Resource(name)
		require.True(t, exists)
		require.Equal(t, "deleted", r.Phase, "%s", name)
	}
	w.AssertOwner(t, "artifact/perf-report", "stand/perf-nightly")
	baseline, _ := w.Resource("artifact/baseline-report")
	require.True(t, baseline.Foreign)
	require.Equal(t, "ready", baseline.Phase)
	w.AssertNoLeaks(t)
}

func TestFullWorkFailureCleansEverything(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, true, 0)
	w.OnAgentActivity("bare-1", "run-work", mock.Anything, params.Work).Return("", errors.New("benchmark failed")).Once()
	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "benchmark failed")
	require.Equal(t, "failure", w.Outcome("run/test-perf-nightly"))
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
	for _, name := range []ref.OwnerRef{networkRef, subnetRef, vmRef, "agent/edge-1", "agent/bare-1"} {
		r, ok := w.Resource(name)
		require.True(t, ok)
		require.Equal(t, "deleted", r.Phase)
	}
}

func TestFullCancellationDuringVMReadiness(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, true, time.Hour)
	w.Env.RegisterDelayedCallback(w.Env.CancelWorkflow, 10*time.Second)
	w.Env.ExecuteWorkflow(wf, params)
	require.True(t, temporal.IsCanceledError(w.Env.GetWorkflowError()))
	require.Equal(t, "canceled", w.Outcome("run/test-perf-nightly"))
	w.AssertNoLeaks(t)
	for _, call := range w.Calls() {
		require.NotEqual(t, "run-work", call.Name)
	}
}

func TestFullMissingForeignArtifact(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, false, 0)
	w.OnAgentActivity("bare-1", "run-work", mock.Anything, params.Work).Return("ok", nil).Once()
	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "baseline-report does not exist")
	_, exists := w.Resource("artifact/baseline-report")
	require.False(t, exists)
	// This pipeline reads the baseline output after handing its survivors away.
	// A later failure must preserve those explicit handoffs.
	w.AssertOwner(t, networkRef, "stand/perf-nightly")
	w.AssertNoLeaks(t)
	w.Env.AssertExpectations(t)
}

func TestFullDockerInstallFailure(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, true, 0)
	w.OnAgentActivity("bare-1", "run-work", mock.Anything, params.Work).Return("ok", nil).Once()
	w.OnAgentActivity("bare-1", "docker.install", mock.Anything).
		Return(dockerlib.InstallReport{}, temporal.NewNonRetryableApplicationError("install refused", "InstallFailed", nil)).Once()
	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "install refused")
	require.Empty(t, w.AgentState("bare-1").Capabilities)
	require.Len(t, w.AgentState("edge-1").Capabilities, 1)
	_, exists := w.Resource("docker/hello")
	require.False(t, exists)
	w.Env.AssertExpectations(t)
	w.AssertNoLeaks(t)
}

func TestFullVMCreationFailure(t *testing.T) {
	t.Parallel()
	w, params, wf := fullWorld(t, true, 0)
	w.FailResource(vmRef, errors.New("cloud quota exceeded"))
	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "cloud quota exceeded")
	for _, call := range w.Calls() {
		require.NotEqual(t, "run-work", call.Name)
	}
	w.AssertNoLeaks(t)
}
