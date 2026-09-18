package main

import (
	"testing"
	"time"

	dockerlib "github.com/graphene-ci/library/docker"
	"github.com/graphene-ci/library/docker/dockertest"
	"github.com/graphene-ci/library/file/filetest"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/pipelinetest"
	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// The run owner and what the machines "wrote" — fixtures, not machines.
const (
	runId                = "test-stroppy-demo"
	runRef  ref.OwnerRef = "run/" + runId
	stand   ref.OwnerRef = "stand/stroppy-demo"
	logPath string       = "/var/lib/graphene-agent/work/jobs/bench.log"

	infraPassed = "...                                                   [100%]\n3 passed in 2.10s\n{\"cpus\": 2, \"diskMBps\": 312.5, \"connectP99Ms\": 0.41}\n"
	infraFailed = "=========== 1 failed, 2 passed in 3.40s ===========\n{\"cpus\": 2, \"diskMBps\": 12.0}\n"
	benchTail   = "=== bench summary ===\n  iterations_total                         6000.000\n  iteration_duration                       count=6000 avg=40.1 p(50)~=38.0 p(90)~=55.0 p(95)~=61.0 p(99)~=80.000\n"
)

var machines = []id.AgentId{dbAgent, runnerAgent}

// demoWorld is two connecting machines with docker and file resources
// simulated; every tool that runs as a job answers from a fixture.
func demoWorld(t *testing.T, infraOnRunner string, infraExit int) (*pipelinetest.World, Params, any) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	w := pipelinetest.Install(t, suite.NewTestWorkflowEnvironment())
	dockertest.Install(w, "29.8.1")
	filetest.Install(w)
	w.ConnectAfter(dbAgent, time.Second)
	w.ConnectAfter(runnerAgent, 2*time.Second)
	for _, agent := range machines {
		w.File(agent, workDir+"/out/junit.xml", []byte("<testsuites/>"))
	}
	w.File(runnerAgent, logPath, []byte(benchTail))
	params := Params{
		DbHost: "10.0.0.1", DbUser: "ubuntu", DbHostKey: "ssh-ed25519 AAAA-db",
		RunnerHost: "10.0.0.2:22", RunnerUser: "ubuntu", RunnerHostKey: "ssh-ed25519 AAAA-runner",
		Key: pipeline.UseSecret("demo-ssh-key"), Keep: time.Hour,
	}
	wf := pipelinetest.Workflow(w, pipelineId, run)
	dockertest.OnJob(w, dbAgent, "pg-ready").Return(dockerlib.JobReport{}, nil).Maybe()
	dockertest.OnJob(w, dbAgent, "infra-tests").Return(dockerlib.JobReport{Stdout: infraPassed}, nil).Maybe()
	dockertest.OnJob(w, runnerAgent, "infra-tests").Return(dockerlib.JobReport{ExitCode: infraExit, Stdout: infraOnRunner}, nil).Maybe()
	return w, params, wf
}

func phase(t *testing.T, w *pipelinetest.World, name ref.OwnerRef) string {
	t.Helper()
	r, ok := w.Resource(name)
	require.True(t, ok, "%s", name)
	return r.Phase
}

func TestDemoHappyPath(t *testing.T) {
	t.Parallel()
	w, params, wf := demoWorld(t, infraPassed, 0)
	var bench dockerlib.JobSpec
	w.OnAgentActivity(runnerAgent, "docker.job", mock.Anything, mock.MatchedBy(func(s dockerlib.JobSpec) bool {
		if s.Name == "bench" {
			bench = s
		}
		return s.Name == "bench"
	})).Return(dockerlib.JobReport{Tail: benchTail, LogPath: logPath}, nil).Once()
	w.OnAgentActivity(runnerAgent, "publish-metrics", mock.Anything, mock.Anything).Return(true, nil).Once()

	w.Env.ExecuteWorkflow(wf, params)
	require.NoError(t, w.Env.GetWorkflowError())
	w.Env.AssertExpectations(t)

	// The bench is described in docker's own terms: image, command, a
	// read-only bind of the directory the config file was written to.
	require.Equal(t, "host", string(bench.Host.NetworkMode))
	require.Equal(t, workDir, bench.Host.Mounts[0].Source)
	require.True(t, bench.Host.Mounts[0].ReadOnly)
	_, ok := w.Resource(ref.OwnerRef("file/" + string(runnerAgent) + "-opt-stroppy-demo-stroppy-config.json"))
	require.True(t, ok, "the config reached the runner as a File resource")

	var result Result
	require.NoError(t, w.Env.GetWorkflowResult(&result))
	require.Equal(t, "29.8.1", result.Db.DockerVersion)
	require.Equal(t, "test-pg", result.PostgresId)
	require.True(t, result.Runner.Infra.Passed)
	require.Equal(t, "3 passed in 2.10s", result.Runner.Infra.Summary)
	require.InDelta(t, 312.5, result.Db.Infra.DiskMBps, 0.001)
	require.NotEmpty(t, result.Db.Infra.JUnitDigest)
	require.InDelta(t, 6000, result.Bench.Iterations, 0.001)
	require.InDelta(t, 100, result.Bench.PerSecond, 0.001)
	require.InDelta(t, 80, result.Bench.P99Ms, 0.001)
	require.NotEmpty(t, result.LogDigest)

	// Ownership after the run: postgres and its machine's agent are on the
	// stand (Keep) with everything under that agent; reports and the log
	// are on the stand; the runner's agent and its files died with the run.
	require.Equal(t, "success", w.Outcome(runRef))
	w.AssertOwner(t, "docker/pg", "agent/db-1")
	w.AssertOwner(t, "agent/db-1", stand)
	w.AssertOwner(t, "artifact/stroppy-log-"+runId, stand)
	for _, agent := range machines {
		w.AssertOwner(t, ref.OwnerRef("artifact/infra-junit-"+string(agent)+"-"+runId), stand)
	}
	require.Equal(t, "deleted", phase(t, w, "agent/runner-1"))
	require.Equal(t, "deleted", phase(t, w, ref.OwnerRef("file/"+string(runnerAgent)+"-opt-stroppy-demo-tests-test-infra.py")))
	w.AssertNoLeaks(t)

	// The stand's TTL collects postgres with its agent; the artifacts stay.
	w.Advance(params.Keep + time.Minute)
	require.Equal(t, "deleted", phase(t, w, "docker/pg"))
	require.Equal(t, "deleted", phase(t, w, "agent/db-1"))
	w.AssertOwner(t, "artifact/stroppy-log-"+runId, stand)
	w.AssertNoLeaks(t)
}

// No load test on infrastructure that failed its own checks — but the junit
// reports of BOTH machines are kept: that is what one reads afterwards.
func TestDemoInfraFailureStopsBeforeBench(t *testing.T) {
	t.Parallel()
	w, params, wf := demoWorld(t, infraFailed, 1)

	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "infrastructure suite failed on runner-1: 1 failed, 2 passed in 3.40s")
	require.Equal(t, "failure", w.Outcome(runRef))
	for _, call := range w.Calls() {
		require.NotEqual(t, "publish-metrics", call.Name)
	}
	for _, agent := range machines {
		w.AssertOwner(t, ref.OwnerRef("artifact/infra-junit-"+string(agent)+"-"+runId), stand)
	}
	for _, name := range []ref.OwnerRef{"docker/pg", "agent/db-1", "agent/runner-1"} {
		require.Equal(t, "deleted", phase(t, w, name), "%s", name)
	}
	w.AssertNoLeaks(t)
}

// Keep is ignored on failure: everything is torn down; the log was handed to
// the stand BEFORE the failure and survives.
func TestDemoBenchFailureTearsDown(t *testing.T) {
	t.Parallel()
	w, params, wf := demoWorld(t, infraPassed, 0)
	dockertest.OnJob(w, runnerAgent, "bench").Return(dockerlib.JobReport{
		ExitCode: 1, LogPath: logPath,
		Tail: "Error: failed to run go workload: driver dispatch: context deadline exceeded\nUsage:\n  stroppy run …\n",
	}, nil).Once()

	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "stroppy exit 1: Error: failed to run go workload")
	require.Equal(t, "failure", w.Outcome(runRef))
	for _, name := range []ref.OwnerRef{"docker/pg", "agent/db-1", "agent/runner-1"} {
		require.Equal(t, "deleted", phase(t, w, name), "%s", name)
	}
	w.AssertOwner(t, "artifact/stroppy-log-"+runId, stand)
	w.AssertNoLeaks(t)
}

func TestDemoDockerInstallFailure(t *testing.T) {
	t.Parallel()
	w, params, wf := demoWorld(t, infraPassed, 0)
	w.OnAgentActivity(runnerAgent, "docker.install", mock.Anything).
		Return(dockerlib.InstallReport{}, temporal.NewNonRetryableApplicationError("no docker recipe for distribution: gentoo", "InstallFailed", nil)).Once()

	w.Env.ExecuteWorkflow(wf, params)
	require.ErrorContains(t, w.Env.GetWorkflowError(), "no docker recipe")
	_, exists := w.Resource("docker/pg")
	require.False(t, exists)
	w.AssertNoLeaks(t)
}
