// Command stroppy-demo is the two-machine benchmark demo: two existing
// Linux hosts join a run over ssh, both get docker, one hosts postgres, both
// pass a pytest infrastructure suite carried inside this binary, then
// stroppy loads the database from the second machine. Reports become
// artifacts, numbers become metrics, postgres stays on the stand.
//
// This file is the PIPELINE — what Graphene sees: params, resources,
// activities, ownership. Tools run as docker jobs described in docker's own
// terms; what a tool's output means lives in ./infra and ./stroppy.
package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"

	dockerlib "github.com/graphene-ci/library/docker"
	filelib "github.com/graphene-ci/library/file"
	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/file"
	"github.com/graphene-ci/pipeline/pkg/pipeline"

	"github.com/graphene-ci/examples/demo/infra"
	"github.com/graphene-ci/examples/demo/stroppy"
)

const (
	pipelineId    = "stroppy-demo"
	dbAgent       = "db-1"
	runnerAgent   = "runner-1"
	postgresImage = "docker.stroppy.io/library/postgres:16"
	pythonImage   = "docker.stroppy.io/astral-sh/uv:python3.12-bookworm-slim"
	pgContainer   = "pg"
	pgPassword    = "stroppy"
	pgPort        = "5432"
	// workDir is where brought files land on a machine; jobs bind it as /work.
	workDir = "/opt/stroppy-demo"
)

// Params is the typed input of a run: the CLI flags, the UI form and the
// door's validation all derive from this type.
type Params struct {
	// Two existing machines, reached over ssh; one key for both.
	DbHost        string `json:"dbHost" validate:"required"`
	DbUser        string `json:"dbUser" validate:"required"`
	DbHostKey     string `json:"dbHostKey" validate:"required"`
	RunnerHost    string `json:"runnerHost" validate:"required"`
	RunnerUser    string `json:"runnerUser" validate:"required"`
	RunnerHostKey string `json:"runnerHostKey" validate:"required"`
	// Key NAMES the secret with the private key; the value resolves on
	// the server at install time.
	Key pipeline.SecretRef `json:"key"`
	// DbAddr is how the runner reaches postgres (a private IP, say);
	// empty means the host part of DbHost.
	DbAddr string `json:"dbAddr,omitempty"`

	// The infrastructure suite's thresholds — pytest gets them as env.
	DiskMiB      int     `json:"diskMiB,omitempty"`
	MinDiskMBps  float64 `json:"minDiskMBps,omitempty"`
	MaxConnectMs float64 `json:"maxConnectMs,omitempty"`

	// The load.
	VUs      int           `json:"vus,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	// Keep leaves postgres (with its machine's agent) on the stand this
	// long after the run; zero tears everything down with the run.
	Keep time.Duration `json:"keep,omitempty"`
}

func (p Params) withDefaults() Params {
	if p.DbAddr == "" {
		p.DbAddr = p.DbHost
		if host, _, err := net.SplitHostPort(p.DbHost); err == nil {
			p.DbAddr = host
		}
	}
	p.DiskMiB = orDefault(p.DiskMiB, 256)
	p.MinDiskMBps = orDefault(p.MinDiskMBps, 50)
	p.MaxConnectMs = orDefault(p.MaxConnectMs, 20)
	p.VUs = orDefault(p.VUs, 4)
	p.Duration = orDefault(p.Duration, time.Minute)
	return p
}

// Machine is what the run learned about one machine.
type Machine struct {
	Addresses     []string     `json:"addresses,omitempty"`
	DockerVersion string       `json:"dockerVersion,omitempty"`
	Infra         infra.Report `json:"infra"`
}

// Result is what the run reports: small typed values. Big data (reports,
// logs) goes through artifacts.
type Result struct {
	Db         Machine        `json:"db"`
	Runner     Machine        `json:"runner"`
	PostgresId string         `json:"postgresId,omitempty"`
	Bench      stroppy.Report `json:"bench"`
	LogDigest  string         `json:"logDigest,omitempty"`
}

func main() {
	pipeline.Main(pipelineId, run)
}

// run is the pipeline: each step is a plain function of the context and the
// handles the previous steps produced.
func run(ctx pipeline.Context, params Params) (Result, error) {
	params = params.withDefaults()
	var res Result

	// Step 1 — two machines join the run.
	db, runner, err := connectMachines(ctx, params, &res)
	if err != nil {
		return res, err
	}
	// Step 2 — docker on both.
	if err := installDocker(ctx, db, runner, &res); err != nil {
		return res, err
	}
	// Step 3 — postgres on the database machine.
	pg, err := startPostgres(ctx, db, &res)
	if err != nil {
		return res, err
	}
	// Step 4 — OUR python: the pytest suite on both machines.
	if err := testInfra(ctx, params, db, runner, &res); err != nil {
		return res, err
	}
	// Step 5 — the load, its log as an artifact, the database left standing.
	return res, runBench(ctx, params, runner, pg, &res)
}

// Step 1. connectMachines declares both agents (ssh install, in parallel)
// and waits for them to connect. What the machine IS (os, cpus, memory,
// addresses) the agent reports itself at hello — the record carries it.
func connectMachines(ctx pipeline.Context, p Params, res *Result) (pipeline.AgentHandle, pipeline.AgentHandle, error) {
	db := pipeline.NewAgentViaSSH(ctx, dbAgent, pipeline.SSHInstall{
		Address: p.DbHost, User: p.DbUser, KeyRef: p.Key, HostKey: p.DbHostKey,
	}, pipeline.WithLabels(map[string]string{"role": "db"}))
	runner := pipeline.NewAgentViaSSH(ctx, runnerAgent, pipeline.SSHInstall{
		Address: p.RunnerHost, User: p.RunnerUser, KeyRef: p.Key, HostKey: p.RunnerHostKey,
	}, pipeline.WithLabels(map[string]string{"role": "runner"}))

	// Outputs exist only behind Ready: the first read blocks until the
	// agent has connected.
	dbState, err := db.TryReady(ctx)
	if err != nil {
		return db, runner, err
	}
	runnerState, err := runner.TryReady(ctx)
	if err != nil {
		return db, runner, err
	}
	res.Db.Addresses, res.Runner.Addresses = dbState.Addresses, runnerState.Addresses
	return db, runner, nil
}

// Step 2. installDocker converges the engine on both machines at once; the
// library body also publishes capability "docker" onto each record.
func installDocker(ctx pipeline.Context, db, runner pipeline.AgentHandle, res *Result) error {
	reports, err := pipelineactivity.ActivityAll(ctx, []pipelineactivity.Target{db, runner},
		dockerlib.Install(),
		pipelineactivity.WithTimeout(15*time.Minute),
		pipelineactivity.WithHeartbeat(time.Minute),
	)
	if err != nil {
		return err
	}
	res.Db.DockerVersion, res.Runner.DockerVersion = at(reports, 0).Version, at(reports, 1).Version
	return nil
}

// Step 3. startPostgres declares the container as an ORDINARY resource of
// the tree (owned by the db agent, visible in CLI/UI). "Running" is not
// "accepts connections": a one-shot job with postgres' own pg_isready waits
// for that.
func startPostgres(ctx pipeline.Context, db pipeline.AgentHandle, res *Result) (pipeline.Resource[dockerlib.Info], error) {
	pg := dockerlib.Container(ctx, db, dockerlib.Spec{
		Name: pgContainer,
		Config: &container.Config{
			Image: postgresImage,
			Env:   []string{"POSTGRES_PASSWORD=" + pgPassword},
		},
		Host: &container.HostConfig{
			NetworkMode:   "host",
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways},
		},
	})
	info, err := pg.TryReady(ctx)
	if err != nil {
		return pg, err
	}
	res.PostgresId = info.Id

	ready, err := pipelineactivity.Activity(ctx, db, dockerlib.Job(dockerlib.JobSpec{
		Name: "pg-ready",
		Config: &container.Config{
			Image: postgresImage,
			Cmd:   []string{"sh", "-c", "until pg_isready -h 127.0.0.1 -p " + pgPort + " -U postgres; do sleep 1; done"},
		},
		Host: &container.HostConfig{NetworkMode: "host"},
	}), pipelineactivity.WithTimeout(3*time.Minute))
	if err == nil && ready.ExitCode != 0 {
		err = fmt.Errorf("postgres is not ready: %s", ready.Tail)
	}
	return pg, err
}

// Step 4. testInfra runs OUR python on both machines: the pytest suite
// travels inside this binary (go:embed), a File resource puts it on each
// machine, a docker job runs it with the run's parameters as env. Docker's
// own words throughout: the suite reaches the container through a bind
// mount, the parameters through Env.
func testInfra(ctx pipeline.Context, p Params, db, runner pipeline.AgentHandle, res *Result) error {
	agents := []pipeline.AgentHandle{db, runner}
	// Declared together, awaited together: the two files converge in parallel.
	suites := make([]pipeline.Resource[filelib.Info], len(agents))
	for i, agent := range agents {
		suites[i] = filelib.File(ctx, agent, workDir+"/"+infra.SuitePath, file.FromEmbed(infra.Tests, infra.SuitePath))
	}
	for _, suite := range suites {
		if _, err := suite.TryReady(ctx); err != nil {
			return err
		}
	}
	reports, err := pipelineactivity.ActivityAll(ctx, []pipelineactivity.Target{db, runner},
		dockerlib.Job(dockerlib.JobSpec{
			Name: "infra-tests",
			Config: &container.Config{
				Image: pythonImage,
				Env: []string{
					"DISK_MIB=" + strconv.Itoa(p.DiskMiB),
					"MIN_DISK_MBPS=" + strconv.FormatFloat(p.MinDiskMBps, 'f', -1, 64),
					"MAX_CONNECT_MS=" + strconv.FormatFloat(p.MaxConnectMs, 'f', -1, 64),
					"PEER=" + net.JoinHostPort(p.DbAddr, pgPort),
				},
				// pytest decides the exit status; the measured numbers
				// follow on stdout for the typed result.
				Cmd: []string{"sh", "-c", "uv run --no-project --with pytest pytest -q -p no:cacheprovider -o junit_family=xunit1 " +
					"/work/" + infra.SuitePath + " --junitxml=/work/out/junit.xml; rc=$?; cat /work/out/infra.json; echo; exit $rc"},
			},
			Host: &container.HostConfig{
				NetworkMode: "host",
				Mounts:      []mount.Mount{{Type: mount.TypeBind, Source: workDir, Target: "/work"}},
			},
		}),
		pipelineactivity.WithTimeout(10*time.Minute),
		pipelineactivity.WithHeartbeat(2*time.Minute),
	)
	if err != nil {
		return err
	}
	res.Db.Infra, res.Runner.Infra = infra.Parse(at(reports, 0).ExitCode, at(reports, 0).Stdout), infra.Parse(at(reports, 1).ExitCode, at(reports, 1).Stdout)

	// The junit report of each machine is an ARTIFACT on the stand — kept
	// whether the suite passed or not.
	for i, machine := range []*Machine{&res.Db, &res.Runner} {
		name := fmt.Sprintf("infra-junit-%s-%s", agents[i].AgentId(), ctx.RunId())
		junit := pipeline.NewArtifact(ctx, name, artifact.FromAgentFile(agents[i], workDir+"/out/junit.xml"))
		machine.Infra.JUnitDigest = junit.Ready(ctx).Blob.Digest
		pipeline.ToStand(ctx, junit)
	}
	// No load test on infrastructure that failed its own checks. Decided by
	// the exit code: the plan's recording pass walks this code with zero
	// values, and zero must mean "go on".
	for i, machine := range []Machine{res.Db, res.Runner} {
		if at(reports, i).ExitCode != 0 {
			return fmt.Errorf("infrastructure suite failed on %s: %s", agents[i].AgentId(), machine.Infra.Summary)
		}
	}
	return nil
}

// Step 5. runBench runs the workload on the runner — the same Job verb, a
// different image — publishes its log as an artifact and its numbers as
// metrics, and on success hands postgres to the stand.
func runBench(ctx pipeline.Context, p Params, runner pipeline.AgentHandle, pg pipeline.Resource[dockerlib.Info], res *Result) error {
	req := stroppy.Request{
		RunId:    string(ctx.RunId()),
		URL:      fmt.Sprintf("postgresql://postgres:%s@%s/postgres?sslmode=disable", pgPassword, net.JoinHostPort(p.DbAddr, pgPort)),
		VUs:      p.VUs,
		Duration: p.Duration,
	}
	config := filelib.File(ctx, runner, workDir+"/stroppy-config.json", file.FromBytes(stroppy.Config(req)))
	if _, err := config.TryReady(ctx); err != nil {
		return err
	}
	// One-shot: a second execution would load the data twice. AtMostOnce
	// surfaces an undeterminable outcome as an error, never as a re-run.
	job, err := pipelineactivity.Activity(ctx, runner, dockerlib.Job(dockerlib.JobSpec{
		Name:   "bench",
		Config: &container.Config{Image: stroppy.Image, Cmd: stroppy.Args("/work/stroppy-config.json")},
		Host: &container.HostConfig{
			NetworkMode: "host",
			Mounts:      []mount.Mount{{Type: mount.TypeBind, Source: workDir, Target: "/work", ReadOnly: true}},
		},
	}),
		pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce),
		pipelineactivity.WithTimeout(p.Duration*2+10*time.Minute),
		pipelineactivity.WithHeartbeat(2*time.Minute),
	)
	if err != nil {
		return err
	}
	res.Bench = stroppy.Parse(job.Tail, p.Duration)

	// The full log is an ARTIFACT: the file on the runner goes to the blob
	// store, the record outlives the run on the stand. Named by run — an
	// artifact is one immutable blob; a second run is a second record.
	logArtifact := pipeline.NewArtifact(ctx, "stroppy-log-"+string(ctx.RunId()), artifact.FromAgentFile(runner, job.LogPath))
	res.LogDigest = logArtifact.Ready(ctx).Blob.Digest
	pipeline.ToStand(ctx, logArtifact)

	// A failed bench tears everything down — the log is enough for the
	// post-mortem, the machines are not kept busy for nothing.
	if job.ExitCode != 0 {
		return fmt.Errorf("stroppy exit %d: %s", job.ExitCode, stroppy.ErrorLine(job.Tail))
	}
	if job.Tail != "" && res.Bench.Summary == "" {
		return errors.New("stroppy exited 0 without a bench summary")
	}
	// The numbers become METRICS of the run: a ten-line activity of our
	// own, on the machine that measured them.
	if _, err := pipelineactivity.Activity(ctx, runner, pipelineactivity.Fn("publish-metrics", stroppy.Publish, res.Bench)); err != nil {
		return err
	}
	// Long life is a TRANSFER: postgres (and with it its machine's agent —
	// the root of that subtree) goes to the pipeline's stand; KeepFor
	// bounds the stay. Without Keep everything dies with the run.
	if p.Keep > 0 {
		pipeline.ToStand(ctx, pg, pipeline.KeepFor(p.Keep))
	}
	return nil
}

// at is the i-th result of an ActivityAll, or the zero value: the plan's
// recording pass returns no results and must still walk to the end.
func at[T any](results []T, i int) T {
	var zero T
	if i < len(results) {
		return results[i]
	}
	return zero
}

func orDefault[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}
