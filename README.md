# examples

Buildable user pipelines for the current Graphene Go SDK. Each example is its
own Go module and, at the same time, an executable pipeline binary.

| Example | What it shows |
|---|---|
| `minimal/` | an existing ssh machine, agent install, an at-most-once activity, publishing an artifact and handing it to a stand |
| `full/` | typed params, cron/webhook, Crossplane resources, two agents, Docker, selections, artifacts, flows, telemetry and lifetime |
| `childcell/` | the smallest child pipeline — squares a number (used by `suite`) |
| `suite/` | a parent pipeline that fans out over cells of `childcell` with `pipeline.RunAll` (concurrency, typed results, failure isolation) |
| `echocell/` | a tiny standalone pipeline used to prove the source-first flow |

## Локальные тесты full

С опубликованными версиями SDK и библиотек достаточно этого репозитория:

```bash
cd full
GOWORK=off go test -race ./...
GOWORK=off go run . plan
```

Для совместной разработки разместите checkout `pipeline`, `library` и `examples` рядом. Корневой Makefile
проверяет `full` против текущего SDK и библиотек через игнорируемый `go.work`:

```bash
make configure
make test
make lint
cd full
go test -race ./...
```

Тесты исполняют `run` с fixtures двух агентов, Kubernetes/Crossplane, Docker и
артефактов; проверяют результат, очереди, дерево, cleanup и TTL при успехе,
ошибках работы/создания VM/установки Docker, отмене и отсутствии чужого артефакта. Живая инфраструктура не
используется. Остальные примеры сохраняют собственные Go-модули; корневой
`make test` покрывает только `full`.

Подробности: [локальные тесты](https://graphene-ci.github.io/docs/sdk/testing).

## Plan (local, no server)

```bash
cd full
go run . plan            # -o json for machine-readable, -o mermaid for a diagram
```

## Run against an installation

A real run needs a Graphene installation and the external systems the example's
params name. Push the pipeline and start a run with
[`graphenectl`](https://github.com/graphene-ci/graphene) (or the binary's own
`push` / `run`):

```bash
# source-first: build on the server, keep the source, one token
graphenectl revision materialize suite --upload .
graphenectl invoke pipeline suite activate --data '{"revisionId":"<rev>"}'
graphenectl run start suite --params '{"count":4,"concurrency":2}'
```

The step-by-step first run is in the
[docs](https://graphene-ci.github.io/docs/start/first-look).
