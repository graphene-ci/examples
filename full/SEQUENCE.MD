# examples/full (perf-nightly): полная последовательность вызовов

Рабочий файл: как ЭТОТ пример исполняется от `graphene run start` до
уборки. Термины и сетевая связанность — ниже, затем один сплошной
timeline без искусственной сегментации.

## Терминология

| Термин | Что это |
|---|---|
| **API Gateway** | Единственный вход graphene-server (`:7233`): за ним management API, worker API, agent API, Temporal proxy, registry proxy. Никто, кроме сервера, Temporal не видит. |
| **user container** | ОДИН образ/бинарь пользователя (этот `main.go`). Работает в двух ролях: **orchestrator** — крутит workflow прогона и библиотечные activities (k8s-ресурсы: kubeconfig не покидает его контур); **executor** — исполняет machine-activities на машине агента. |
| **agent** | Наш процесс на машине + её запись (entity). Исходящее соединение к Gateway, сеть не слушает. |
| **machine** | Само железо/VM. Агент = полный доступ уровня пользователя машины; контейнер executor'а — упаковка, не изоляция (`/host`, workspace same-path, `DOCKER_HOST`). |
| **server worker** | Temporal worker внутри graphene-server: системные activities (`server.agent.*`, `server.container.*`, `server.artifact.*`) и entity-записи agent/artifact. |

## Поверхности API за Gateway

Один порт `:7233` (cmux делит по содержимому запроса). TLS — прокси
перед Gateway (caddy: `reverse_proxy h2c://host:7233`).

| API | Протокол | Кто зовёт | Auth | Где определено |
|---|---|---|---|---|
| **management** — RunsAPI, ResourcesAPI, NamespacesAPI, SecretsAPI | ConnectRPC (connect-JSON / gRPC-Web / gRPC) | CLI, UI, graphene-ci-cloud | admin/run token `[@ns]` | `graphene/proto/management/v1` |
| **worker API** — SecretsAPI, CapabilitiesAPI, BlobsAPI (стриминг) | gRPC | user container (обе роли, из тел activities) | run token | `pipeline/proto/workerplane/v1` |
| **agent API** — AgentAPI (bidi-стрим Session) | gRPC | graphene-agent (только исходящее) | agent token (`agentId:token`) | `agent/proto/agent/v1` |
| **Temporal proxy** — WorkflowService | gRPC (unknown-service handler) | user container (SDK обеих ролей) | run token | `graphene/internal/temporalproxy` |
| **registry proxy** — `/v2/*` | HTTP | агент (pull), docker push | agent/run/admin token | `graphene/internal/httpapi` |
| **probes** — `/healthz`, `/healthz/*`, grpc.health.v1 | HTTP + gRPC | балансировщики, kubelet | без токена | `httpapi` + `probes` |

CLI пользователя работает ТОЛЬКО с management API (connect-go клиент).
Остальное — внутренние контуры системы.

## Стрелки

- `-)` — установление ПОСТОЯННОГО сетевого соединения (bidi-стрим, dial+poll);
- `->>` — вызов (RPC / activity / команда);
- `-->>` — ответ / результат.

## Общая картина: main.go (perf-nightly) сверху вниз

Ход именно этого файла, строка за строкой, в общих словах — механика
каждого шага в детальных диаграммах Д1–Д6.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    actor U as CLI
    participant G as graphene-server
    participant O as orchestrator:<br/>Main(ctx, params)
    participant E as entity workflows<br/>(по одному на ресурс)
    participant M as машины<br/>(агенты + executors)

    U->>G: run start perf-nightly
    G->>O: запуск user container, Main пошёл (Д1)

    Note over O: k8sClient := NewClientFromSecret("kubeconfig")<br/>— только ССЫЛКА, значение позже

    O->>E: net := k8slib.Resource(Network) — выделен под свой workflow (Д2)
    O->>E: sub := k8slib.Resource(Subnet, Parent(net))<br/>— NetworkIDRef резолвит кластер
    O->>E: vmAgent := NewAgent("edge-1") — запись агента, ждёт connect (Д3)
    O->>M: bareAgent := NewAgentViaSSH("bare-1") — ssh-install агента (Д3)
    O->>E: vm := k8slib.Resource(Instance,<br/>user-data=vmAgent.CloudInit(), Children(vmAgent))
    E->>M: облако подняло vm-1 → cloud-init поднял агента edge-1 (Д3)

    Note over E: все entity крутят СВОИ циклы параллельно<br/>всему, что ниже: converge · reconcile · drift-heal (Д2)

    O->>O: sub.Ready(ctx) · vmId := vm.Ready(ctx).Status…ID<br/>— ожидание, типизированный статус

    O->>M: report := Activity(bareAgent, "run-work", AtMostOnce)<br/>— первое касание поднимает executor (Д4), работа на машине (Д5)

    O->>G: edges := SelectAgents(role=edge) → [edge-1, bare-1]
    O->>M: ActivityAll(edges, dockerlib.Install())<br/>— docker на обе машины + capability на записи (Д5)

    O->>G: baseline := AttachArtifact("baseline-report")<br/>— ЧУЖАЯ запись: узнать, не создать
    O->>E: dockerContainer := dockerlib.Container(vmAgent, nginx)<br/>— ресурс дерева, Parent(vmAgent) (Д2)
    O->>M: reportArtifact := NewArtifact(FromAgentFile(bareAgent, …))<br/>— файл машины → blob store → entity-запись (Д5)

    O->>E: ToStand(vm, KeepFor(keep)) · ToStand(reportArtifact)<br/>— передача владения стенду (Д6)

    O->>O: Result{report, version, containerId, vmId, baselineDigest}
    O->>G: return Result — прогон Completed
    U->>G: run result → Result

    Note over G,M: прогон мёртв — entity живут в стенде (Д2)
    G->>E: TTL истёк → каскад: vm-1 → sub → net + дети (Д6)
    destroy E
    E-xG: finalize → записей больше нет
    destroy M
    G-xM: StopContainer: снос executors + workspace (Д6)
    destroy O
    G-xO: reaper: снос orchestrator-контейнера (Д6)
```

## Д1. Старт прогона (managed)

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    actor U as CLI
    participant G as graphene-server
    participant T as Temporal

    U->>+G: management: RunsAPI.StartRun<br/>{runId, pipeline, params, image, labels}
    G->>G: namespace из токена, ValidateUserLabels
    G->>T: ExecuteWorkflow id=run/{id}, queue=run/{id},<br/>SA EntityLabels, ConflictPolicy=USE_EXISTING
    create participant O as orchestrator (user container)
    G->>O: docker run через сокет СВОЕГО хоста:<br/>graphene-run-{ns}-{runId}<br/>env: адрес сервера, namespace, run token
    G-->>-U: {workflowId, temporalRunId}
    Note over O: inplace-вариант: тот же бинарь запускает<br/>пользователь где угодно — дальше без отличий
    O-)G: постоянное соединение (run token)
    Note over O,G: SDK думает, что говорит с Temporal —<br/>это Temporal proxy сервера
    O->>T: poll очереди run/{id} (через proxy)
    T-->>O: workflow task → Main(ctx, params)
```

## Д2. Выделение ресурса и жизнь его entity workflow

Объявление ресурса в user workflow не создаёт его «в себе»: ресурс
выделяется в ОТДЕЛЬНЫЙ top-level workflow (по одному на ресурс),
живущий своим циклом дольше прогона (child умер бы вместе с ним).
Принадлежность — ownership tree в search attributes. Здесь k8s-ресурс;
agent/artifact-записи — те же entity, только их activity-тела крутит
server worker.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    participant O as user workflow run/{id}
    participant T as Temporal
    participant A as activity-тела k8s<br/>(в orchestrator)
    participant G as graphene-server
    participant K as кластер (crossplane)

    Note over O: k8slib.Resource(&vpc.Network{...})
    create participant E as entity workflow vpc.network/net
    O->>E: ExecuteWorkflow id="vpc.network/net"<br/>ConflictPolicy=USE_EXISTING (через declare-activity)
    activate E
    E->>E: SetQueryHandler(describe) · SetUpdateHandler(команды,<br/>entity-set-labels) · GetSignalChannel(entity-delete)
    E->>E: phase=creating · UpsertSearchAttributes<br/>(Kind, Phase, Owner, Labels)

    E->>+A: ExecuteActivity k8s.apply
    A->>G: worker API: GetSecret(kubeconfig) —<br/>значение ТОЛЬКО здесь
    A->>K: server-side apply манифеста
    A-->>-E: applied
    loop convergence (pollInterval, до timeout)
        E->>+A: ExecuteActivity k8s.observe
        A->>K: get live object
        A-->>-E: ready? (WithReady / Ready-condition)
    end
    E->>E: phase=ready · UpsertSearchAttributes
    loop declare-activity ждёт
        A->>E: QueryWorkflow(describe) → phase
    end
    A-->>O: Ready → live-объект (futures)

    loop main loop (serialized, до смерти)
        alt Update-команда (transfer-owner, set-labels, свои)
            T->>E: validator → очередь → exec (dedup RequestID)
        else reconcile-тик (AwaitWithTimeout истёк)
            E->>+A: ExecuteActivity k8s.observe
            A->>K: get live
            A-->>-E: live
            opt drift (WithDrifted) или объект пропал
                E->>+A: ExecuteActivity k8s.apply (re-apply)
                A-->>-E: healed
            end
        else ContinueAsNewSuggested
            E->>T: ContinueAsNew(envelope: spec, state, labels,<br/>pending, dedup-кэш) — тот же workflow id
        end
    end

    T->>E: SignalWorkflow(entity-delete)
    E->>E: phase=deleting · отклонить pending ·<br/>Await(AllHandlersFinished)
    E->>+A: ExecuteActivity k8s.delete (WithFinalize)
    A->>K: delete объекта
    loop finalize ждёт исчезновения
        A->>K: get → NotFound?
    end
    A-->>-E: gone
    E->>E: phase=deleted · UpsertSearchAttributes
    deactivate E
    destroy E
    E-xT: workflow Completed (describe отвечает и после)
```

## Д3. Подключение машины: два пути агента

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    participant O as user workflow run/{id}
    participant G as graphene-server<br/>(server worker)
    participant SE as entity workflows<br/>agent/bare-1 · agent/edge-1
    participant K as кластер (crossplane)

    Note over O,G: путь 1: машина уже ЕСТЬ — ssh-install
    O->>+G: activity server.agent.declare bare-1 + SSHInstall
    G->>SE: ExecuteWorkflow agent/bare-1<br/>(labels + graphene.io/run)
    create participant BA as agent bare-1
    G->>BA: ssh root@host (ключ-секрет резолвится НА СЕРВЕРЕ,<br/>host key обязателен) → install-скрипт
    BA-)G: AgentAPI.Session — постоянный bidi-стрим,<br/>ИСХОДЯЩИЙ (hello · heartbeat · machine facts)
    G->>SE: connected на записи agent/bare-1
    G-->>-O: AgentState (ready + Needs satisfied)

    Note over O,K: путь 2: машину СОЗДАЁТ прогон — identity в user-data
    O->>+G: activity server.agent.declare edge-1
    G->>SE: ExecuteWorkflow agent/edge-1 — запись ЖДЁТ connect
    Note over O,K: vm-1 (k8s-ресурс, Д2) применён с<br/>user-data = CloudInit(agent edge-1)
    create participant EA as agent edge-1
    K->>EA: облако подняло vm → cloud-init поставил агента
    EA-)G: AgentAPI.Session
    G->>SE: connected на записи agent/edge-1
    G-->>-O: AgentState
```

## Д4. Первое касание машины: executor

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    participant O as user workflow run/{id}
    participant T as Temporal
    participant G as graphene-server
    participant BA as agent bare-1

    O->>+G: activity server.container.ensure<br/>(первая Activity на этом агенте)
    G->>BA: EnsureContainer{image, env} по Session-стриму
    BA->>G: pull образа через registry proxy /v2
    create participant BX as executor@bare-1<br/>(user container, role=executor)
    BA->>BX: runc run: rootfs образа · / → /host ·<br/>workspace same-path · env: MACHINE_ROOT,<br/>WORKSPACE, DOCKER_HOST, адрес сервера, run token
    BX-)G: постоянное соединение → poll agent/bare-1/run/{id}
    loop «ensured = executor поллит»
        G->>T: DescribeTaskQueue: есть ACTIVITY poller?
    end
    G-->>-O: ensured — можно диспатчить
    O->>+BX: activity (например run-work, AtMostOnce)
    BX-->>-O: результат
```

## Д5. Machine-activities: работа, установка, сборка, артефакт

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    participant O as user workflow run/{id}
    participant G as graphene-server
    participant SE as entity workflows<br/>agent/… · artifact/…
    participant X as executor (любой)
    participant MA as машина<br/>(FS · git · docker daemon)

    Note over O,G: выборка «всех помеченных» — снапшот, без владения
    O->>+G: activity server.agent.select {role=edge}
    G-->>-O: [edge-1, bare-1] (visibility по записям)

    par dockerlib.Install() на каждом (ActivityAll)
        O->>+X: task docker.install
        X->>MA: get.docker.com НА МАШИНЕ (machine.Shell, chroot /host)
        X->>G: worker API: PublishCapability(docker)
        G->>SE: UpdateWorkflow на записи агента
        X-->>-O: InstallReport{version}
    and то же на втором агенте
        O->>X: …
    end

    Note over O,MA: сборка (dockerlib.Build) — buildx машины
    O->>+X: task docker.build {Context: workspace-путь}
    X->>MA: docker buildx build — daemon видит<br/>ТОТ ЖЕ путь (same-path workspace)
    MA-->>X: digest через --metadata-file
    X-->>-O: BuildReport{Digest}

    Note over O,MA: артефакт из файла машины
    O->>+X: task artifact upload
    X->>MA: чтение файла через /host
    X->>G: worker API: BlobsAPI.PutBlob (stream чанков)
    G->>G: sha256 на СЕРВЕРЕ → blob store (s3)
    X-->>-O: BlobRef{digest, location}
    O->>G: activity server.artifact.declare
    G->>SE: ExecuteWorkflow artifact/perf-report (entity-запись)
```

## Д6. Завершение, стенд, уборка

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': {'noteBkgColor': '#3a3f4b', 'noteTextColor': '#e8eaf0', 'noteBorderColor': '#5a6070'}}}%%
sequenceDiagram
    autonumber
    actor U as CLI
    participant G as graphene-server<br/>(sweeper · reaper)
    participant T as Temporal
    participant O as orchestrator
    participant E as entity workflows ресурсов
    participant A as агенты машин

    Note over O,E: долгая жизнь = ПЕРЕДАЧА, не sleep
    O->>E: UpdateWorkflow transfer-owner{stand/perf-nightly, keep}<br/>на vm-поддерево · артефакт → stand без TTL
    E->>E: owner=stand/… · EntityKeepUntil=now+keep<br/>(search attributes)
    O->>T: return Result → run/{id} Completed
    U->>+G: RunsAPI.RunResult
    G-->>-U: Result (typed JSON)

    Note over G,E: entity живут в стенде, реконсилят

    loop sweeper-тик
        G->>T: ListWorkflow: EntityKeepUntil <= now
        G->>T: каскад по EntityOwner, deepest-first
    end
    G->>E: SignalWorkflow(entity-delete): vm-1 → sub → net<br/>(+ дети: docker/hello, agent/edge-1 через Children)
    destroy E
    E-xT: finalize → phase=deleted → Completed

    Note over G,A: очередь run/{id} пуста — последний ресурс умер
    G->>A: StopContainer каждому агенту прогона
    A->>A: снос executor + workspace
    destroy O
    G-xO: reaper: run закрыт И очередь пуста →<br/>docker rm orchestrator-контейнера
```

## Дыры поверхности (для грумминга CLI)

Минимальный CLI = 1:1 management API: `run start|get|result|cancel|list`,
`res list|get|tree|delete|transfer|invoke`, `ns`, `secret` (+`-l k=v`).

1. **Нет стрима** — наблюдение за прогоном только поллингом
   GetRun/List; нужен RunsAPI.WatchRun / ResourcesAPI.Watch.
2. **Нет логов** — stdout executor'ов лежит у агента, orchestrator'а —
   в docker; наружу не отдаётся ничем.
3. **Нет событий прогона** — «что сейчас происходит» есть только в
   Temporal history, management её не проксирует.
4. **Нет манифеста пайплайна** ([?9]) — CLI нечего показать про
   pipeline до запуска (Params/Result/ресурсы).
