# examples

User pipelines against the [pipeline](https://github.com/graphene-ci/pipeline)
library. Each example is its own Go module.

| Example | What it is |
|---|---|
| `full/` | The TARGET UX sketch — not compiling code; the experience the library converges to: main wrapper with typed params, Pulumi-style chained resources, provider libraries, an agent with cloud-init identity, libraries on top of the agent. Every gap between it and today's library is future surface work. |
| `minimal/` | The same story told with today's surface — compiles: typed params, a cloud machine owned by the run, one-shot `Action` on it, keep window, teardown by the run's end. |
