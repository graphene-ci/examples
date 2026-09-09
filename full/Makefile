.DEFAULT_GOAL := help

.PHONY: configure test lint
configure: ## Prepare the local SDK/library workspace and pinned tools
	$(MAKE) -C .. configure

test: ## Run local workflow tests
	$(MAKE) -C .. test

lint: ## Lint the example
	$(MAKE) -C .. lint

IMAGE := perf-nightly:latest

.PHONY: tidy
tidy: ## Resolve module dependencies
	go mod tidy

.PHONY: build
build: ## Build the pipeline binary
	go build -o bin/perf-nightly .

.PHONY: image
image: ## Build the worker image
	docker build -t $(IMAGE) .

.PHONY: push
push: image ## Push the worker image to the registry
	docker push $(IMAGE)

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'

.PHONY: cluster-up
cluster-up: ## Bring up the example's cluster (kind + crossplane + provider-yc)
	./cluster/cluster.sh up

.PHONY: cluster-down
cluster-down: ## Delete the example's kind cluster
	./cluster/cluster.sh down

.PHONY: cluster-status
cluster-status: ## Show the cluster's state
	./cluster/cluster.sh status
