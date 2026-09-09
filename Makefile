.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin
export GOTOOLCHAIN := go1.26.5
export GOWORK := $(CURDIR)/go.work
GOLANGCI_LINT_VERSION := v2.12.2

.PHONY: configure test lint plan help
configure: go.work $(BIN)/golangci-lint ## Prepare full example with sibling SDK and library checkouts

go.work:
	GOWORK=off go work init ./full ../pipeline ../library/docker ../library/k8s

$(BIN)/golangci-lint:
	mkdir -p $(BIN)
	GOWORK=off GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

test: go.work ## Run full example's local workflow tests
	go test ./full/...

lint: configure ## Lint the full example
	$(BIN)/golangci-lint run ./full/...

plan: go.work ## Build the full example's local plan
	cd full && go run . plan

help: ## Show targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
