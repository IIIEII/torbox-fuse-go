.PHONY: build run test test-short test-race vet lint tidy-check clean \
       docker-build docker-up docker-down help

BINARY   := torbox-media-center
DOCKER   := docker compose
GO       := go
CGO      := CGO_ENABLED=1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	$(CGO) $(GO) build -o $(BINARY) ./cmd/torbox-media-center

run: build ## Build and run (requires TORBOX_API_KEY)
	./$(BINARY)

# ---- test targets ----

test-short: ## Run unit tests only (skip e2e/stress tagged !short)
	$(CGO) $(GO) test -short -count=1 ./...

test: ## Run all tests including e2e/stress (needs FUSE + TORBOX_API_KEY)
	$(CGO) $(GO) test -count=1 ./...

test-race: ## Run unit tests with race detector
	$(CGO) $(GO) test -race -short -count=1 ./...

test-coverage: ## Run unit tests with coverage report
	$(CGO) $(GO) test -short -cover -coverprofile=coverage.out ./...
	@echo "Coverage summary:"
	$(GO) tool cover -func=coverage.out | tail -1

# ---- static analysis ----

vet: ## Run go vet
	$(CGO) $(GO) vet ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

tidy-check: ## Verify go.mod and go.sum are tidy
	$(CGO) $(GO) mod tidy
	git diff --exit-code go.mod go.sum

# ---- docker ----

docker-build: ## Build Docker image
	TORBOX_API_KEY=dummy $(DOCKER) build

docker-up: ## Start via Docker Compose
	$(DOCKER) up -d

docker-down: ## Stop Docker Compose
	$(DOCKER) down

# ---- cleanup ----

clean: ## Remove built binary and coverage
	rm -f $(BINARY) coverage.out