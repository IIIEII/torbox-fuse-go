.PHONY: build run test lint clean docker-build docker-up docker-down help

BINARY   := torbox-media-center
DOCKER   := docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the binary
	go build -o $(BINARY) ./cmd/torbox-media-center

run: build ## Build and run (requires TORBOX_API_KEY)
	./$(BINARY)

test: ## Run tests
	go test ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

clean: ## Remove built binary
	rm -f $(BINARY)

docker-build: ## Build Docker image
	$(DOCKER) build

docker-up: ## Start via Docker Compose
	$(DOCKER) up -d

docker-down: ## Stop Docker Compose
	$(DOCKER) down