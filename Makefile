COMPOSE ?= podman-compose
GO ?= go

.PHONY: test check build image up down fixture integration benchmark rules
test:
	$(GO) test -race -count=1 -timeout=5m ./...
check:
	$(GO) vet ./...
	test -z "$$(gofmt -l .)"
build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -mod=readonly -trimpath -o bin/collector ./cmd/collector
	CGO_ENABLED=0 $(GO) build -mod=readonly -trimpath -o bin/fixture ./cmd/fixture
image:
	podman build -f Containerfile -t localhost/insights-otel-ingestion-sli:local .
up: image
	$(COMPOSE) up -d
down:
	$(COMPOSE) down
fixture:
	$(GO) run ./cmd/fixture -scenario happy
integration:
	python3 scripts/integration.py --compose "$(COMPOSE)"
benchmark:
	$(GO) test ./internal/store -run '^$$' -bench BenchmarkBatch -benchtime=10x -benchmem
rules:
	podman run --rm -v "$(CURDIR)/deploy/local:/work:ro,z" -w /work --entrypoint /bin/promtool docker.io/prom/prometheus:v3.5.0 test rules rules.test.yaml
