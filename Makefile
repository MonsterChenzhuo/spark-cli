GO ?= go
BINARY := spark-cli
PKG := github.com/opay-bigdata/spark-cli
LDFLAGS := -s -w -X $(PKG)/cmd.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build install unit-test e2e test lint fmt tidy clean release-snapshot

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY) .

install:
	$(GO) install -ldflags "$(LDFLAGS)" .

unit-test:
	$(GO) test -race -count=1 ./...

e2e: build
	$(GO) test -race -count=1 -tags=e2e ./tests/e2e/...

test: unit-test e2e

lint:
	$(GO) vet ./...
	@gofmt -l . | tee /dev/stderr | (! read)
	@$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6 run

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy

clean:
	rm -f $(BINARY)
	rm -rf dist/

release-snapshot:
	$(GO) run github.com/goreleaser/goreleaser/v2@latest release --snapshot --clean
