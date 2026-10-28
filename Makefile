REGISTRY        ?= ghcr.io/triageagent-dev/xray
BUILD_DATE ?= $(shell date -u '+%Y-%m-%d')
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

LAST_TAG ?= $(shell git describe --tags --abbrev=0 2>/dev/null)
COMMITS_SINCE_TAG ?= $(shell git rev-list $(LAST_TAG)..HEAD --count 2>/dev/null || echo "0")
BASE_VERSION ?= $(shell if [ -n "$(LAST_TAG)" ]; then echo $(LAST_TAG) | sed 's/^v//'; else echo "0.0.0"; fi)

ifeq ($(shell git describe --exact-match --tags 2>/dev/null),)
NEXT_VERSION := $(shell echo $(BASE_VERSION) | awk -F. '{print $$1"."$$2"."$(COMMITS_SINCE_TAG)}')
VERSION ?= v$(NEXT_VERSION)-$(GIT_COMMIT)
else
VERSION ?= $(shell echo $(LAST_TAG) | sed 's/^v//')
endif

# Strict-SemVer chart version (Helm `version:` must be MAJOR.MINOR.PATCH, no `v`,
# no commit suffix). Derived from VERSION by stripping a leading `v` and any
# `-<commit>` build suffix, so the chart version auto-increments in lock-step
# with each release (was frozen at 0.1.0, which masked breaking chart changes).
CHART_VERSION ?= $(shell echo $(VERSION) | sed 's/^v//' | sed 's/-.*//')

LDFLAGS := \
	-s -w \
	-X 'github.com/triageagent-dev/x-ray/v2/internal/version.Version=$(VERSION)' \
	-X 'github.com/triageagent-dev/x-ray/v2/internal/version.GitCommit=$(GIT_COMMIT)' \
	-X 'github.com/triageagent-dev/x-ray/v2/internal/version.BuildDate=$(BUILD_DATE)'

LOCALARCH ?= $(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')
LOCALOS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')

.PHONY: help ui ui-dev ui-test build test lint fmt push image clean version generate sync-crds setup-envtest devenv run-dev run-dev-vertex graph-build graph-serve graph-test graph-install keygen maps-image spec-map

ENVTEST_K8S_VERSION ?= 1.34.0
OLLAMA_ENDPOINT     ?= http://localhost:8090
EMBED_MODEL         ?= nomic
EMBED_DIMS          ?= 256

# graph: the code-graph mode of the SAME binary (`xray graph ...`).
# One repo, one source, two builds - the split is a build tag, not a codebase.
#
#   `build`        CGO_ENABLED=0 -> the cluster image (ko, chainguard/static).
#                  Carries the memory server and `xray graph serve-snapshots|
#                  servicemap|search|impact|services|update|status|build|
#                  install|keygen|dump <snapshot>`. No Tree-sitter, no parsing.
#   `graph-build`  CGO_ENABLED=1 -> the local/CI build. Adds the parse modes
#                  `xray graph serve|snapshot|dump <dir>`; needs a C toolchain.
GRAPH_BIN       ?= bin/xray-parser
GRAPH_ADDR      ?= :8084
GRAPH_REPOS     ?= xray=$(CURDIR)
SNAPSHOT_DIR    ?= $(HOME)/.xray-maps
MAPS_IMAGE_OUT  ?= bin/xray-maps.oci.tar
MAPS_IMAGE_REF  ?= xray-maps:local
SPEC_MAP_REF    ?= ghcr.io/triageagent-dev/corpus-maps:xray-spec-local
# k8s Secret name for the MCP bearer token; `make devenv` creates it (token: dev-admin-token-xray).
MCP_TOKEN_SECRET    ?= xray-mcp-token

##@ General

help: ## Display this help
	@echo "X-Ray Server"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 } /^##@/ { printf "\n%s\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""
	@echo "Current version: $(VERSION)"

version: ## Show version info
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(GIT_COMMIT)"
	@echo "Build Date: $(BUILD_DATE)"

##@ Development

generate: ## Generate CRD manifests and deepcopy, then apply to cluster
	@echo "Generating CRD manifests..."
	@command -v controller-gen >/dev/null 2>&1 || go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
	@controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./api/v1"
	@controller-gen crd:allowDangerousTypes=true paths="./api/v1" output:crd:artifacts:config=config/crd
	@$(MAKE) sync-crds
	@echo "✓ Generation complete"

sync-crds: ## Apply CRDs from config/crd/ to the current cluster
	@echo "Applying CRDs to cluster..."
	@kubectl apply -f config/crd/
	@echo "✓ CRDs applied"

# The web UI (ui/) is a Next.js static export that //go:embed compiles into the
# binary. `make build` builds it first; UI=0 skips that for a binary that serves
# the API alone and needs no Node.
UI      ?= 1
UI_DEP  := $(if $(filter 1,$(UI)),ui,)
WEB_DIR := internal/webui/web

# BASE_PATH=/path builds the UI for a server whose --public-url ends in that path:
# the export and the API it calls both live under it. The export is bound to it.
BASE_PATH ?=
UI_ENV    := $(if $(BASE_PATH),NEXT_BASE_PATH=$(BASE_PATH) NEXT_PUBLIC_API_URL=$(BASE_PATH),)

ui: ## Build the web UI (ui/) and stage it for embedding in the binary (BASE_PATH=/x for a server under /x)
	@command -v npm >/dev/null 2>&1 || { echo "npm not found: install Node 20+, or build without the UI: make build UI=0"; exit 1; }
	@cd ui && npm ci --no-audit --no-fund && $(UI_ENV) NEXT_PUBLIC_UI_VERSION=$(VERSION) npm run build
	@mkdir -p $(WEB_DIR)
	@find $(WEB_DIR) -mindepth 1 ! -name .gitkeep -delete
	@cp -R ui/out/. $(WEB_DIR)/
	@echo "✓ staged $$(find $(WEB_DIR) -type f ! -name .gitkeep | wc -l | tr -d ' ') UI files into $(WEB_DIR)"

ui-dev: ## UI hot reload on :3000, proxying /v1 to a running server: make ui-dev API_URL=http://localhost:8080 (no voice, no live events)
	@cd ui && NEXT_PUBLIC_API_URL=$(or $(API_URL),http://localhost:8080) npm run dev

ui-test: ## Type-check, lint and test the web UI
	@cd ui && npm ci --no-audit --no-fund && npx tsc --noEmit && npm run lint && npm test

build: $(UI_DEP) ## Build the xray binary with the web UI embedded (UI=0: API only)
	@echo "Building xray binary..."
	@CGO_ENABLED=0 GOOS=$(LOCALOS) GOARCH=$(LOCALARCH) go build \
		-ldflags "$(LDFLAGS)" \
		-o bin/xray \
		./cmd/xray
	@echo "✓ Binary: bin/xray"

run: $(UI_DEP) ## Run the server locally with the web UI on :8080, sign-in off (--dev)
	@go run -ldflags "$(LDFLAGS)" ./cmd/xray --dev

setup-envtest: ## Install envtest binaries (kube-apiserver + etcd) for devenv
	@command -v setup-envtest >/dev/null 2>&1 || go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
	@echo "Fetching envtest assets for k8s $(ENVTEST_K8S_VERSION)..."
	@setup-envtest use $(ENVTEST_K8S_VERSION) --bin-dir bin/envtest -p path >/dev/null
	@echo "✓ envtest assets ready"

devenv: setup-envtest ## Boot a local envtest control plane + MemoryCell CRD (blocks; Ctrl-C to tear down)
	@KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) --bin-dir bin/envtest -p path)" \
		go run ./hack/devenv

run-dev: ## Run xray against devenv with local Ollama embeddings (run `make devenv` in another terminal first)
	@test -f .devenv/kubeconfig || { echo "✗ .devenv/kubeconfig not found — run 'make devenv' in another terminal first"; exit 1; }
	@KUBECONFIG=.devenv/kubeconfig go run -ldflags "$(LDFLAGS)" ./cmd/xray \
		--dev --namespace xray --mcp-address :8083 --mcp-oauth=false \
		--mcp-token-secret $(MCP_TOKEN_SECRET) \
		--embedding-endpoint=$(OLLAMA_ENDPOINT) \
		--embedding-model=$(EMBED_MODEL) \
		--embedding-dims=$(EMBED_DIMS) \
		--log-level=debug

GCP_PROJECT  ?= $(shell gcloud config get-value project 2>/dev/null)
GCP_LOCATION ?= europe-west4
VERTEX_MODEL ?= gemini-embedding-001

run-dev-vertex: ## Run xray against devenv with Vertex AI embeddings (needs ADC; run `make devenv` first)
	@test -f .devenv/kubeconfig || { echo "✗ .devenv/kubeconfig not found — run 'make devenv' in another terminal first"; exit 1; }
	@KUBECONFIG=.devenv/kubeconfig go run -ldflags "$(LDFLAGS)" ./cmd/xray \
		--dev --namespace xray --mcp-address :8083 --mcp-oauth=false \
		--mcp-token-secret $(MCP_TOKEN_SECRET) \
		--gcp-project=$(GCP_PROJECT) \
		--gcp-location=$(GCP_LOCATION) \
		--embedding-model=$(VERTEX_MODEL) \
		--embedding-dims=$(EMBED_DIMS) \
		--log-level=debug

##@ Quality

lint: ## Run gofmt + go vet
	@gofmt_out=$$(gofmt -l $$(git ls-files "*.go")); \
	if [ -n "$$gofmt_out" ]; then echo "Not formatted:\n$$gofmt_out"; exit 1; fi
	@echo "✓ gofmt"
	@go vet ./...
	@echo "✓ go vet"

fmt: ## Format Go code
	@gofmt -w $$(git ls-files "*.go")

test: ## Run Go tests
	@go test ./...

##@ Container Images

image: $(UI_DEP) ## Build container image locally (ko.local), web UI included
	@VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
		KO_DOCKER_REPO=ko.local ko build \
		--tags=$(VERSION),latest \
		--bare \
		--local \
		./cmd/xray
	@echo "✓ Image built locally"

push: $(UI_DEP) ## Build and push xray image, web UI included
	@echo "Building and pushing xray image $(VERSION) (chart $(CHART_VERSION))..."
	@sed -i.bak 's/^version:.*/version: $(CHART_VERSION)/' charts/xray/Chart.yaml && rm -f charts/xray/Chart.yaml.bak
	@sed -i.bak 's/^appVersion:.*/appVersion: "$(VERSION)"/' charts/xray/Chart.yaml && rm -f charts/xray/Chart.yaml.bak
	@sed -i.bak 's/^  tag:.*/  tag: "$(VERSION)"/' charts/xray/values.yaml && rm -f charts/xray/values.yaml.bak
	@VERSION=$(VERSION) GIT_COMMIT=$(GIT_COMMIT) BUILD_DATE=$(BUILD_DATE) \
		KO_DOCKER_REPO=$(REGISTRY) ko build \
		--tags=$(VERSION),latest \
		--platform=linux/amd64,linux/arm64 \
		--bare \
		--image-label org.opencontainers.image.version=$(VERSION) \
		--image-label org.opencontainers.image.revision=$(GIT_COMMIT) \
		./cmd/xray
	@echo "✓ Pushed: $(REGISTRY):$(VERSION)"
	@echo "✓ Pushed: $(REGISTRY):latest"

##@ Cleanup

##@ graph (code graph)

graph-build: ## Build the CGO binary — server + ALL graph modes incl. the parser
	@echo "Building $(GRAPH_BIN) (CGO_ENABLED=1) from ./cmd/xray..."
	@CGO_ENABLED=1 go build -o $(GRAPH_BIN) ./cmd/xray
	@echo "✓ Built $(GRAPH_BIN) (adds: graph serve|snapshot|dump)"

graph-serve: graph-build ## Build then serve the local graph (needs an embeddings endpoint at $(OLLAMA_ENDPOINT))
	@echo "Serving graph on $(GRAPH_ADDR) for repos: $(GRAPH_REPOS)"
	@$(GRAPH_BIN) graph serve \
		-repo "$(GRAPH_REPOS)" \
		-addr "$(GRAPH_ADDR)" \
		-embedding-endpoint=$(OLLAMA_ENDPOINT) \
		-embedding-model=$(EMBED_MODEL) \
		-embedding-dims=$(EMBED_DIMS)

graph-test: ## Run the graph substrate tests under CGO (exercises the parser)
	@CGO_ENABLED=1 go test ./internal/codegraph/... ./cmd/xray/...

graph-install: graph-build ## Install embedded graph skills + hook into this repo's .claude/
	@$(GRAPH_BIN) graph install -dir $(CURDIR)

keygen: ## Generate an age identity for encrypted snapshots: make keygen SNAPSHOT_KEY=<file> (prints the recipient)
	@test -n "$(SNAPSHOT_KEY)" || { echo "SNAPSHOT_KEY=<path to write the identity> is required" >&2; exit 2; }
	@go run ./cmd/xray graph keygen -out "$(SNAPSHOT_KEY)"

maps-image: ## Pack snapshots into a local OCI image tarball: make maps-image MAPS="team skills" (never pushes)
	@sh hack/maps-oci.sh "$(SNAPSHOT_DIR)" "$(MAPS_IMAGE_OUT)" "$(MAPS_IMAGE_REF)" $(MAPS)

spec-map: graph-build ## Build the spec as a graph map and an OCI image tarball in bin/spec-map (never pushes)
	@sh hack/spec-map.sh $(GRAPH_BIN) bin/spec-map $(SPEC_MAP_REF)

##@ Cleanup

clean: ## Remove build artifacts
	@rm -rf bin/ ui/out ui/.next
	@find $(WEB_DIR) -mindepth 1 ! -name .gitkeep -delete 2>/dev/null || true
	@go clean
	@echo "✓ Cleaned"

upsert-memory: ## Upsert a memory item: make upsert-memory ARGS="--title Foo --kind stable_fact --content Bar"
	@python3 scripts/upsert_memory.py $(ARGS)
