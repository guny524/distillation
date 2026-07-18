# Image name SSOT. IMG = ${REPO_NAME}/${COMPONENT}:${TAG} = ghcr.io/guny524/distillation:<tag>.
# The prod overlay's kustomization.yaml `images.newName` MUST equal ${REPO_NAME}/${COMPONENT}
# (ghcr.io/guny524/distillation); if the pulled name != this pushed name, pods ImagePullBackOff.
REPO_NAME = ghcr.io/guny524
COMPONENT = distillation
STATE = main
VERSION = v00.00.01
# Build for the host arch by default (arm64 on an Apple Silicon Mac, amd64 on an
# amd64 host/CI). The image is just the static distill binary now (opencode is
# fetched at runtime by the comprehend container, matching its own arch), so a
# cross-arch image is fine -- override with `make image-build ARCH=amd64`.
ARCH ?= $(shell go env GOARCH)
CI_COMMIT_SHORT_SHA ?= $(shell git rev-parse --short=8 HEAD)
BUILD_DATE = $(shell date +"%y%m%d")

TAG = ${STATE}-${VERSION}-${ARCH}-${CI_COMMIT_SHORT_SHA}-${BUILD_DATE}
IMG = ${REPO_NAME}/${COMPONENT}:${TAG}

.PHONY: lint test build image-build image-push clean sqlc

lint:
	go vet ./...
	@for f in schemas/*.json; do \
		jq . "$$f" > /dev/null && echo "OK $$f"; \
	done

sqlc:
	sqlc generate -f internal/db/sqlc.yaml

test: lint
	go test -race -v ./...

build: sqlc test
	CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} go build -ldflags="-s -w" -o distill ./cmd/distill

image-build: build
	docker build . -t ${IMG} --platform linux/${ARCH}

image-push:
	docker push ${IMG}

clean:
	rm -f distill
