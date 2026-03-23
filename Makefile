REPO_NAME = ghcr.io/guny524/distillation
COMPONENT = distillation
STATE = main
VERSION = v00.00.01
ARCH = amd64
CI_COMMIT_SHORT_SHA ?= $(shell git rev-parse --short=8 HEAD)
BUILD_DATE = $(shell date +"%y%m%d")

TAG = ${STATE}-${VERSION}-${ARCH}-${CI_COMMIT_SHORT_SHA}-${BUILD_DATE}
IMG = ${REPO_NAME}/${COMPONENT}:${TAG}

SCHEMAS := $(wildcard schemas/*.json)

.PHONY: lint lint-schema test build image-build image-push clean

lint:
	go vet ./...

lint-schema:
	@for f in $(SCHEMAS); do \
		jq . "$$f" > /dev/null && echo "OK $$f"; \
	done

test: lint lint-schema
	go test -race -v ./...

build: test
	CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} go build -ldflags="-s -w" -o distill ./cmd/distill

image-build: build
	docker build . -t ${IMG} --platform linux/${ARCH}

image-push:
	docker push ${IMG}

clean:
	rm -f distill
