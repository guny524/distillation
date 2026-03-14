REPO_NAME = ghcr.io/guny524/distillation
COMPONENT = distillation
STATE = main
VERSION = v00.00.01
ARCH = amd64
CI_COMMIT_SHORT_SHA ?= $(shell git rev-parse --short=8 HEAD)
BUILD_DATE = $(shell date +"%y%m%d")

TAG = ${STATE}-${VERSION}-${ARCH}-${CI_COMMIT_SHORT_SHA}-${BUILD_DATE}
IMG = ${REPO_NAME}/${COMPONENT}:${TAG}

PYTHON ?= uv run python
SCRIPTS := $(wildcard scripts/*.py)
SCHEMAS := $(wildcard schemas/*.json)

.PHONY: lint lint-schema image-build image-push clean

lint:
	@for f in $(SCRIPTS); do \
		$(PYTHON) -m py_compile "$$f" && echo "OK $$f"; \
	done

lint-schema:
	@for f in $(SCHEMAS); do \
		$(PYTHON) -c "import json, sys; json.load(open('$$f'))" && echo "OK $$f"; \
	done

image-build:
	docker build . -t ${IMG} --platform linux/${ARCH}

image-push:
	docker push ${IMG}

clean:
	- rm -rf __pycache__ scripts/__pycache__ *.pyc
