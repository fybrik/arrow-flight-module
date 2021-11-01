include Makefile.env

REPOSITORY ?= helm/afm
DOCKER_TAG ?= latest
HELM_TAG ?= 0.0.0

DOCKER_NAME ?= arrow-flight-module
CHART ?= arrow-flight-module-chart

TEMP := /tmp
HELM_CHART_PATH := oci://${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/
IMG := ${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}:${DOCKER_TAG}
IMG_REPOSITORY := ${DOCKER_HOSTNAME}/${DOCKER_NAMESPACE}/${DOCKER_NAME}

export HELM_EXPERIMENTAL_OCI=1

all: test build

.PHONY: helm-verify
helm-verify:
	helm lint helm/afm

.PHONY: test
test:
	pipenv run python -m unittest discover

.PHONY: build
build:
	pipenv lock -r > requirements.txt
	docker build -f build/Dockerfile . -t ${IMG}
	rm requirements.txt

.PHONY: docker-push
docker-push:
	docker push ${IMG}

.PHONY: push-to-kind
push-to-kind:
	kind load docker-image ${IMG}

.PHONY: chart-push
chart-push:
	helm package ${REPOSITORY} --destination=${TEMP} --version=${HELM_TAG}
	helm push ${TEMP}/${CHART}-${HELM_TAG}.tgz ${HELM_CHART_PATH}
	rm -rf ${TEMP}/${CHART}-${HELM_TAG}.tgz


.PHONY: deploy
deploy:
	helm install --set image.repository=${IMG_REPOSITORY} --set image.tag=${DOCKER_TAG} afm ./helm/afm

.PHONY: clean
clean:
	helm uninstall afm || true


include hack/make-rules/tools.mk

