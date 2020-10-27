REPOSITORY ?= afm/afm
TAG ?= latest

IMG = ${REPOSITORY}:${TAG}
CHART_IMG = ${HELM_REPOSITORY}:${TAG}

export HELM_EXPERIMENTAL_OCI=1

all: test build

.PHONY: helm-verify
helm-verify:
	helm lint helm/afm
	helm install --generate-name --dry-run -f helm/afm/values.yaml.sample  helm/afm

.PHONY: test
test:
	pipenv run python -m unittest discover

.PHONY: build
build:
	pipenv lock -r > requirements.txt
	docker build -f build/Dockerfile . -t ${IMG}
	rm requirements.txt

.PHONY: push
push:
	docker push ${IMG}

.PHONY: push-to-kind
push-to-kind:
	kind load docker-image ${IMG}

.PHONY: chart-push
chart-push:
	helm chart save ./helm/afm ${CHART_IMG}
	helm chart push ${CHART_IMG}
	helm chart remove ${CHART_IMG}

.PHONY: deploy
deploy:
	helm install --set image.repository=${HELM_REPOSITORY} --set image.tag=${TAG} afm ./helm/afm

.PHONY: clean
clean:
	helm uninstall afm || true

