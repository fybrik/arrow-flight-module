REPOSITORY ?= afm/afm
TAG ?= latest

IMG = ${REPOSITORY}:${TAG}
CHART_IMG = ${HELM_REPOSITORY}:${TAG}

export HELM_EXPERIMENTAL_OCI=1

all: build push

.PHONY: build
build:
	pipenv lock -r > requirements.txt
	docker build -f build/Dockerfile . -t ${IMG}
	rm requirements.txt

.PHONY: push-to-kind
push-to-kind:
	kind load docker-image ${IMG}

.PHONY: push
push:
	docker push ${IMG}

.PHONY: deploy
deploy:
	helm install --set image.repository=${HELM_REPOSITORY} --set image.tag=${TAG} afm ./helm/afm

.PHONY: chart-push
chart-push:
	helm chart save ./helm/afm ${CHART_IMG}
	helm chart push ${CHART_IMG}
	helm chart remove ${CHART_IMG}

.PHONY: clean
clean:
	helm uninstall afm || true

helm-verify:
	helm lint helm/afm
	helm install --generate-name --dry-run -f helm/afm/values.yaml.sample  helm/afm
	
.PHONY: clean
test:
	pipenv run python -m unittest discover
