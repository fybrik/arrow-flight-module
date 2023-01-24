#!/usr/bin/env bash

set -x
set -e


export WORKING_DIR=test-script
export ACCESS_KEY=1234
export SECRET_KEY=1234
export TOOLBIN=tools/bin

kubernetesVersion=$1
fybrikVersion=$2
moduleVersion=$3
certManagerVersion=$4

# Trim the last two charts of the module version
# to construct the module resource path
moduleResourceVersion=${moduleVersion%??}".0"

if [ $moduleVersion == '0.5.0' ] || [ $moduleVersion == '0.6.0' ] || [ $moduleVersion == '0.7.0' ]
then
  oldTestVersion=true
elif [ $moduleVersion == 'master' ]
then
    oldTestVersion=false
else
    git checkout tags/v$moduleVersion
    oldTestVersion=false
fi

if [ $kubernetesVersion == "kind19" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.19.16@sha256:476cb3269232888437b61deca013832fee41f9f074f9bed79f57e4280f7c48b7
elif [ $kubernetesVersion == "kind20" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.20.15@sha256:a32bf55309294120616886b5338f95dd98a2f7231519c7dedcec32ba29699394
elif [ $kubernetesVersion == "kind21" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.21.14@sha256:9d9eb5fb26b4fbc0c6d95fa8c790414f9750dd583f5d7cee45d92e8c26670aa1
elif [ $kubernetesVersion == "kind22" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.22.15@sha256:7d9708c4b0873f0fe2e171e2b1b7f45ae89482617778c1c875f1053d4cef2e41
elif [ $kubernetesVersion == "kind23" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.23.13@sha256:ef453bb7c79f0e3caba88d2067d4196f427794086a7d0df8df4f019d5e336b61
elif [ $kubernetesVersion == "kind24" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.24.7@sha256:577c630ce8e509131eab1aea12c022190978dd2f745aac5eb1fe65c0807eb315
elif [ $kubernetesVersion == "kind25" ]
then
    ${TOOLBIN}/kind delete cluster
    ${TOOLBIN}/kind create cluster --image=kindest/node:v1.25.3@sha256:f52781bc0d7a19fb6c405c2af83abfeb311f130707a0e219175677e366cc45d1
else
    echo "Unsupported kind version"
    exit 1
fi


#quick start

${TOOLBIN}/helm repo add jetstack https://charts.jetstack.io
${TOOLBIN}/helm repo add hashicorp https://helm.releases.hashicorp.com
${TOOLBIN}/helm repo add fybrik-charts https://fybrik.github.io/charts
${TOOLBIN}/helm repo update


${TOOLBIN}/helm install cert-manager jetstack/cert-manager \
    --namespace cert-manager \
    --version v$certManagerVersion \
    --create-namespace \
    --set installCRDs=true \
    --wait --timeout 400s

if [ $fybrikVersion == "master" ]
then
	rm -rf fybrik
	git clone https://github.com/fybrik/fybrik.git
	cd fybrik
	../${TOOLBIN}/helm dependency update charts/vault
	../${TOOLBIN}/helm install vault charts/vault --create-namespace -n fybrik-system \
	    --set "vault.injector.enabled=false" \
	    --set "vault.server.dev.enabled=true" \
	    --values charts/vault/env/dev/vault-single-cluster-values.yaml
	../${TOOLBIN}/kubectl wait --for=condition=ready --all pod -n fybrik-system --timeout=120s
	../${TOOLBIN}/helm install fybrik-crd charts/fybrik-crd -n fybrik-system --wait
	../${TOOLBIN}/helm install fybrik charts/fybrik --set "coordinator.catalog=katalog" --set global.tag=master -n fybrik-system --wait
	cd -
	rm -rf fybrik
else
	${TOOLBIN}/helm install vault fybrik-charts/vault --create-namespace -n fybrik-system \
        --set "vault.injector.enabled=false" \
        --set "vault.server.dev.enabled=true" \
        --values https://raw.githubusercontent.com/fybrik/fybrik/v$fybrikVersion/charts/vault/env/dev/vault-single-cluster-values.yaml
    ${TOOLBIN}/kubectl wait --for=condition=ready --all pod -n fybrik-system --timeout=400s

	${TOOLBIN}/helm install fybrik-crd fybrik-charts/fybrik-crd -n fybrik-system --version v$fybrikVersion --wait
	${TOOLBIN}/helm install fybrik fybrik-charts/fybrik --set "coordinator.catalog=katalog" -n fybrik-system --version v$fybrikVersion --wait
fi

# apply modules

# Related to https://github.com/cert-manager/cert-manager/issues/2908
# Fybrik webhook not really ready after "helm install --wait"
# A workaround is to loop until the module is applied as expected
if [ $moduleVersion == "master" ]
then
	CMD="${TOOLBIN}/kubectl apply -f ../module.yaml -n fybrik-system"
else
	CMD="${TOOLBIN}/kubectl apply -f https://github.com/fybrik/arrow-flight-module/releases/download/v$moduleVersion/module.yaml -n fybrik-system"
fi

count=0
until $CMD
do
  if [[ $count -eq 10 ]]
  then
    break
  fi
  sleep 1
  ((count=count+1))
done

# Notebook sample

${TOOLBIN}/kubectl create namespace fybrik-notebook-sample
${TOOLBIN}/kubectl config set-context --current --namespace=fybrik-notebook-sample

#localstack
${TOOLBIN}/helm repo add localstack-charts https://localstack.github.io/helm-charts
${TOOLBIN}/helm install localstack localstack-charts/localstack --set startServices="s3" --set service.type=ClusterIP
${TOOLBIN}/kubectl wait --for=condition=ready --all pod -n fybrik-notebook-sample --timeout=600s

${TOOLBIN}/kubectl port-forward svc/localstack 4566:4566 &


export ENDPOINT="http://127.0.0.1:4566"
export BUCKET="demo"
export OBJECT_KEY="PS_20174392719_1491204439457_log.csv"
export FILEPATH="$WORKING_DIR/PS_20174392719_1491204439457_log.csv"
aws configure set aws_access_key_id ${ACCESS_KEY} && aws configure set aws_secret_access_key ${SECRET_KEY} && aws --endpoint-url=${ENDPOINT} s3api create-bucket --bucket ${BUCKET} && aws --endpoint-url=${ENDPOINT} s3api put-object --bucket ${BUCKET} --key ${OBJECT_KEY} --body ${FILEPATH}


cat << EOF | ${TOOLBIN}/kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: paysim-csv
type: Opaque
stringData:
  access_key: "${ACCESS_KEY}"
  secret_key: "${SECRET_KEY}"
EOF


if [ $moduleVersion == "master" ] || [ $oldTestVersion = false ]
then
	${TOOLBIN}/kubectl apply -f $WORKING_DIR/Asset.yaml -n fybrik-notebook-sample
else
	${TOOLBIN}/kubectl apply -f $WORKING_DIR/Asset-$moduleResourceVersion.yaml -n fybrik-notebook-sample
fi

${TOOLBIN}/kubectl describe Asset paysim-csv -n fybrik-notebook-sample


if [ $moduleVersion == "master" ] || [ $oldTestVersion = false ]
then
	${TOOLBIN}/kubectl -n fybrik-system create configmap sample-policy --from-file=$WORKING_DIR/sample-policy.rego
else
	${TOOLBIN}/kubectl -n fybrik-system create configmap sample-policy --from-file=$WORKING_DIR/sample-policy-$moduleResourceVersion.rego
fi
${TOOLBIN}/kubectl -n fybrik-system label configmap sample-policy openpolicyagent.org/policy=rego

c=0
while [[ $(${TOOLBIN}/kubectl get cm sample-policy -n fybrik-system -o 'jsonpath={.metadata.annotations.openpolicyagent\.org/policy-status}') != '{"status":"ok"}' ]]
do
    echo "waiting"
    ((c++)) && ((c==25)) && break
    sleep 1
done


if [ $moduleVersion == "master" ] || [ $oldTestVersion = false ]
then
	${TOOLBIN}/kubectl apply -f $WORKING_DIR/fybrikapplication.yaml
else
	${TOOLBIN}/kubectl apply -f $WORKING_DIR/fybrikapplication-$moduleResourceVersion.yaml
fi

c=0
while [[ $(${TOOLBIN}/kubectl get fybrikapplication my-notebook -o 'jsonpath={.status.ready}') != "true" ]]
do
    echo "waiting"
    ((c++)) && ((c==30)) && break
    sleep 1
done


${TOOLBIN}/kubectl get pods -n fybrik-blueprints
POD_NAME=$(${TOOLBIN}/kubectl get pods -n fybrik-blueprints -o=name | sed "s/^.\{4\}//")

${TOOLBIN}/kubectl get cm -n fybrik-blueprints -o yaml

${TOOLBIN}/kubectl cp $WORKING_DIR/test.py ${POD_NAME}:/tmp -n fybrik-blueprints
${TOOLBIN}/kubectl exec -i ${POD_NAME} -n fybrik-blueprints -- python /tmp/test.py > res.out


${TOOLBIN}/kubectl logs ${POD_NAME} -n fybrik-blueprints

DIFF=$(diff -b $WORKING_DIR/expected.txt res.out)
RES=0
if [ "${DIFF}" == "" ]
then
    echo "test succeeded"
else
    RES=1
fi

pkill kubectl
${TOOLBIN}/kubectl delete namespace fybrik-notebook-sample
${TOOLBIN}/kubectl -n fybrik-system delete configmap sample-policy

if [ ${RES} == 1 ]
then
  echo "test failed"
  exit 1
fi
