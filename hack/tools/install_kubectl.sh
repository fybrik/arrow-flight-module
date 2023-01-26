#!/usr/bin/env bash
# Copyright 2020 The Kubernetes Authors.
# SPDX-License-Identifier: Apache-2.0

source ./common.sh

header_text "Checking for bin/kubectl ${KUBECTL_VERSION}"
[[ -f bin/kubectl && `bin/kubectl version -o=yaml 2> /dev/null | bin/yq e '.clientVersion.gitVersion' -` == "v${KUBECTL_VERSION}" ]] && exit 0

header_text "Installing bin/kubectl ${KUBECTL_VERSION}"

mkdir -p ./bin
curl -L https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl -o bin/kubectl

chmod +x bin/kubectl
