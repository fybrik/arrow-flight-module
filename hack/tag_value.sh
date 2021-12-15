#!/usr/bin/env bash
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

: ${TOOLBIN:=./hack/tools/bin}

${TOOLBIN}/yq eval --inplace ".image.tag = \"$DOCKER_TAG\"" helm/afm/values.yaml
