#!/usr/bin/env bash
# Copyright 2020 IBM Corp.
# SPDX-License-Identifier: Apache-2.0

: ${TOOLBIN:=./tools/bin}
: ${ROOT_DIR:=../}

${TOOLBIN}/yq e --inplace ".spec.chart.name = \"ghcr.io/fybrik/arrow-flight-module-chart:$DOCKER_TAG\"" ${ROOT_DIR}module.yaml
${TOOLBIN}/yq e --inplace '.spec.chart.values."image.tag" |= strenv(DOCKER_TAG)' ${ROOT_DIR}module.yaml
${TOOLBIN}/yq e --inplace '.spec.chart.values."containerSecurityContext.readOnlyRootFilesystem" |= true ' ${ROOT_DIR}module.yaml



