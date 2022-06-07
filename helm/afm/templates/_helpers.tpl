{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "arrow-flight-module.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "arrow-flight-module.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "arrow-flight-module.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
processPodSecurityContext skips certain keys in Values.podSecurityContext
map if running on openshift.
*/}}
{{- define "fybrik.processPodSecurityContext" }}
{{- $podSecurityContext := deepCopy .podSecurityContext }}
{{- if .context.Capabilities.APIVersions.Has "security.openshift.io/v1" }}
  {{- range $k, $v := .podSecurityContext }}
    {{- if or (eq $k "runAsUser") (eq $k "seccompProfile") }}
      {{- $_ := unset $podSecurityContext $k }}
    {{- end }}
   {{- end }}
{{- end }}
{{- $podSecurityContext | toYaml }}
{{- end }}
