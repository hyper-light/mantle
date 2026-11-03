{{/*
The chart's names and labels, and each pod's name, from which its Service and Secret are named.
*/}}

{{- define "mantle.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The release's name is the StatefulSet's and every pod's prefix: pod N is `<fullname>-N`. */}}
{{- define "mantle.fullname" -}}
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

{{- define "mantle.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "mantle.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "mantle.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mantle.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Pod N's name: its Service's too, and its Secret's prefix. */}}
{{- define "mantle.podName" -}}
{{- printf "%s-%d" (include "mantle.fullname" .root) (int .index) -}}
{{- end -}}

{{/* The pod's grace, refused unless it is above the node's orderly stop. */}}
{{- define "mantle.grace" -}}
{{- if le (int .Values.terminationGracePeriodSeconds) (int .Values.stopSeconds) -}}
{{- fail (printf "terminationGracePeriodSeconds (%d) must be above stopSeconds (%d): the node's orderly stop ends before the kubelet's SIGKILL" (int .Values.terminationGracePeriodSeconds) (int .Values.stopSeconds)) -}}
{{- end -}}
{{- int .Values.terminationGracePeriodSeconds -}}
{{- end -}}
