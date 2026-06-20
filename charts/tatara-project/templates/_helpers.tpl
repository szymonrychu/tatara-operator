{{/*
Expand the name of the chart.
*/}}
{{- define "tatara-project.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "tatara-project.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels stamped onto every rendered CR.
*/}}
{{- define "tatara-project.labels" -}}
helm.sh/chart: {{ include "tatara-project.chart" . }}
app.kubernetes.io/name: {{ include "tatara-project.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Namespace the CRs are created in: the operator watches this namespace, so it
must match the tatara-operator release namespace. Defaults to the release
namespace; override with .Values.namespace.
*/}}
{{- define "tatara-project.namespace" -}}
{{- default .Release.Namespace .Values.namespace }}
{{- end }}
