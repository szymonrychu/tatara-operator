{{- define "tatara-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tatara-operator.fullname" -}}
{{- printf "%s" (include "tatara-operator.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tatara-operator.labels" -}}
app.kubernetes.io/name: {{ include "tatara-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "tatara-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tatara-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "tatara-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tatara-operator.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
