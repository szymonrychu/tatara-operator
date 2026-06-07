{{/*
Expand the name of the chart.
*/}}
{{- define "tatara-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "tatara-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "tatara-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "tatara-operator.labels" -}}
helm.sh/chart: {{ include "tatara-operator.chart" . }}
{{ include "tatara-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "tatara-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tatara-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "tatara-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tatara-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Map camelCase values.* scalars to SCREAMING_SNAKE ConfigMap keys.
Strict: values.yaml carries only scalars; this macro is the single mapping point.
*/}}
{{- define "tatara-operator.envConfig" -}}
HTTP_ADDR: {{ .Values.httpAddr | quote }}
METRICS_ADDR: {{ .Values.metricsAddr | quote }}
HEALTH_ADDR: {{ .Values.healthAddr | quote }}
INTERNAL_ADDR: {{ .Values.internalAddr | quote }}
CALLBACK_URL: {{ .Values.callbackUrl | quote }}
OIDC_ISSUER: {{ .Values.oidcIssuer | quote }}
OIDC_AUDIENCE: {{ .Values.oidcAudience | quote }}
MEMORY_IMAGE: {{ .Values.memoryImage | quote }}
LIGHTRAG_IMAGE: {{ .Values.lightragImage | quote }}
NEO4J_IMAGE: {{ .Values.neo4jImage | quote }}
OPENAI_SECRET_NAME: {{ .Values.openaiSecretName | quote }}
INGESTER_IMAGE: {{ .Values.ingesterImage | quote }}
EXTERNAL_WEBHOOK_BASE: {{ .Values.externalWebhookBase | quote }}
OPERATOR_OIDC_CLIENT_ID: {{ .Values.operatorOidcClientId | quote }}
ANTHROPIC_SECRET_NAME: {{ .Values.anthropicSecretName | quote }}
CLI_OIDC_SECRET_NAME: {{ .Values.cliOidcSecretName | quote }}
IMAGE_PULL_SECRET: {{ .Values.imagePullSecret | quote }}
NAMESPACE: {{ .Values.namespace | quote }}
LOG_LEVEL: {{ .Values.logLevel | quote }}
{{- end -}}
