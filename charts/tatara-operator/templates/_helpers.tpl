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
OPERATOR_OIDC_SECRET_NAME: {{ default (include "tatara-operator.fullname" .) .Values.operatorOidcSecretName | quote }}
ANTHROPIC_SECRET_NAME: {{ .Values.anthropicSecretName | quote }}
CLI_OIDC_SECRET_NAME: {{ .Values.cliOidcSecretName | quote }}
CALLBACK_HMAC_SECRET_NAME: {{ .Values.callbackHmacSecretName | quote }}
IMAGE_PULL_SECRET: {{ .Values.imagePullSecret | quote }}
NAMESPACE: {{ .Values.namespace | quote }}
LOG_LEVEL: {{ .Values.logLevel | quote }}
INGRESS_HOST: {{ .Values.ingressHost | quote }}
INGRESS_CLASS_NAME: {{ .Values.ingressClassName | quote }}
INGRESS_REWRITE_TARGET: {{ .Values.ingressRewriteTarget | quote }}
MEMORY_PATH_PREFIX: {{ .Values.memoryPathPrefix | quote }}
CHAT_PATH_PREFIX: {{ .Values.chatPathPrefix | quote }}
CHAT_IMAGE: {{ .Values.chatImage | quote }}
GRAFANA_MCP_IMAGE: {{ .Values.grafanaMcpImage | quote }}
LEADER_ELECTION: {{ .Values.leaderElection | quote }}
TASK_RETENTION_HOURS: {{ .Values.taskRetentionHours | quote }}
PUSH_METRICS_ALLOWED_PREFIXES: {{ .Values.pushMetricsAllowedPrefixes | quote }}
AGENT_CPU_REQUEST: {{ .Values.agentCpuRequest | quote }}
AGENT_CPU_LIMIT: {{ .Values.agentCpuLimit | quote }}
AGENT_MEMORY_REQUEST: {{ .Values.agentMemoryRequest | quote }}
AGENT_MEMORY_LIMIT: {{ .Values.agentMemoryLimit | quote }}
{{- if and .Values.agentRunAsNonRoot (not .Values.agentRunAsUser) }}
{{- fail "agentRunAsNonRoot:true requires a numeric agentRunAsUser: the kubelet cannot verify a non-numeric image USER (e.g. agent) is non-root, so agent pods fail with CreateContainerConfigError. Set agentRunAsUser to the image's numeric uid, or set agentRunAsNonRoot:false." }}
{{- end }}
AGENT_RUN_AS_NON_ROOT: {{ .Values.agentRunAsNonRoot | quote }}
AGENT_RUN_AS_USER: {{ .Values.agentRunAsUser | quote }}
AGENT_FS_GROUP: {{ .Values.agentFsGroup | quote }}
{{/* List-shaped placement (rule 6): one JSON document key, empty default keeps the chart cluster-agnostic (rule 14). */}}
AGENT_SCHEDULING: {{ .Values.agentScheduling | toJson | quote }}
{{/* S3 conversation persistence (issue #114): empty s3Bucket disables it. */}}
S3_ENDPOINT: {{ .Values.s3Endpoint | quote }}
S3_BUCKET: {{ .Values.s3Bucket | quote }}
S3_REGION: {{ .Values.s3Region | quote }}
S3_KEY_PREFIX: {{ .Values.s3KeyPrefix | quote }}
S3_FORCE_PATH_STYLE: {{ .Values.s3ForcePathStyle | quote }}
S3_SECRET_NAME: {{ .Values.s3SecretName | quote }}
S3_CONVERSATION_RETENTION_HOURS: {{ .Values.s3ConversationRetentionHours | quote }}
{{- end -}}
