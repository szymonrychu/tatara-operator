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
GRAFANA_MCP_IMAGE: {{ .Values.grafanaMcpImage | quote }}
{{/* Per-Project memory-stack ServiceMonitor + PrometheusRule (issue #200). additionalLabels is a JSON object (rule 6), empty default keeps the chart cluster-agnostic (rule 14). */}}
MEMORY_MONITORING_ENABLED: {{ .Values.memoryMonitoring.enabled | quote }}
MEMORY_MONITOR_LABELS: {{ .Values.memoryMonitoring.additionalLabels | toJson | quote }}
LEADER_ELECTION: {{ .Values.leaderElection | quote }}
IDLE_POD_REAP_MINUTES: {{ .Values.idlePodReapMinutes | quote }}
MEMORY_PROVISIONING_TIMEOUT_MINUTES: {{ .Values.memoryProvisioningTimeoutMinutes | quote }}
PUSH_METRICS_ALLOWED_PREFIXES: {{ .Values.pushMetricsAllowedPrefixes | quote }}
INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES: {{ .Values.incidentRefireCommentCooldownMinutes | quote }}
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
{{/* Token-budget admission gate (issue #189): off until tokenBudgetEnabled. */}}
TOKEN_BUDGET_ENABLED: {{ .Values.tokenBudgetEnabled | quote }}
TOKEN_BUDGET_MODE: {{ .Values.tokenBudgetMode | quote }}
TOKEN_BUDGET_PROACTIVE_PERCENT: {{ .Values.tokenBudgetProactivePercent | quote }}
TOKEN_BUDGET_EMERGENCY_PERCENT: {{ .Values.tokenBudgetEmergencyPercent | quote }}
TOKEN_BUDGET_RESET_SCHEDULE: {{ .Values.tokenBudgetResetSchedule | quote }}
TOKEN_BUDGET_WINDOW: {{ .Values.tokenBudgetWindow | quote }}
TOKEN_BUDGET_TOKEN_LIMIT: {{ .Values.tokenBudgetTokenLimit | quote }}
{{/* Claude account-usage poller (claudeSubscription gate): off until usageEnabled. */}}
USAGE_ENABLED: {{ .Values.usageEnabled | quote }}
USAGE_AUTH_MODE: {{ .Values.usageAuthMode | quote }}
USAGE_POLL_INTERVAL: {{ .Values.usagePollInterval | quote }}
USAGE_USER_AGENT: {{ .Values.usageUserAgent | quote }}
USAGE_BASE_URL: {{ .Values.usageBaseUrl | quote }}
USAGE_SECRET_NAME: {{ .Values.usageSecretName | quote }}
USAGE_OAUTH_CLIENT_ID: {{ .Values.usageOauthClientId | quote }}
USAGE_TOKEN_URL: {{ .Values.usageTokenUrl | quote }}
USAGE_REFRESH_MARGIN: {{ .Values.usageRefreshMargin | quote }}
{{- end -}}
