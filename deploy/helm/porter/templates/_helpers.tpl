{{- define "porter.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "porter.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "porter.selector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "porter.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* The Secret and key holding Porter's database URL. */}}
{{- define "porter.databaseSecret" -}}
{{- if .Values.database.cloudNativePG.enabled -}}
{{ include "porter.fullname" . }}-db-app
{{- else -}}
{{ required "database.existingSecret is required unless database.cloudNativePG.enabled" .Values.database.existingSecret }}
{{- end -}}
{{- end -}}

{{- define "porter.databaseKey" -}}
{{- if .Values.database.cloudNativePG.enabled -}}uri{{- else -}}{{ .Values.database.existingSecretKey }}{{- end -}}
{{- end -}}

{{- define "porter.commonEnv" -}}
- name: PORTER_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "porter.databaseSecret" . }}
      key: {{ include "porter.databaseKey" . }}
- name: PORTER_TEMPORAL_ADDRESS
  value: {{ .Values.temporal.address | quote }}
- name: PORTER_TEMPORAL_NAMESPACE
  value: {{ .Values.temporal.namespace | quote }}
{{- end -}}

{{- define "porter.podSettings" -}}
serviceAccountName: {{ include "porter.fullname" . }}
automountServiceAccountToken: false
securityContext:
  {{- toYaml .Values.podSecurityContext | nindent 2 }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* The specs served to agents, sorted, as a JSON list. Their order sets
     each `porter mcp` port (8090, 8091, ...). */}}
{{- define "porter.agentSpecs" -}}
{{- $names := list -}}
{{- range $n := keys .Values.specs | sortAlpha -}}
{{- if or (not $.Values.agents.specs) (has $n $.Values.agents.specs) -}}
{{- $names = append $names $n -}}
{{- end -}}
{{- end -}}
{{- range $w := .Values.agents.writes -}}
{{- if not (has $w $names) -}}
{{- fail (printf "agents.writes: %s is not a spec served to agents" $w) -}}
{{- end -}}
{{- end -}}
{{- toJson $names -}}
{{- end -}}

{{/* Arguments of `porter gateway-config` for the agents Deployment and its test. */}}
{{- define "porter.gatewayConfigArgs" -}}
{{- $g := .Values.agents.gateway -}}
- gateway-config
{{- range $i, $name := include "porter.agentSpecs" . | fromJsonArray }}
- --spec={{ $name }}=/specs/{{ $name }}.json
- --upstream={{ $name }}=http://127.0.0.1:{{ add 8090 $i }}/
{{- end }}
{{- range .Values.agents.writes }}
- --writes={{ . }}
{{- end }}
- --issuer={{ required "agents.gateway.issuer is required" $g.issuer }}
- --jwks={{ required "agents.gateway.jwksURL is required" $g.jwksURL }}
{{- range $g.audiences }}
- --audience={{ . }}
{{- end }}
- --agent-claim={{ $g.claims.agent }}
- --user-claim={{ $g.claims.user }}
- --roles-claim={{ $g.claims.roles }}
{{- with $g.rateLimit }}
- --rate-limit={{ . }}
{{- end }}
{{- end -}}
