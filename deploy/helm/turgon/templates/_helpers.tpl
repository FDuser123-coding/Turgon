{{- define "turgon.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "turgon.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "turgon.selector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "turgon.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* The Secret and key holding Turgon's database URL. */}}
{{- define "turgon.databaseSecret" -}}
{{- if .Values.database.cloudNativePG.enabled -}}
{{ include "turgon.fullname" . }}-db-app
{{- else -}}
{{ required "database.existingSecret is required unless database.cloudNativePG.enabled" .Values.database.existingSecret }}
{{- end -}}
{{- end -}}

{{- define "turgon.databaseKey" -}}
{{- if .Values.database.cloudNativePG.enabled -}}uri{{- else -}}{{ .Values.database.existingSecretKey }}{{- end -}}
{{- end -}}

{{- define "turgon.commonEnv" -}}
- name: TURGON_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "turgon.databaseSecret" . }}
      key: {{ include "turgon.databaseKey" . }}
- name: TURGON_TEMPORAL_ADDRESS
  value: {{ .Values.temporal.address | quote }}
- name: TURGON_TEMPORAL_NAMESPACE
  value: {{ .Values.temporal.namespace | quote }}
{{- include "turgon.signingEnv" . }}
{{- with .Values.temporal.tls }}
{{- if .enabled }}
- name: TURGON_TEMPORAL_TLS
  value: "true"
{{- end }}
{{- with .serverName }}
- name: TURGON_TEMPORAL_SERVER_NAME
  value: {{ . | quote }}
{{- end }}
{{- if .caKey }}
- name: TURGON_TEMPORAL_CA
  value: /etc/turgon/temporal-tls/ca
{{- end }}
{{- if .certKey }}
- name: TURGON_TEMPORAL_CERT
  value: /etc/turgon/temporal-tls/cert
- name: TURGON_TEMPORAL_KEY
  value: /etc/turgon/temporal-tls/key
{{- end }}
{{- end }}
{{- with .Values.temporal.existingSecret }}
- name: TURGON_TEMPORAL_API_KEY
  valueFrom:
    secretKeyRef: { name: {{ . }}, key: {{ $.Values.temporal.apiKeyKey }}, optional: true }
- name: TURGON_PAYLOAD_KEYS
  valueFrom:
    secretKeyRef: { name: {{ . }}, key: {{ $.Values.temporal.payloadKeysKey }}, optional: true }
{{- end }}
{{- end -}}

{{/* Files every Turgon container may need: the Temporal TLS files, and
     the public keys runtime specs must be signed with. */}}
{{- define "turgon.sharedMounts" -}}
{{- with .Values.temporal.tls }}
{{- if or .caKey .certKey }}
- { name: temporal-tls, mountPath: /etc/turgon/temporal-tls, readOnly: true }
{{- end }}
{{- end }}
{{- if .Values.specSigning.trustedKeys }}
- { name: trusted-keys, mountPath: /etc/turgon/trusted-keys, readOnly: true }
{{- end }}
{{- end -}}

{{/* Runtime specs are loaded only if a trusted key signed them. */}}
{{- define "turgon.signingEnv" -}}
{{- if .Values.specSigning.trustedKeys }}
- name: TURGON_TRUSTED_KEYS
  value: /etc/turgon/trusted-keys/keys.pem
{{- end }}
{{- end -}}

{{- define "turgon.sharedVolumes" -}}
{{- if .Values.specSigning.trustedKeys }}
- name: trusted-keys
  configMap:
    name: {{ include "turgon.fullname" . }}-trusted-keys
{{- end }}
{{- with .Values.temporal.tls }}
{{- if or .caKey .certKey }}
- name: temporal-tls
  secret:
    secretName: {{ required "temporal.tls.existingSecret is required with temporal.tls.caKey or certKey" .existingSecret }}
    items:
      {{- with .caKey }}
      - { key: {{ . }}, path: ca }
      {{- end }}
      {{- if .certKey }}
      - { key: {{ .certKey }}, path: cert }
      - { key: {{ required "temporal.tls.keyKey is required with temporal.tls.certKey" .keyKey }}, path: key }
      {{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "turgon.podSettings" -}}
serviceAccountName: {{ include "turgon.fullname" . }}
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
     each `turgon mcp` port (8090, 8091, ...). */}}
{{- define "turgon.agentSpecs" -}}
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

{{/* Arguments of `turgon gateway-config` for the agents Deployment and its test. */}}
{{- define "turgon.gatewayConfigArgs" -}}
{{- $g := .Values.agents.gateway -}}
- gateway-config
{{- range $i, $name := include "turgon.agentSpecs" . | fromJsonArray }}
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

{{/* The operator's pod template for workers (a PodTemplateSpec). */}}
{{- define "turgon.workerTemplate" -}}
metadata:
  labels:
    {{- include "turgon.selector" . | nindent 4 }}
spec:
  {{- include "turgon.podSettings" . | nindent 2 }}
  containers:
    - name: worker
      image: {{ include "turgon.image" . }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      env:
        {{- include "turgon.commonEnv" . | nindent 8 }}
        {{- with .Values.workers.consoleURL }}
        - name: TURGON_CONSOLE_URL
          value: {{ . | quote }}
        {{- end }}
      {{- with .Values.workers.secretEnvFrom }}
      envFrom:
        {{- range . }}
        - secretRef:
            name: {{ . }}
        {{- end }}
      {{- end }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      resources:
        {{- toYaml .Values.workers.resources | nindent 8 }}
      volumeMounts:
        - { name: tmp, mountPath: /tmp }
        {{- include "turgon.sharedMounts" . | nindent 8 }}
  volumes:
    {{- include "turgon.sharedVolumes" . | nindent 4 }}
    - name: tmp
      emptyDir: { sizeLimit: 64Mi }
{{- end -}}
