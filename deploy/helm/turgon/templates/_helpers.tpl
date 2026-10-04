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
{{- with .Values.agents.pushAllow }}
- name: TURGON_A2A_PUSH_ALLOW
  value: {{ join "," . | quote }}
{{- end }}
{{- with .Values.workers.pluginNetworkAllow }}
- name: TURGON_PLUGIN_NETWORK_ALLOW
  value: {{ join "," . | quote }}
{{- end }}
- name: TURGON_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "turgon.databaseSecret" . }}
      key: {{ include "turgon.databaseKey" . }}
{{- if or .Values.splink.enabled .Values.splink.url }}
- name: TURGON_SPLINK_URL
  value: {{ .Values.splink.url | default (printf "http://%s-splink:8080" (include "turgon.fullname" .)) | quote }}
{{- with .Values.splink.existingSecret }}
- name: TURGON_SPLINK_TOKEN
  valueFrom:
    secretKeyRef: { name: {{ . }}, key: token }
{{- end }}
{{- end }}
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
{{- include "turgon.secretsEnv" . }}
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
{{- if eq .Values.secrets.backend "openbao" }}
{{- with .Values.secrets.openbao }}
{{- if eq .auth "kubernetes" }}
- { name: openbao-token, mountPath: /var/run/secrets/turgon/openbao, readOnly: true }
{{- else if eq .auth "approle" }}
- { name: openbao-approle, mountPath: /etc/turgon/openbao-approle, readOnly: true }
{{- end }}
{{- if .caSecret }}
- { name: openbao-ca, mountPath: /etc/turgon/openbao-ca, readOnly: true }
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* Connection secrets from OpenBao or Vault (secrets.backend: openbao). */}}
{{- define "turgon.secretsEnv" -}}
{{- if eq .Values.secrets.backend "openbao" }}
{{- with .Values.secrets.openbao }}
- name: TURGON_SECRETS
  value: openbao
- name: TURGON_OPENBAO_ADDR
  value: {{ required "secrets.openbao.address is required with secrets.backend openbao" .address | quote }}
- name: TURGON_OPENBAO_MOUNT
  value: {{ .mount | quote }}
- name: TURGON_OPENBAO_AUTH
  value: {{ .auth | quote }}
{{- with .namespace }}
- name: TURGON_OPENBAO_NAMESPACE
  value: {{ . | quote }}
{{- end }}
{{- with .authMount }}
- name: TURGON_OPENBAO_AUTH_MOUNT
  value: {{ . | quote }}
{{- end }}
{{- if .caSecret }}
- name: TURGON_OPENBAO_CACERT
  value: /etc/turgon/openbao-ca/ca.crt
{{- end }}
{{- if eq .auth "kubernetes" }}
- name: TURGON_OPENBAO_ROLE
  value: {{ required "secrets.openbao.role is required with kubernetes auth" .role | quote }}
- name: TURGON_OPENBAO_JWT_FILE
  value: /var/run/secrets/turgon/openbao/token
{{- else if eq .auth "approle" }}
- name: TURGON_OPENBAO_ROLE
  value: {{ required "secrets.openbao.role (the role ID) is required with approle auth" .role | quote }}
- name: TURGON_OPENBAO_SECRET_ID_FILE
  value: /etc/turgon/openbao-approle/secret-id
{{- else if eq .auth "token" }}
- name: TURGON_OPENBAO_TOKEN
  valueFrom:
    secretKeyRef: { name: {{ required "secrets.openbao.existingSecret (key token) is required with token auth" .existingSecret }}, key: token }
{{- else }}
{{- fail "secrets.openbao.auth must be kubernetes, approle or token" }}
{{- end }}
{{- end }}
{{- else if eq .Values.secrets.backend "aws" }}
- name: TURGON_SECRETS
  value: aws
- name: TURGON_AWS_REGION
  value: {{ required "secrets.aws.region is required with secrets.backend aws" .Values.secrets.aws.region | quote }}
{{- include "turgon.secretsPrefix" . }}
{{- else if eq .Values.secrets.backend "azure" }}
- name: TURGON_SECRETS
  value: azure
- name: TURGON_AZURE_VAULT_URL
  value: {{ required "secrets.azure.vaultURL is required with secrets.backend azure" .Values.secrets.azure.vaultURL | quote }}
{{- include "turgon.secretsPrefix" . }}
{{- else if eq .Values.secrets.backend "gcp" }}
- name: TURGON_SECRETS
  value: gcp
- name: TURGON_GCP_PROJECT
  value: {{ required "secrets.gcp.project is required with secrets.backend gcp" .Values.secrets.gcp.project | quote }}
{{- include "turgon.secretsPrefix" . }}
{{- else if ne .Values.secrets.backend "env" }}
{{- fail "secrets.backend must be env, openbao, aws, azure or gcp" }}
{{- end }}
{{- end -}}

{{- define "turgon.secretsPrefix" -}}
{{- with .Values.secrets.prefix }}
- name: TURGON_SECRETS_PREFIX
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/* Pod labels the platform's workload identity needs (AKS). */}}
{{- define "turgon.identityLabels" -}}
{{- if and (eq .Values.secrets.backend "azure") .Values.secrets.azure.workloadIdentity }}
azure.workload.identity/use: "true"
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
{{- if eq .Values.secrets.backend "openbao" }}
{{- with .Values.secrets.openbao }}
{{- if eq .auth "kubernetes" }}
# A service account token for OpenBao alone: bound to its audience, valid
# for an hour and rotated by the kubelet. Pods mount no other token.
- name: openbao-token
  projected:
    sources:
      - serviceAccountToken:
          path: token
          audience: {{ .audience | quote }}
          expirationSeconds: 3600
{{- else if eq .auth "approle" }}
- name: openbao-approle
  secret:
    secretName: {{ required "secrets.openbao.existingSecret (key secret-id) is required with approle auth" .existingSecret }}
    items:
      - { key: secret-id, path: secret-id }
{{- end }}
{{- with .caSecret }}
- name: openbao-ca
  secret:
    secretName: {{ . }}
    items:
      - { key: ca.crt, path: ca.crt }
{{- end }}
{{- end }}
{{- end }}
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
    {{- include "turgon.identityLabels" . | nindent 4 }}
spec:
  {{- include "turgon.podSettings" . | nindent 2 }}
  containers:
    - name: worker
      image: {{ include "turgon.image" . }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      env:
        {{- include "turgon.commonEnv" . | nindent 8 }}
        {{- include "turgon.connectorEnv" . | nindent 8 }}
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
        {{- include "turgon.connectorMounts" . | nindent 8 }}
    {{- include "turgon.connectorSidecars" . | nindent 4 }}
  volumes:
    {{- include "turgon.sharedVolumes" . | nindent 4 }}
    {{- include "turgon.connectorVolumes" . | nindent 4 }}
    - name: tmp
      emptyDir: { sizeLimit: 64Mi }
{{- end -}}

{{/* Connectors in sidecars (connectors.sidecars): the worker's environment. */}}
{{- define "turgon.connectorEnv" -}}
{{- with .Values.connectors.sidecars }}
{{- $pairs := list }}
{{- range . }}
{{- $pairs = append $pairs (printf "%s=unix:///var/run/turgon/connectors/%s.sock" .name .name) }}
{{- end }}
- name: TURGON_CONNECTORS
  value: {{ join "," $pairs | quote }}
{{- end }}
{{- end -}}

{{- define "turgon.connectorMounts" -}}
{{- if .Values.connectors.sidecars }}
- { name: connectors, mountPath: /var/run/turgon/connectors }
{{- end }}
{{- end -}}

{{/* The sidecar containers. Each listens on a Unix socket in a volume only
     its pod mounts; the worker keeps policy, approval, idempotency and audit. */}}
{{- define "turgon.connectorSidecars" -}}
{{- $seen := dict }}
{{- range .Values.connectors.sidecars }}
{{- if hasKey $seen .name }}
{{- fail (printf "connectors.sidecars: %s is listed twice" .name) }}
{{- end }}
{{- $_ := set $seen .name true }}
- name: connector-{{ .name }}
  image: "{{ required "connectors.sidecars[].image.repository is required" .image.repository }}:{{ .image.tag | default $.Chart.AppVersion }}"
  imagePullPolicy: {{ .image.pullPolicy | default "IfNotPresent" }}
  env:
    - name: TURGON_CONNECTOR_LISTEN
      value: unix:///var/run/turgon/connectors/{{ .name }}.sock
    - name: TURGON_CONNECTOR
      value: {{ .name | quote }}
    - name: JAVA_TOOL_OPTIONS
      value: "-XX:MaxRAMPercentage=75 -Djava.io.tmpdir=/tmp"
    {{- with .env }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  securityContext:
    {{- toYaml $.Values.securityContext | nindent 4 }}
  resources:
    {{- toYaml (.resources | default $.Values.connectors.resources) | nindent 4 }}
  volumeMounts:
    - { name: connectors, mountPath: /var/run/turgon/connectors }
    - { name: connector-tmp-{{ .name }}, mountPath: /tmp }
    {{- with .volumeMounts }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end }}
{{- end -}}

{{- define "turgon.connectorVolumes" -}}
{{- if .Values.connectors.sidecars }}
- name: connectors
  emptyDir: { medium: Memory, sizeLimit: 1Mi }
{{- range .Values.connectors.sidecars }}
- name: connector-tmp-{{ .name }}
  emptyDir: { sizeLimit: 256Mi }
{{- with .volumes }}
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
