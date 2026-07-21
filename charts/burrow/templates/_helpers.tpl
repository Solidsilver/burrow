{{/*
Chart name / instance naming.
*/}}
{{- define "burrow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "burrow.fullname" -}}
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

{{- define "burrow.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels / selector labels.
*/}}
{{- define "burrow.labels" -}}
helm.sh/chart: {{ include "burrow.chart" . }}
{{ include "burrow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "burrow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "burrow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "burrow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "burrow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding the recovery phrase: ours or the user's.
*/}}
{{- define "burrow.secretName" -}}
{{- if .Values.burrow.existingSecret }}
{{- .Values.burrow.existingSecret }}
{{- else }}
{{- include "burrow.fullname" . }}
{{- end }}
{{- end }}

{{/*
Image reference (tag defaults to appVersion).
*/}}
{{- define "burrow.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{/*
Pinned paths, shared by the bootstrap init container and the daemon so both
resolve the same config/data dirs. The PVCs are mounted at /config and /data
and burrow uses a SUBDIRECTORY on each: volume roots are root-owned, while a
subdirectory can be created (and chmod 0700'd by the daemon) as uid 10001
thanks to fsGroup — no root init container needed.
*/}}
{{- define "burrow.env" -}}
- name: BURROW_CONFIG_DIR
  value: /config/burrow
- name: BURROW_DATA_DIR
  value: /data/burrow
- name: BURROW_SOCKET
  value: /run/burrow/daemon.sock
{{- if .Values.persistence.blobs.enabled }}
- name: BURROW_BLOBS_DIR
  value: /blobs/burrow
{{- end }}
{{- end }}

{{/*
Values validation, called from the StatefulSet so `helm template` fails too.
*/}}
{{- define "burrow.validate" -}}
{{- if and .Values.burrow.recoveryPhrase .Values.burrow.existingSecret }}
{{- fail "set only ONE of burrow.recoveryPhrase / burrow.existingSecret" }}
{{- end }}
{{- if and (not .Values.burrow.recoveryPhrase) (not .Values.burrow.existingSecret) }}
{{- fail "identity required: set burrow.recoveryPhrase (chart creates the Secret) or burrow.existingSecret (you manage it). See values.yaml — generate a phrase with: docker run --rm ghcr.io/solidsilver/burrow init" }}
{{- end }}
{{- if and .Values.burrow.deviceName (not (regexMatch "^[a-zA-Z0-9_-]+$" .Values.burrow.deviceName)) }}
{{- fail "burrow.deviceName must match [a-zA-Z0-9_-]+ (it is part of the device identity)" }}
{{- end }}
{{- if and .Values.web.ingress.enabled (not .Values.web.service.enabled) }}
{{- fail "web.ingress.enabled requires web.service.enabled = true" }}
{{- end }}
{{- range .Values.backupSources }}
{{- if not .name }}
{{- fail "every backupSources entry needs a name" }}
{{- end }}
{{- if or (and .hostPath .existingClaim) (and (not .hostPath) (not .existingClaim)) }}
{{- fail (printf "backupSources[%s]: set exactly one of hostPath / existingClaim" .name) }}
{{- end }}
{{- end }}
{{- end }}
