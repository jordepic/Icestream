{{- define "icestream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "icestream.fullname" -}}
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

{{- define "icestream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "icestream.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "icestream.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "icestream.labels" -}}
app.kubernetes.io/name: {{ include "icestream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "icestream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "icestream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Conversion-channel Service name + URL. In flinkMode=remote the session-cluster TaskManagers
     reach the icestream pod's channel server (lookup work-in/results-out) through this Service. */}}
{{- define "icestream.channelServiceName" -}}
{{- printf "%s-channel" (include "icestream.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Iceberg + Paimon catalog env. Include with nindent. */}}
{{- define "icestream.catalogEnv" -}}
- name: ICESTREAM_ICEBERG_REST_URI
  value: {{ .Values.appConfig.iceberg.restUri | quote }}
- name: ICESTREAM_ICEBERG_WAREHOUSE
  value: {{ .Values.appConfig.iceberg.warehouse | quote }}
{{- range $k, $v := .Values.appConfig.iceberg.extraOptions }}
- name: ICESTREAM_ICEBERG_OPT_{{ $k | upper | replace "." "_" | replace "-" "_" }}
  value: {{ $v | quote }}
{{- end }}
- name: ICESTREAM_PAIMON_WAREHOUSE
  value: {{ .Values.appConfig.paimon.warehouse | quote }}
- name: ICESTREAM_PAIMON_DATABASE
  value: {{ .Values.appConfig.paimon.database | quote }}
{{- range $k, $v := .Values.appConfig.paimon.extraOptions }}
- name: ICESTREAM_PAIMON_OPT_{{ $k | upper | replace "." "_" | replace "-" "_" }}
  value: {{ $v | quote }}
{{- end }}
{{- end -}}
