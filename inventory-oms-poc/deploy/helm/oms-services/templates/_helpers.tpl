{{- define "oms-services.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- define "oms-services.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "oms-services.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- define "oms-services.serviceAccountName" -}}
{{- if .Values.global.serviceAccount.create }}{{ include "oms-services.fullname" . }}{{ else }}default{{ end }}
{{- end }}