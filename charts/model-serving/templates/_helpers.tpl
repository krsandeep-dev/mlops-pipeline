{{- define "model-serving.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "model-serving.labels" -}}
app.kubernetes.io/name: {{ include "model-serving.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- /*
Selector labels stay deliberately narrow -- just the name, exactly as the raw M3
manifest selected. A Deployment's selector is immutable, so keeping it minimal means
the chart can adopt or replace the raw deployment without a selector conflict.
*/ -}}
{{- define "model-serving.selectorLabels" -}}
app.kubernetes.io/name: {{ include "model-serving.name" . }}
{{- end -}}
