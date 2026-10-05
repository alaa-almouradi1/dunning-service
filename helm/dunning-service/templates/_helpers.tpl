{{- define "dunning.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dunning.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "dunning.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "dunning.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "dunning.env" -}}
- name: DUNNING_KAFKA_BROKERS
  value: {{ .Values.config.kafkaBrokers | quote }}
- name: DUNNING_KAFKA_TOPIC
  value: {{ .Values.config.kafkaTopic | quote }}
- name: DUNNING_KAFKA_DLQ_TOPIC
  value: {{ .Values.config.kafkaDlqTopic | quote }}
- name: DUNNING_KAFKA_GROUP_ID
  value: {{ .Values.config.kafkaGroupId | quote }}
- name: DUNNING_BILLING_API_URL
  value: {{ .Values.config.billingApiUrl | quote }}
- name: DUNNING_RETRY_SCHEDULE
  value: {{ .Values.config.retrySchedule | quote }}
- name: DUNNING_SCHEDULER_INTERVAL_SECONDS
  value: {{ .Values.config.schedulerIntervalSeconds | quote }}
- name: DUNNING_LOG_LEVEL
  value: {{ .Values.config.logLevel | quote }}
- name: DUNNING_DB_POOL_SIZE
  value: {{ .Values.config.dbPoolSize | quote }}
- name: DUNNING_SCHEDULER_CONCURRENCY
  value: {{ .Values.scheduler.concurrency | quote }}
- name: DUNNING_KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.config.kafkaSecurityProtocol | quote }}
{{- with .Values.config.kafkaSaslMechanism }}
- name: DUNNING_KAFKA_SASL_MECHANISM
  value: {{ . | quote }}
{{- end }}
{{- with .Values.config.kafkaSaslUsername }}
- name: DUNNING_KAFKA_SASL_USERNAME
  value: {{ . | quote }}
{{- end }}
- name: DUNNING_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: DUNNING_DATABASE_URL
- name: DUNNING_BILLING_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: DUNNING_BILLING_API_KEY
{{- range list "DUNNING_ADMIN_TOKEN" "DUNNING_METRICS_TOKEN" "DUNNING_KAFKA_SASL_PASSWORD" }}
- name: {{ . }}
  valueFrom:
    secretKeyRef:
      name: {{ $.Values.existingSecret }}
      key: {{ . }}
      optional: true
{{- end }}
{{- end -}}
