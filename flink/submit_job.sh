#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER="${JOBMANAGER:-flink-jobmanager:8081}"
JOB_NAME="ecommerce-flink-to-hbase"
JOB_FILE="/opt/flink/job/flink_job.py"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log "Aguardando o Flink JobManager em ${JOBMANAGER}..."
until /opt/flink/bin/flink list -m "${JOBMANAGER}" >/dev/null 2>&1; do
  sleep 3
done

log "JobManager disponível. Verificando jobs ativos..."

if /opt/flink/bin/flink list -m "${JOBMANAGER}" 2>/dev/null | grep -Fq "${JOB_NAME}"; then
  log "O job ${JOB_NAME} já está em execução. Nenhuma nova submissão será feita."
else
  log "Submetendo ${JOB_NAME}..."
  /opt/flink/bin/flink run -d \
    -m "${JOBMANAGER}" \
    -Dpipeline.name="${JOB_NAME}" \
    -pyexec python3 \
    -pyclientexec python3 \
    -py "${JOB_FILE}"
  log "Job submetido."
fi

log "Contêiner de submissão mantido ativo para inspeção."
exec sleep infinity
