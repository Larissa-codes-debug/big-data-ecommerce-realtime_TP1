#!/usr/bin/env bash
set -e
docker compose exec -T flink-jobmanager flink run -d -py /opt/flink/job/flink_job.py
