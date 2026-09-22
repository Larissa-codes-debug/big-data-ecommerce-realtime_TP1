# Roteiro de execução e validação

## Subir o ambiente

```bash
docker compose build
docker compose up -d
docker compose ps
```

## Conferir o Flink

```bash
docker compose logs --tail=200 flink-job
docker compose exec flink-jobmanager flink list
```

## Conferir o HDFS

```bash
docker compose exec namenode hdfs dfs -ls /data/raw/ecommerce/logs
docker compose exec namenode hdfs dfs -cat /data/raw/ecommerce/logs/* | head -n 5
```

## Conferir o HBase

```bash
docker compose exec hbase hbase shell -n <<'EOF'
list
scan 'ecommerce_metrics', {LIMIT => 10}
EOF
```

## Executar Spark

```bash
docker compose exec -T spark spark-submit /opt/spark/jobs/etl_ecommerce.py
```

## Validar tudo

```bash
bash scripts/validate_pipeline.sh
```
