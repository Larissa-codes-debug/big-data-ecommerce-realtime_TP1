# big-data-ecommerce-realtime_TP1
Monitoramento de vendas e logística de um e-commerce. Os alunos atuarão como Engenheiros de Dados de uma varejista online, construindo um pipeline que combina processamento em tempo real (streaming) e em lote (batch) para monitorar cliques, carrinho e status de entrega. A entrega consiste em código e vídeo — sem relatório escrito


## Fluxo de arquivos do Flink (versão simplificada)

O Flume grava os arquivos em `/var/lib/flink-input/staging`. O serviço
`flink-file-finalizer` move arquivos `.json` que não foram modificados por
pelo menos 15 segundos para `/var/lib/flink-input/ready`. O Flink lê somente
`ready`, evitando o erro `SimpleStreamFormat is not splittable` causado por
arquivos ainda em crescimento.

### Testes básicos

```bash
docker compose config
docker compose up -d --build
docker compose ps
docker compose exec -T flink-jobmanager flink list
docker compose logs --tail=100 flink-file-finalizer
docker compose logs --tail=200 flink-jobmanager
```
