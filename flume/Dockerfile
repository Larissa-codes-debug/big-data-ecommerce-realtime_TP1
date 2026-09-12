FROM eclipse-temurin:8-jre-jammy

RUN apt-get update && \
    apt-get install -y wget procps bash && \
    rm -rf /var/lib/apt/lists/*

# ==================================================
# APACHE FLUME 1.9.0
# ==================================================

RUN wget https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz && \
    tar -xzf apache-flume-1.9.0-bin.tar.gz && \
    mv apache-flume-1.9.0-bin /opt/flume && \
    rm apache-flume-1.9.0-bin.tar.gz

# ==================================================
# HADOOP 3.2.1
# Mesma versão utilizada pelo HDFS do projeto
# ==================================================

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz && \
    mv hadoop-3.2.1 /opt/hadoop && \
    rm hadoop-3.2.1.tar.gz

# ==================================================
# VARIÁVEIS
# ==================================================

ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

ENV PATH="${FLUME_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

# ==================================================
# CONFIGURAÇÃO DO FLUME
# ==================================================

COPY flume.config /opt/flume/conf/flume.conf

# ==================================================
# DIRETÓRIOS DO FLUME
# ==================================================

RUN mkdir -p \
    /var/lib/flume/checkpoint \
    /var/lib/flume/checkpoint-backup \
    /var/lib/flume/data \
    /var/log/myapp

# ==================================================
# SCRIPT DE INICIALIZAÇÃO
# ==================================================

RUN printf '#!/bin/bash\n\
export FLUME_CLASSPATH="${HADOOP_CONF_DIR}:$(hadoop classpath --glob)"\n\
echo "HADOOP CLASSPATH configurado"\n\
echo "$FLUME_CLASSPATH"\n\
exec flume-ng agent \
--name agent \
--conf /opt/flume/conf \
--conf-file /opt/flume/conf/flume.conf \
-Dflume.root.logger=INFO,console\n' \
> /usr/local/bin/start-flume.sh && \
chmod +x /usr/local/bin/start-flume.sh

# ==================================================
# INICIA O FLUME
# ==================================================

CMD ["/usr/local/bin/start-flume.sh"]