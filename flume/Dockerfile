FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && \
    apt-get install -y wget procps && \
    rm -rf /var/lib/apt/lists/*

# ==========================
# Apache Flume 1.9.0
# ==========================

RUN wget https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz && \
    tar -xzf apache-flume-1.9.0-bin.tar.gz && \
    mv apache-flume-1.9.0-bin /opt/flume && \
    rm apache-flume-1.9.0-bin.tar.gz

ENV FLUME_HOME=/opt/flume
ENV PATH="${FLUME_HOME}/bin:${PATH}"

# ==========================
# Hadoop 3.2.1
# ==========================

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz && \
    mv hadoop-3.2.1 /opt/hadoop && \
    rm hadoop-3.2.1.tar.gz
    
ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

# ==========================
# Configuração do Flume
# ==========================

COPY flume.config /opt/flume/conf/flume.conf

# ==========================
# Diretórios necessários
# ==========================

RUN mkdir -p \
    /var/lib/flume/checkpoint \
    /var/lib/flume/checkpoint-backup \
    /var/lib/flume/data \
    /var/log/myapp

# ==========================
# Configuração Hadoop
# ==========================

RUN cp /opt/hadoop/etc/hadoop/core-site.xml /opt/flume/conf/ && \
    cp /opt/hadoop/etc/hadoop/hdfs-site.xml /opt/flume/conf/

# ==========================
# Inicia o agente Flume
# ==========================
RUN printf '#!/bin/bash\n\
export FLUME_CLASSPATH="${HADOOP_CONF_DIR}:$(hadoop classpath --glob)"\n\
exec flume-ng agent \
--name agent \
--conf /opt/flume/conf \
--conf-file /opt/flume/conf/flume.conf \
-Dflume.root.logger=INFO,console\n' \
> /usr/local/bin/start-flume.sh && \
chmod +x /usr/local/bin/start-flume.sh

CMD ["/usr/local/bin/start-flume.sh"]