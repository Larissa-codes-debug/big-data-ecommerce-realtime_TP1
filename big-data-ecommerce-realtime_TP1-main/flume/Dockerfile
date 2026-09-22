
FROM eclipse-temurin:8-jre

USER root

ARG HADOOP_VERSION=3.2.1
ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="${FLUME_HOME}/bin:${HADOOP_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl \
       wget \
       bash \
       procps \
       ca-certificates \
       tar \
    && rm -rf /var/lib/apt/lists/*

# Instala o Apache Flume
RUN wget -q \
    https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz \
    && tar -xzf apache-flume-${FLUME_VERSION}-bin.tar.gz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} \
    && rm apache-flume-${FLUME_VERSION}-bin.tar.gz

# Instala as bibliotecas do Hadoop compatíveis com o cluster HDFS
RUN wget -q \
    https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm hadoop-${HADOOP_VERSION}.tar.gz



RUN mkdir -p \
    /var/lib/flume/checkpoint-hdfs \
    /var/lib/flume/data-hdfs \
    /var/lib/flume/checkpoint-flink \
    /var/lib/flume/data-flink \
    /var/lib/flink-input \
    /var/log/myapp

COPY conf/flume.conf ${FLUME_HOME}/conf/flume.conf

WORKDIR ${FLUME_HOME}

# Remove a versão antiga do Guava que entra em conflito com o Hadoop 3.2.1
RUN rm -f ${FLUME_HOME}/lib/guava-11.0.2.jar

CMD ["flume-ng", "agent", "-n", "agent", "-c", "/opt/flume/conf", "-f", "/opt/flume/conf/flume.conf"]