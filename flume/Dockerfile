FROM eclipse-temurin:8-jre

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       curl \
       wget \
       bash \
       procps \
    && rm -rf /var/lib/apt/lists/*

ENV FLUME_VERSION=1.11.0
ENV FLUME_HOME=/opt/flume
ENV PATH="${FLUME_HOME}/bin:${PATH}"

RUN wget -q https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz \
    && tar -xzf apache-flume-${FLUME_VERSION}-bin.tar.gz -C /opt \
    && mv /opt/apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} \
    && rm apache-flume-${FLUME_VERSION}-bin.tar.gz

RUN mkdir -p \
    /var/lib/flume/checkpoint-hdfs \
    /var/lib/flume/data-hdfs \
    /var/lib/flume/checkpoint-flink \
    /var/lib/flume/data-flink \
    /var/lib/flink-input \
    /var/log/myapp

COPY conf/flume.conf ${FLUME_HOME}/conf/flume.conf

WORKDIR ${FLUME_HOME}

CMD ["flume-ng", "agent", "-n", "agent", "-c", "/opt/flume/conf", "-f", "/opt/flume/conf/flume.conf"]