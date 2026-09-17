FROM eclipse-temurin:8-jre-jammy

ARG HBASE_VERSION=2.4.18
RUN apt-get update && apt-get install -y wget procps netcat-openbsd bash && rm -rf /var/lib/apt/lists/*
RUN wget -q https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz \
    && tar -xzf hbase-${HBASE_VERSION}-bin.tar.gz -C /opt \
    && ln -s /opt/hbase-${HBASE_VERSION} /opt/hbase \
    && rm hbase-${HBASE_VERSION}-bin.tar.gz

ENV HBASE_HOME=/opt/hbase
ENV PATH=/opt/hbase/bin:$PATH
COPY hbase-site.xml /opt/hbase/conf/hbase-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 16000 16010 16020 16030 9090
CMD ["/entrypoint.sh"]
