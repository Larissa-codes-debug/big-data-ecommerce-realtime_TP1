FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && apt-get install -y wget procps

RUN wget https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz && \
    tar -xzf apache-flume-1.9.0-bin.tar.gz && \
    mv apache-flume-1.9.0-bin /opt/flume && \
    rm apache-flume-1.9.0-bin.tar.gz

ENV PATH="/opt/flume/bin:${PATH}"