FROM flink:1.18

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.18.1

RUN wget -P /opt/flink/lib \
https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.18.1/flink-avro-1.18.1.jar

USER flink

COPY flink_job.py /opt/flink/job/flink_job.py