FROM flink:1.18.1-scala_2.12-java11

USER root
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

COPY flink_job.py /opt/flink/job/flink_job.py

RUN mkdir -p /var/lib/flink-input && chown -R flink:flink /var/lib/flink-input /opt/flink/job
USER flink
