FROM flink:1.18.1-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY flink_job.py /opt/flink/job/flink_job.py
COPY requirements.txt /opt/flink/job/requirements.txt

RUN pip3 install --no-cache-dir -r /opt/flink/job/requirements.txt

RUN mkdir -p /var/lib/flink-input

RUN pip install --no-cache-dir happybase

USER flink