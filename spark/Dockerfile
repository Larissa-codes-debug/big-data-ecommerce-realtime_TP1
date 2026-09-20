FROM spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir pyspark==3.5.3 happybase==1.3.0 thrift==0.16.0

RUN mkdir -p /opt/spark/jobs

COPY jobs/ /opt/spark/jobs/

WORKDIR /opt/spark

CMD ["tail", "-f", "/dev/null"]
