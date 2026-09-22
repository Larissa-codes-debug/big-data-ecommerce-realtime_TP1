FROM spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir pyspark==3.5.3

RUN mkdir -p /opt/spark/jobs
RUN mkdir -p /opt/spark/conf

COPY jobs/ /opt/spark/jobs/

WORKDIR /opt/spark

CMD ["tail", "-f", "/dev/null"]