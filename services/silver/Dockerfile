FROM apache/spark:4.0.1-python3

# Kafka source and the S3A connector (Garage). Resolved once at build time into
# a local Ivy cache so containers start without touching Maven.
ENV SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1 \
    SPARK_IVY=/opt/ivy

USER root
RUN mkdir -p /opt/ivy /app && chown -R spark:spark /opt/ivy /app
USER spark
WORKDIR /app

RUN echo 'print("deps resolved")' > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --master local[1] --conf spark.jars.ivy=$SPARK_IVY --packages $SPARK_PACKAGES /tmp/noop.py \
 && rm /tmp/noop.py

COPY --chown=spark:spark src ./src
COPY --chown=spark:spark entrypoint.sh ./entrypoint.sh
RUN cd src && python3 -m zipfile -c /app/silver.zip silver

ENTRYPOINT ["/app/entrypoint.sh"]
