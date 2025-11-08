# ./docker/jupyter/Dockerfile
FROM local/jupyter-spark:3.5.0-aws34-ice161
USER root
ENV SPARK_HOME=/opt/bitnami/spark
# установка jupyterLab и pyspark
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && pip cache purge \
    && rm -rf /var/lib/apt/lists/*

# --- Iceberg + Spark + MinIO (Jupyter) ---
RUN set -eux; \
    # --- Iceberg + Spark + MinIO --- - уже есть в исходном образе local/spark:3.5.0-aws34-ice161
    # curl -fL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
    #   https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    # curl -fL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.517.jar \
    #   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar && \
    # curl -fL -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    #   https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -fL -o $SPARK_HOME/jars/postgresql-42.7.2.jar \
      https://jdbc.postgresql.org/download/postgresql-42.7.2.jar && \
    # --- AWS SDK v2 (нужно для org.apache.iceberg.aws.s3.S3FileIO)
    # Версия AWS SDK v2.25.48 совместима с Iceberg 1.6.1
    curl -fL -o $SPARK_HOME/jars/bundle-2.25.48.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.48/bundle-2.25.48.jar && \
    curl -fL -o $SPARK_HOME/jars/s3-2.25.48.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.48/s3-2.25.48.jar && \
    curl -fL -o $SPARK_HOME/jars/url-connection-client-2.25.48.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.25.48/url-connection-client-2.25.48.jar && \
    curl -fL -o $SPARK_HOME/jars/graphframes-spark3_2.12-0.9.0-spark3.5.jar \
      https://repo1.maven.org/maven2/io/graphframes/graphframes-spark3_2.12/0.9.0-spark3.5/graphframes-spark3_2.12-0.9.0-spark3.5.jar

# вернём права и пользователя
USER 1001
