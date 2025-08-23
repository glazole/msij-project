# ./docker/jupyter/Dockerfile
FROM bitnami/spark:3.5.0
USER root
ENV SPARK_HOME=/opt/bitnami/spark
# установка jupyterLab и pyspark
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip curl && \
    pip3 install --no-cache-dir jupyterlab pyspark==3.5.0 \
    && rm -rf /var/lib/apt/lists/*

# Подкладываем те же JAR'ы (как в spark/Dockerfile):
RUN curl -fL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.517.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar && \
    curl -fL -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

# вернём права и пользователя
USER 1001
