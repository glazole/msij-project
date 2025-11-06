# ./docker/spark/Dockerfile
FROM docker.io/bitnami/spark:3.5.0
USER root
ENV SPARK_HOME=/opt/bitnami/spark

# Упаковываем совместимые версии (проверено на Spark 3.5.0)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar \
    $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.517.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
# --- Iceberg + MinIO (REST Catalog) совместимые JAR'ы ---
# Iceberg AWS SDK v2 requires these (checked with 1.6.1)
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.48/bundle-2.25.48.jar \
    $SPARK_HOME/jars/bundle-2.25.48.jar
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.48/s3-2.25.48.jar \
    $SPARK_HOME/jars/s3-2.25.48.jar
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.25.48/url-connection-client-2.25.48.jar \
    $SPARK_HOME/jars/url-connection-client-2.25.48.jar

RUN chown -R 1001:0 "$SPARK_HOME/jars"
# вернём права и пользователя
USER 1001

