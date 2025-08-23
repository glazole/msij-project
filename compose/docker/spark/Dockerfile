# ./docker/spark/Dockerfile
FROM bitnami/spark:3.5.0
USER root
ENV SPARK_HOME=/opt/bitnami/spark

# Упаковываем совместимые версии (проверено на Spark 3.5.0)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.517/aws-java-sdk-bundle-1.12.517.jar \
    $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.517.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

RUN chown -R 1001:0 "$SPARK_HOME/jars"
# вернём права и пользователя
USER 1001

