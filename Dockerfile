FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir \
    delta-spark==4.0.0

RUN curl -fL \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar \
    -o /opt/spark/jars/delta-spark_2.13-4.0.0.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar \
    -o /opt/spark/jars/delta-storage-4.0.0.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    -o /opt/spark/jars/bundle-2.24.6.jar

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs /opt/spark/jobs
COPY configs /opt/spark/configs
COPY data /opt/spark/data

RUN mkdir -p /opt/spark/work-dir \
    && chown -R spark:spark /opt/spark/jobs /opt/spark/configs /opt/spark/data /opt/spark/work-dir \
    && chown spark:spark /opt/spark/jars/*.jar

USER spark

WORKDIR /opt/spark/work-dir
