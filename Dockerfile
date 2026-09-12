FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

# Give the Spark user a real home directory.
RUN mkdir -p /home/spark \
    && chown -R spark:spark /home/spark

ENV HOME=/home/spark
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

# Delta Lake + JupyterLab
RUN pip install --no-cache-dir \
    delta-spark==4.0.0 \
    jupyterlab

# Delta Lake + S3/MinIO dependencies
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
COPY landing /opt/spark/landing

RUN mkdir -p \
        /opt/spark/work-dir \
        /opt/spark/notebooks \
        /home/spark/.local/share/jupyter/runtime \
    && chown -R spark:spark \
        /opt/spark/jobs \
        /opt/spark/configs \
        /opt/spark/landing \
        /opt/spark/work-dir \
        /opt/spark/notebooks \
        /home/spark \
    && chown spark:spark /opt/spark/jars/*.jar

USER spark

ENV HOME=/home/spark
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime

WORKDIR /opt/spark/work-dir
