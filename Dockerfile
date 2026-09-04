FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

# Delta Lake Python package
RUN pip install --no-cache-dir \
    delta-spark==4.0.0

# Spark configuration
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Spark jobs
COPY jobs /opt/spark/jobs

# Directories used by Spark
RUN mkdir -p \
        /opt/spark/work-dir \
        /opt/spark/jobs \
    && chown -R spark:spark \
        /opt/spark/work-dir \
        /opt/spark/jobs

USER spark

WORKDIR /opt/spark/work-dir
