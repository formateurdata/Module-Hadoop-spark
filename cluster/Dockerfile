# Image unique du cluster : Spark 4.1.3 (image officielle) + Hadoop 3.5.0.
# Hadoop 3.5 est la première version qui exige Java 17 côté serveur, celui de l'image Spark.
# L'image officielle apache/hadoop n'existe qu'en amd64 : celle-ci tourne en natif sur
# amd64 et arm64 (Mac Apple Silicon), grâce à la distribution aarch64 publiée par Apache.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

ARG HADOOP_VERSION=3.5.0
ARG TARGETARCH
RUN set -eux; \
    suffix=""; \
    if [ "$TARGETARCH" = "arm64" ]; then suffix="-aarch64"; fi; \
    file="hadoop-${HADOOP_VERSION}${suffix}.tar.gz"; \
    for base in https://dlcdn.apache.org/hadoop/common https://archive.apache.org/dist/hadoop/common; do \
        curl -fsSL "$base/hadoop-${HADOOP_VERSION}/$file" -o /tmp/hadoop.tgz && break; \
    done; \
    curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/$file.sha512" -o /tmp/hadoop.sha512; \
    echo "$(grep -oE '[0-9a-f]{128}' /tmp/hadoop.sha512)  /tmp/hadoop.tgz" | sha512sum -c -; \
    tar -xzf /tmp/hadoop.tgz -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop; \
    rm -rf /tmp/hadoop.tgz /tmp/hadoop.sha512 /opt/hadoop/share/doc; \
    mkdir -p /data /home/spark; \
    chown -R spark:spark /opt/hadoop /data /home/spark

# pandas et pyarrow pour les executors (échanges Arrow avec Python), installés avec uv
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
RUN uv pip install --system --break-system-packages --no-cache \
        pandas==2.3.3 pyarrow==25.0.1

ENV HOME=/home/spark \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/data/logs \
    HADOOP_HEAPSIZE_MAX=512m \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
ENV PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin

COPY conf/hadoop/ $HADOOP_CONF_DIR/
COPY conf/spark/spark-defaults.conf $SPARK_HOME/conf/
COPY demarrer.sh /usr/local/bin/demarrer.sh

USER spark
WORKDIR /cours
ENTRYPOINT ["/usr/local/bin/demarrer.sh"]
