# Imagen única para todos los roles del clúster (master, workers, history server y driver).
# Base oficial de Apache Spark: Spark 3.5.5 + Scala 2.12 + Java 17 + Python 3.10 (Ubuntu 22.04)
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

# Se ejecuta como root para poder escribir en los bind mounts de Windows/WSL (outputs/, samples/).
USER root

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/project/src \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PROJECT_DIR=/opt/project \
    LAKE_DIR=/opt/lake \
    MPLCONFIGDIR=/tmp/matplotlib \
    PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/entrypoint.sh /usr/local/bin/nyc-entrypoint.sh
RUN chmod +x /usr/local/bin/nyc-entrypoint.sh \
 && mkdir -p /opt/lake/spark-events /opt/project

WORKDIR /opt/project
ENTRYPOINT ["/usr/local/bin/nyc-entrypoint.sh"]
CMD ["bash"]
