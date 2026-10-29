# Image Spark officielle + bibliothèques Python utiles au projet
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir pandas==2.2.3 pyarrow==17.0.0 matplotlib==3.9.2 pyyaml==6.0.2

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
WORKDIR /opt/project
