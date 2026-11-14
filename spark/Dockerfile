# Image Spark officielle + bibliothèques Python utiles au projet
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir pandas==2.2.3 pyarrow==17.0.0 matplotlib==3.9.2 pyyaml==6.0.2 \
    elasticsearch==8.17.0

# Connecteur Kafka pour Spark Structured Streaming (intégré à l'image : pas de
# téléchargement --packages au lancement, fonctionne hors ligne, identique sur master et worker)
RUN python3 - <<'PY'
import urllib.request
base = "https://repo1.maven.org/maven2/"
for jar in [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
]:
    urllib.request.urlretrieve(base + jar, "/opt/spark/jars/" + jar.rsplit("/", 1)[1])
    print("ok", jar)
PY

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
WORKDIR /opt/project
