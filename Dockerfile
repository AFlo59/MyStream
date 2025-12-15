# Dockerfile personnalisé pour Spark avec Delta Lake et Jupyter
FROM apache/spark-py:latest

USER root

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Installer Jupyter et les packages Python nécessaires
RUN pip3 install --no-cache-dir \
    jupyter \
    delta-spark \
    kafka-python \
    findspark \
    python-dotenv

# Créer les répertoires nécessaires
RUN mkdir -p /opt/spark/data \
    /opt/spark/checkpoints \
    /opt/spark/notebooks \
    /opt/spark/scripts \
    /opt/spark/delta

# Configurer les variables d'environnement pour Delta Lake
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# SPARK_HOME est déjà défini dans l'image apache/spark-py

# Exposer les ports
EXPOSE 8080 7077 8888

WORKDIR /opt/spark

