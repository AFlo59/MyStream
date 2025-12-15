# Dockerfile personnalisé pour Spark avec Delta Lake et Jupyter
FROM bitnami/spark:latest

USER root

# Installer Python et les dépendances nécessaires
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Installer Jupyter et les packages Python nécessaires
RUN pip3 install --no-cache-dir \
    jupyter \
    pyspark \
    delta-spark \
    kafka-python \
    findspark \
    python-dotenv

# Créer les répertoires nécessaires
RUN mkdir -p /opt/bitnami/spark/data \
    /opt/bitnami/spark/checkpoints \
    /opt/bitnami/spark/notebooks \
    /opt/bitnami/spark/scripts \
    /opt/bitnami/spark/delta

# Configurer les variables d'environnement pour Delta Lake
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Exposer le port Jupyter
EXPOSE 8888

WORKDIR /opt/bitnami/spark

