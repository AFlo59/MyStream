# Dockerfile personnalisé pour Spark avec Delta Lake et Jupyter
FROM apache/spark-py:latest

USER root

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements.txt dans l'image
COPY requirements.txt /tmp/requirements.txt

# Installer les packages Python depuis requirements.txt
# Note : pyspark est déjà inclus dans l'image apache/spark-py (Spark 3.4.0)
# et n'est donc pas dans requirements.txt pour éviter les conflits de version
# Delta Lake 2.4.0 est compatible avec Spark 3.4.0
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

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

