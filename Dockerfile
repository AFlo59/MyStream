# Dockerfile personnalisé pour Spark avec Delta Lake et Jupyter
FROM apache/spark-py:latest

USER root

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    netcat-openbsd \
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

# Copier les scripts dans l'image
COPY scripts/ /opt/spark/scripts/

# Rendre les scripts exécutables
RUN chmod +x /opt/spark/scripts/*.sh 2>/dev/null || true

# Pré-télécharger le package spark-sql-kafka et ses dépendances pour éviter les problèmes de téléchargement à l'exécution
# Télécharger directement les JARs depuis Maven Central
# Copier dans /opt/spark/jars/ (où Spark cherche les JARs) ET dans le cache Ivy
# spark-sql-kafka-0-10_2.12:3.4.0 dépend de kafka-clients:3.3.2
RUN mkdir -p /root/.ivy2/jars /opt/spark/jars && \
    # Télécharger spark-sql-kafka
    wget -q --no-check-certificate https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar -O /tmp/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    # Télécharger kafka-clients (dépendance requise)
    wget -q --no-check-certificate https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar -O /tmp/kafka-clients-3.3.2.jar && \
    # Copier dans /opt/spark/jars/ (où Spark cherche les JARs)
    cp /tmp/spark-sql-kafka-0-10_2.12-3.4.0.jar /opt/spark/jars/ && \
    cp /tmp/kafka-clients-3.3.2.jar /opt/spark/jars/ && \
    # Copier dans le cache Ivy aussi
    cp /tmp/spark-sql-kafka-0-10_2.12-3.4.0.jar /root/.ivy2/jars/ && \
    cp /tmp/kafka-clients-3.3.2.jar /root/.ivy2/jars/ && \
    # Nettoyer
    rm /tmp/spark-sql-kafka-0-10_2.12-3.4.0.jar /tmp/kafka-clients-3.3.2.jar && \
    echo "✓ Packages Kafka pré-téléchargés (spark-sql-kafka + kafka-clients) dans /opt/spark/jars/ et cache Ivy" || echo "⚠️  Échec du téléchargement (sera téléchargé à l'exécution)"

# Configurer les variables d'environnement pour Delta Lake et Kafka
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# SPARK_HOME est déjà défini dans l'image apache/spark-py
# Le package spark-sql-kafka est maintenant pré-téléchargé dans l'image

# Exposer les ports
EXPOSE 8080 7077 8888

WORKDIR /opt/spark

