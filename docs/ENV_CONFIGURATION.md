# Configuration des variables d'environnement

## Fichier .env (optionnel)

Pour l'instant, un fichier `.env` n'est **pas strictement nécessaire** car les chemins sont codés en dur dans les notebooks pour Docker. Cependant, voici les variables qui pourraient être utiles si vous souhaitez personnaliser la configuration.

## Variables recommandées

Créez un fichier `.env` à la racine du projet avec les variables suivantes :

```bash
# ============================================
# Chemins de données (optionnel - valeurs par défaut dans Docker)
# ============================================
DATA_DIR=/opt/bitnami/spark/data
DELTA_BRONZE_PATH=/opt/bitnami/spark/delta/bronze
DELTA_SILVER_PATH=/opt/bitnami/spark/delta/silver
CHECKPOINT_BRONZE_PATH=/opt/bitnami/spark/checkpoints/bronze
CHECKPOINT_SILVER_PATH=/opt/bitnami/spark/checkpoints/silver

# ============================================
# Configuration Spark (optionnel)
# ============================================
SPARK_APP_NAME=SmartTech-Streaming
SPARK_MASTER=spark://spark-master:7077
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2

# ============================================
# Configuration Kafka (pour Phase 2.2)
# ============================================
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_IOT=sensor-data-iot
KAFKA_CONSUMER_GROUP=spark-streaming-consumer

# ============================================
# Configuration Docker (optionnel)
# ============================================
SPARK_UI_PORT=8080
JUPYTER_PORT=8888
SPARK_MASTER_PORT=7077

# ============================================
# Configuration Jupyter (optionnel)
# ============================================
JUPYTER_TOKEN=
JUPYTER_PASSWORD=
```

## Utilisation dans Python

Pour utiliser ces variables dans vos notebooks :

```python
import os

# Lire une variable avec valeur par défaut
DATA_DIR = os.getenv('DATA_DIR', '/opt/bitnami/spark/data')
DELTA_BRONZE_PATH = os.getenv('DELTA_BRONZE_PATH', '/opt/bitnami/spark/delta/bronze')
```

## Note importante

Pour l'instant, ces variables ne sont **pas utilisées** dans les notebooks car les chemins sont directement définis pour Docker. Elles deviendront utiles si vous voulez :

- Personnaliser les chemins selon l'environnement
- Faciliter le déploiement en production
- Gérer différentes configurations (dev, staging, prod)

## Sécurité

⚠️ **Important** : Le fichier `.env` est dans `.gitignore` et ne doit **jamais** être commité dans Git s'il contient des secrets ou des configurations sensibles.

