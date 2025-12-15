# Configuration des variables d'environnement

## Fichier .env (recommandé)

Un fichier `.env` est **recommandé** pour personnaliser la configuration. Les notebooks utilisent `os.getenv()` pour lire ces variables avec des valeurs par défaut, ce qui permet une grande flexibilité.

**Sans fichier `.env`** : Les notebooks utilisent les valeurs par défaut définies dans le code (`/opt/spark/...`).

**Avec fichier `.env`** : Les variables définies dans `.env` sont injectées automatiquement dans les conteneurs Docker via `docker-compose.yml` et utilisées par les notebooks.

## Création du fichier .env

Pour créer votre fichier `.env`, copiez le fichier `env.example` :

```bash
# Sur Linux/Mac/WSL
cp env.example .env

# Sur Windows PowerShell
Copy-Item env.example .env
```

Vous pouvez ensuite modifier les valeurs selon vos besoins.

## Variables recommandées

Le fichier `.env` devrait contenir les variables suivantes :

```bash
# ============================================
# Chemins de données (optionnel - valeurs par défaut dans Docker)
# ============================================
DATA_DIR=/opt/spark/data
DELTA_BRONZE_PATH=/opt/spark/delta/bronze
DELTA_SILVER_PATH=/opt/spark/delta/silver
CHECKPOINT_BRONZE_PATH=/opt/spark/checkpoints/bronze
CHECKPOINT_SILVER_PATH=/opt/spark/checkpoints/silver

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
DATA_DIR = os.getenv('DATA_DIR', '/opt/spark/data')
DELTA_BRONZE_PATH = os.getenv('DELTA_BRONZE_PATH', '/opt/spark/delta/bronze')
```

## Note importante

Ces variables sont **utilisées** dans les notebooks via `os.getenv()` avec des valeurs par défaut. Elles permettent de :

- ✅ Personnaliser les chemins selon l'environnement
- ✅ Faciliter le déploiement en production
- ✅ Gérer différentes configurations (dev, staging, prod)
- ✅ Utiliser les mêmes notebooks en local et dans Docker

**Dans Docker** : Les variables sont injectées automatiquement depuis le fichier `.env` via `docker-compose.yml` (option `env_file`).

**En local** : Les valeurs par défaut dans le code sont utilisées, ou vous pouvez créer un fichier `.env` et utiliser `python-dotenv` pour le charger.

## Sécurité

⚠️ **Important** : Le fichier `.env` est dans `.gitignore` et ne doit **jamais** être commité dans Git s'il contient des secrets ou des configurations sensibles.

