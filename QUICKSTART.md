# Guide de démarrage rapide - SmartTech Streaming

## Prérequis

- Docker Desktop installé et démarré
- WSL Ubuntu (pour les commandes Linux) ou PowerShell
- Python 3.8+ (pour générer les données de test)

## Démarrage rapide (5 minutes)

### 1. Construire et démarrer l'environnement Docker

**Option A : Avec WSL Ubuntu**
```bash
cd /mnt/c/Users/red59/Documents/MyStream
chmod +x scripts/setup.sh
./scripts/setup.sh
```

**Option B : Avec PowerShell**
```powershell
docker build -t smarttech-spark:latest .
docker-compose up -d
```

### 2. Générer les données de test

```bash
# Avec Python local (Windows)
python scripts/generate_sensor_data.py

# Ou depuis le conteneur Docker
docker exec -it spark-master python3 /opt/bitnami/spark/scripts/generate_sensor_data.py
```

### 3. Démarrer Jupyter Notebook

**Option A : Avec le script**
```bash
chmod +x scripts/start_jupyter.sh
./scripts/start_jupyter.sh
```

**Option B : Manuellement**
```bash
docker exec -it spark-master jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=/opt/bitnami/spark/notebooks
```

### 4. Accéder aux interfaces

- **Jupyter Notebook** : http://localhost:8888
  - Le token d'authentification s'affiche dans les logs du conteneur
- **Spark Master UI** : http://localhost:8080

### 5. Exécuter la pipeline Bronze

1. Ouvrir le notebook `01_pipeline_bronze.ipynb` dans Jupyter
2. Exécuter les cellules dans l'ordre
3. Vérifier les données dans Delta Lake Bronze

## Structure des données

Les données générées sont au format JSON avec la structure suivante :

```json
{
  "sensor_id": "sensor_001",
  "timestamp": "2025-12-15T10:30:00",
  "temperature": 22.5,
  "humidity": 45.2,
  "energy_consumption": 350.0,
  "anomaly_detected": false,
  "building_id": "building_1",
  "sensor_type": "temperature",
  "location": "room_101"
}
```

## Chemins importants (dans le conteneur Docker)

- **Données d'entrée** : `/opt/bitnami/spark/data`
- **Delta Lake Bronze** : `/opt/bitnami/spark/delta/bronze`
- **Delta Lake Silver** : `/opt/bitnami/spark/delta/silver`
- **Checkpoints** : `/opt/bitnami/spark/checkpoints`

## Commandes utiles

### Vérifier le statut des conteneurs
```bash
docker-compose ps
```

### Voir les logs Spark
```bash
docker-compose logs -f spark-master
```

### Arrêter les services
```bash
docker-compose down
```

### Redémarrer les services
```bash
docker-compose restart
```

### Accéder au shell du conteneur Spark
```bash
docker exec -it spark-master bash
```

### Vérifier les données Delta Lake
```bash
docker exec -it spark-master spark-sql \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

## Dépannage

### Les conteneurs ne démarrent pas
- Vérifier que Docker Desktop est démarré
- Vérifier les ports 8080, 7077, 8888 ne sont pas déjà utilisés

### Erreur "Delta Lake not found"
- Vérifier que l'image Docker a été construite avec le Dockerfile
- Reconstruire l'image : `docker build -t smarttech-spark:latest .`

### Pas de données dans Delta Lake
- Vérifier que les fichiers JSON sont dans le dossier `data/`
- Vérifier que la pipeline est bien démarrée dans le notebook
- Consulter les logs : `docker-compose logs spark-master`

## Prochaines étapes

Une fois la pipeline Bronze fonctionnelle, vous pouvez passer à la Phase 2.2 :
- Installation de Kafka
- Pipeline Silver avec Kafka → Delta Lake
- Voir `roadmap.md` pour les détails

