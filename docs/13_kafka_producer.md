# Producteur Kafka : Documentation Complète

## Vue d'ensemble

Le producteur Kafka (`scripts/kafka_sensor_producer.py`) est un script Python qui simule un flux continu de données IoT en produisant des messages JSON dans un topic Kafka. Il est conçu pour alimenter la pipeline Silver.

## Architecture

```
Fichiers JSON (data/)
    ↓ (lecture des patterns)
Producteur Kafka (kafka_sensor_producer.py)
    ↓ (production de messages)
Topic Kafka (sensor-data-iot)
    ↓ (consommation)
Pipeline Silver (02_pipeline_silver.ipynb)
```

## Fonctionnalités Principales

### 1. Lecture des Patterns depuis les JSON Existants

Le producteur lit automatiquement les fichiers JSON dans `data/` pour extraire :
- **sensor_ids** : Identifiants uniques des capteurs
- **building_ids** : Identifiants des bâtiments
- **sensor_types** : Types de capteurs (temperature, humidity, co2, etc.)
- **locations** : Emplacements des capteurs
- **Valeurs numériques** : Températures, humidités, consommations énergétiques

Ces patterns sont utilisés pour générer des données réalistes et cohérentes.

### 2. Génération de Données Similaires

Basé sur les patterns extraits, le producteur génère :
- Des données avec les mêmes valeurs de référence
- Des variations réalistes autour de ces valeurs
- Des anomalies périodiques (10% de chance)

### 3. Mode Continu

Le producteur peut fonctionner en deux modes :
- **Mode continu** : Production infinie de messages (par défaut)
- **Mode batch** : Production d'un nombre limité de messages

## Configuration

### Variables d'Environnement

| Variable | Description | Valeur par défaut |
|----------|-------------|-------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Adresse du broker Kafka | `kafka:29092` (Docker) ou `localhost:9092` (local) |
| `KAFKA_TOPIC_IOT` | Nom du topic Kafka | `sensor-data-iot` |
| `DATA_DIR` | Répertoire contenant les fichiers JSON | `/opt/spark/data` |
| `NUM_SENSORS` | Nombre de capteurs à simuler | `10` |
| `MESSAGES_PER_SENSOR` | Nombre de messages par capteur | `100` |
| `INTERVAL_SECONDS` | Intervalle entre les messages (secondes) | `1.0` |
| `CONTINUOUS_MODE` | Mode continu (true/false) | `true` |

### Arguments en Ligne de Commande

```bash
python3 kafka_sensor_producer.py [num_sensors] [messages_per_sensor] [interval_seconds] [continuous_mode]
```

Exemple :
```bash
python3 kafka_sensor_producer.py 20 50 0.5 true
```

## Utilisation

### Lancement Manuel

```bash
# Depuis le conteneur Docker
docker exec -it spark-jupyter python3 /opt/spark/scripts/kafka_sensor_producer.py

# Depuis la machine hôte (si Kafka est accessible)
python3 scripts/kafka_sensor_producer.py
```

### Lancement Automatique

Le producteur se lance automatiquement au démarrage du conteneur `spark-jupyter` via le script `start_kafka_producer.sh`.

## Fonctionnement Détaillé

### 1. Chargement des Patterns

```python
patterns = load_existing_data_patterns(data_dir)
```

Cette fonction :
- Parcourt tous les fichiers `sensor_data_*.json` dans `data/`
- Extrait les valeurs uniques pour chaque champ
- Retourne un dictionnaire avec les patterns

### 2. Génération de Données

```python
sensor_data = generate_sensor_data(sensor_id, timestamp, patterns)
```

La génération utilise :
- Les patterns existants si disponibles
- Des valeurs par défaut réalistes sinon
- Des variations aléatoires pour simuler un flux réel

### 3. Production dans Kafka

```python
producer.send(topic, key=sensor_id, value=sensor_data)
```

**Caractéristiques** :
- **Clé** : `sensor_id` pour garantir l'ordre par capteur
- **Valeur** : JSON sérialisé en bytes UTF-8
- **Acks** : `all` (attendre confirmation de tous les replicas)
- **Idempotence** : Activée pour garantir exactly-once semantics

## Format des Messages

### Structure JSON

```json
{
  "sensor_id": "sensor-temp-001",
  "timestamp": "2025-01-12T08:22:04Z",
  "temperature": 23.5,
  "humidity": 45.2,
  "energy_consumption": 350.0,
  "anomaly_detected": false,
  "building_id": "B",
  "sensor_type": "temperature",
  "location": "floor_1"
}
```

### Champs

| Champ | Type | Description |
|-------|------|-------------|
| `sensor_id` | string | Identifiant unique du capteur |
| `timestamp` | string | Timestamp ISO 8601 en UTC (format `Z`) |
| `temperature` | float | Température en °C (peut être null) |
| `humidity` | float | Humidité en % (peut être null) |
| `energy_consumption` | float | Consommation énergétique en kWh (peut être null) |
| `anomaly_detected` | boolean | Indicateur d'anomalie détectée |
| `building_id` | string | Identifiant du bâtiment |
| `sensor_type` | string | Type de capteur (temperature, humidity, co2, energy) |
| `location` | string | Emplacement du capteur |

## Configuration Kafka Producer

### Paramètres de Fiabilité

```python
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks='all',  # Attendre confirmation de tous les replicas
    retries=3,  # Nombre de tentatives en cas d'échec
    max_in_flight_requests_per_connection=1,  # Garantir l'ordre
    enable_idempotence=True  # Garantir exactly-once semantics
)
```

### Explication des Paramètres

- **`acks='all'`** : Le producer attend la confirmation de tous les replicas avant de considérer l'envoi comme réussi
- **`retries=3`** : En cas d'échec, le producer réessaie jusqu'à 3 fois
- **`max_in_flight_requests_per_connection=1`** : Garantit l'ordre des messages pour un même capteur
- **`enable_idempotence=True`** : Évite les doublons en cas de retry

## Script d'Initialisation

### `start_kafka_producer.sh`

Ce script :
1. Attend que Kafka soit prêt (vérification de connectivité)
2. Attend 5 secondes supplémentaires pour la création du topic
3. Lance le producteur en mode continu

### Intégration dans Docker Compose

Le script est lancé automatiquement au démarrage du conteneur `spark-jupyter` :

```yaml
command: >
  bash -c "
    /opt/spark/scripts/start_kafka_producer.sh &
    jupyter notebook ...
  "
```

## Gestion des Erreurs

### Erreurs de Connexion Kafka

Si Kafka n'est pas accessible :
- Le script affiche un message d'erreur clair
- Il suggère de vérifier que Kafka est démarré
- Il se termine avec un code d'erreur

### Interruption par l'Utilisateur

Si le producteur est interrompu (Ctrl+C) :
- Les messages en attente sont envoyés (`producer.flush()`)
- Le producer est fermé proprement
- Aucune perte de données

## Monitoring

### Logs du Producteur

Le producteur affiche :
- Le nombre de capteurs simulés
- Le nombre de messages par capteur
- La progression de la production
- Les erreurs éventuelles

### Vérification dans Kafka

Pour vérifier que les messages sont produits :

```bash
# Consulter les messages dans le topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor-data-iot \
  --from-beginning
```

## Performance

### Débit

Le débit dépend de :
- `INTERVAL_SECONDS` : Intervalle entre les messages
- `NUM_SENSORS` : Nombre de capteurs parallèles
- `MESSAGES_PER_SENSOR` : Nombre de messages par capteur

**Exemple** :
- 10 capteurs × 1 message/seconde = **10 messages/seconde**
- 20 capteurs × 0.5 message/seconde = **40 messages/seconde**

### Optimisations

Pour augmenter le débit :
- Réduire `INTERVAL_SECONDS`
- Augmenter `NUM_SENSORS`
- Utiliser plusieurs instances du producteur (partitions différentes)

## Cas d'Usage

### 1. Développement et Tests

Le producteur permet de :
- Tester la pipeline Silver sans données réelles
- Générer des données réalistes basées sur les patterns existants
- Simuler différents scénarios (anomalies, volumes élevés)

### 2. Démonstration

Le mode continu permet de :
- Alimenter un dashboard en temps réel
- Démontrer le fonctionnement de la pipeline
- Montrer la détection d'anomalies

### 3. Charge et Performance

En ajustant les paramètres, on peut :
- Simuler une charge élevée
- Tester la scalabilité de la pipeline
- Valider les performances sous charge

## Intégration avec la Pipeline Silver

### Flux de Données

```
1. Producteur Kafka → Topic Kafka
2. Pipeline Silver lit depuis Kafka
3. Transformations et nettoyage
4. Écriture dans Delta Silver
```

### Synchronisation

Le producteur et la pipeline Silver fonctionnent indépendamment :
- Le producteur produit des messages en continu
- La pipeline Silver consomme à son propre rythme
- Les offsets Kafka permettent de reprendre après une panne

## Dépannage

### Le producteur ne démarre pas

**Vérifications** :
1. Kafka est démarré : `docker ps | grep kafka`
2. Le topic existe : `docker exec kafka kafka-topics --list`
3. La connexion fonctionne : `docker exec spark-jupyter nc -z kafka 29092`

### Aucun message n'est produit

**Vérifications** :
1. Vérifier les logs du conteneur : `docker logs spark-jupyter`
2. Vérifier que le script est lancé : `docker exec spark-jupyter ps aux | grep kafka_sensor_producer`
3. Vérifier les variables d'environnement

### Messages dupliqués

**Cause** : Désactivation de l'idempotence ou problème réseau

**Solution** : Vérifier que `enable_idempotence=True` est activé

## Résumé

| Aspect | Description |
|--------|-------------|
| **Rôle** | Simuler un flux continu de données IoT |
| **Source** | Fichiers JSON dans `data/` (patterns) |
| **Destination** | Topic Kafka `sensor-data-iot` |
| **Mode** | Continu (par défaut) ou batch |
| **Format** | JSON avec structure standardisée |
| **Fiabilité** | Exactly-once semantics (idempotence) |
| **Lancement** | Automatique au démarrage du conteneur |

## Références

- [Documentation Kafka Producer](https://kafka.apache.org/documentation/#producerapi)
- [Python Kafka Client](https://kafka-python.readthedocs.io/)
- [Kafka Best Practices](https://kafka.apache.org/documentation/#producerconfigs)
