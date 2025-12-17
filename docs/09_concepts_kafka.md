# Concepts Kafka pour SmartTech Streaming

## Vue d'ensemble

Apache Kafka est un système de messagerie distribué conçu pour gérer des flux de données en temps réel à grande échelle. Dans le contexte SmartTech, Kafka sert de **message broker** entre les capteurs IoT (producteurs) et Spark Structured Streaming (consommateur).

## Architecture Kafka

```
Producer (Capteurs IoT)
    ↓
Kafka Broker (Topic: sensor-data-iot)
    ↓
Consumer (Spark Structured Streaming)
```

## Concepts fondamentaux

### 1. Topics

Un **topic** est une catégorie ou un flux de données. Dans SmartTech, nous utilisons le topic `sensor-data-iot` pour stocker toutes les mesures des capteurs.

**Caractéristiques** :
- Les topics sont divisés en **partitions** pour la scalabilité
- Les messages sont stockés de manière persistante
- Les messages sont ordonnés par partition

### 2. Partitions

Les **partitions** permettent de :
- **Distribuer les données** : Chaque partition peut être sur un broker différent
- **Paralléliser le traitement** : Chaque partition peut être traitée indépendamment
- **Améliorer les performances** : Plus de partitions = plus de parallélisme

**Exemple** :
- Topic `sensor-data-iot` avec 3 partitions
- Les messages sont distribués entre les partitions (par clé ou round-robin)
- Spark peut traiter les 3 partitions en parallèle

**Dans SmartTech** :
- Les capteurs envoient des messages avec `sensor_id` comme clé
- Kafka distribue les messages par `sensor_id` dans les partitions
- Spark traite chaque partition en parallèle

### 3. Offsets

Les **offsets** sont des identifiants uniques pour chaque message dans une partition. Ils permettent de :

- **Suivre la position de lecture** : Savoir où on en est dans chaque partition
- **Reprendre après une panne** : Reprendre exactement où on s'était arrêté
- **Éviter la duplication** : Ne pas relire les messages déjà traités

**Exemple** :
```
Partition 0: [msg0, msg1, msg2, msg3, ...]
             offset: 0   1    2    3
             
Si Spark a traité jusqu'à l'offset 2, il reprendra à l'offset 3
```

**Dans SmartTech** :
- Spark utilise les **checkpoints** pour stocker les offsets
- Si la pipeline s'arrête, elle reprend exactement où elle s'était arrêtée
- Aucune duplication de données grâce à la gestion des offsets

### 4. Consumer Groups

Les **consumer groups** permettent de :
- **Coordonner plusieurs consommateurs** : Répartir les partitions entre les consommateurs
- **Scalabilité horizontale** : Ajouter des consommateurs pour augmenter le débit
- **Rééquilibrage automatique** : Redistribuer les partitions si un consommateur s'arrête

**Exemple** :
```
Consumer Group: spark-streaming-consumer
├── Consumer 1 → Partition 0, 1
├── Consumer 2 → Partition 2, 3
└── Consumer 3 → Partition 4, 5
```

**Dans SmartTech** :
- Spark utilise le consumer group `spark-streaming-consumer`
- Les offsets sont suivis par consumer group
- Si on ajoute un autre Spark job avec le même consumer group, les partitions seront réparties

## Intérêt d'un message broker dans une architecture temps réel

### Avantages pour SmartTech

1. **Découplage**
   - Les capteurs (producteurs) et Spark (consommateur) sont indépendants
   - Les capteurs peuvent envoyer des données même si Spark est arrêté
   - Spark peut traiter les données à son propre rythme

2. **Buffering**
   - Les messages sont stockés dans Kafka même si Spark est lent
   - Pas de perte de données si le consommateur est temporairement indisponible
   - Permet de gérer les pics de charge

3. **Scalabilité**
   - Facile d'ajouter des capteurs (producteurs) sans modifier Spark
   - Facile d'ajouter des consommateurs pour augmenter le débit
   - Kafka gère automatiquement la distribution

4. **Tolérance aux pannes**
   - Les messages sont persistés sur disque
   - Les offsets permettent de reprendre après une panne
   - Pas de perte de données grâce à la réplication

5. **Débit élevé**
   - Kafka peut traiter des millions de messages par seconde
   - Idéal pour les flux IoT avec de nombreux capteurs
   - Latence faible (quelques millisecondes)

### Comparaison avec l'approche fichiers (Phase 2.1)

| Aspect | Fichiers JSON | Kafka |
|--------|---------------|-------|
| **Débit** | Limité par le système de fichiers | Très élevé (millions de msg/s) |
| **Latence** | Secondes/minutes | Millisecondes |
| **Buffering** | Non (fichiers supprimés après lecture) | Oui (messages persistés) |
| **Scalabilité** | Limitée | Excellente |
| **Tolérance aux pannes** | Basique | Avancée (offsets, réplication) |
| **Cas d'usage** | Tests, développement | Production, temps réel |

## Configuration dans SmartTech

### Producer (Capteurs IoT)

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',  # Attendre confirmation de tous les replicas
    enable_idempotence=True  # Garantir exactly-once
)
```

### Consumer (Spark Structured Streaming)

```python
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data-iot") \
    .option("kafka.group.id", "spark-streaming-consumer") \
    .option("startingOffsets", "earliest") \
    .load()
```

### Gestion des offsets

Les offsets sont gérés automatiquement via les **checkpoints Spark** :

```python
query.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/silver") \
    .start()
```

**Fonctionnement** :
1. Spark lit les messages depuis Kafka
2. Traite les messages
3. Écrit dans Delta Lake
4. Sauvegarde les offsets dans le checkpoint
5. Si panne, reprend depuis le dernier offset sauvegardé

## Bonnes pratiques

1. **Utiliser des clés pour garantir l'ordre**
   - Utiliser `sensor_id` comme clé pour garantir l'ordre par capteur
   - Les messages avec la même clé vont dans la même partition

2. **Configurer la réplication**
   - Utiliser au moins 3 replicas en production
   - Garantit la disponibilité en cas de panne d'un broker

3. **Gérer les offsets correctement**
   - Toujours utiliser des checkpoints pour gérer les offsets
   - Ne pas utiliser `startingOffsets="latest"` en production (perd les messages)

4. **Surveiller les lag**
   - Surveiller le décalage entre producteurs et consommateurs
   - Si le lag augmente, ajouter des consommateurs

## Sources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/streaming-structured-streaming-kafka-integration.html)
- [Kafka Best Practices](https://kafka.apache.org/documentation/#producerconfigs)
