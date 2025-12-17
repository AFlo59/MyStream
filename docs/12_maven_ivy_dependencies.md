# Gestion des Dépendances Spark : Maven et Ivy

## Vue d'ensemble

Apache Spark utilise un système de gestion de dépendances basé sur **Maven** et **Ivy** pour télécharger et gérer les packages Java (JARs) nécessaires à son fonctionnement.

## Qu'est-ce que Maven ?

**Maven** est un outil de gestion de projet et de dépendances pour les projets Java. Il utilise un système de **repositories** (dépôts) centralisés pour stocker et distribuer les artefacts (JARs, bibliothèques).

### Concepts clés Maven

1. **Repository Central** : Dépôt public hébergé par Maven (`repo1.maven.org`)
2. **Coordonnées Maven** : Identifiant unique d'un package
   - Format : `groupId:artifactId:version`
   - Exemple : `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0`
3. **Dépendances transitives** : Maven résout automatiquement les dépendances des dépendances

## Qu'est-ce qu'Ivy ?

**Ivy** est un gestionnaire de dépendances qui peut utiliser les repositories Maven. Spark utilise Ivy pour télécharger et mettre en cache les packages spécifiés via `spark.jars.packages`.

### Comment Spark utilise Ivy

1. **Configuration** : Vous spécifiez les packages dans la configuration Spark :
   ```python
   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
   ```

2. **Téléchargement** : Lors de la première utilisation, Spark/Ivy télécharge le package depuis Maven Central

3. **Cache** : Les packages téléchargés sont mis en cache dans `~/.ivy2/jars/` pour éviter les téléchargements répétés

## Format des Coordonnées Maven

### Structure complète

```
groupId:artifactId:version
```

### Exemple : spark-sql-kafka

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
│              │                        │   │
│              │                        │   └─ Version (3.4.0)
│              │                        └───── Scala version (2.12)
│              └────────────────────────────── Artifact ID (spark-sql-kafka-0-10)
└───────────────────────────────────────────── Group ID (org.apache.spark)
```

### Détails des composants

- **Group ID** (`org.apache.spark`) : Organisation ou projet qui développe le package
- **Artifact ID** (`spark-sql-kafka-0-10_2.12`) : Nom du package
  - `spark-sql-kafka` : Package Kafka pour Spark SQL
  - `0-10` : Version de l'API Kafka (0.10.x)
  - `2.12` : Version de Scala compilée (Scala 2.12)
- **Version** (`3.4.0`) : Version de Spark

## Utilisation dans notre Projet

### Configuration dans le Notebook Silver

```python
builder = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .master("local[*]")
```

### Téléchargement automatique

Lors de la première utilisation de `readStream.format("kafka")`, Spark :
1. Détecte que le package n'est pas présent
2. Télécharge le package depuis Maven Central via Ivy
3. Met en cache le package dans `/root/.ivy2/jars/`
4. Charge le package dans le classpath Spark

### Pré-téléchargement dans le Dockerfile

Pour éviter les problèmes de téléchargement à l'exécution, nous pré-téléchargeons le package lors de la construction de l'image :

```dockerfile
RUN mkdir -p /root/.ivy2/jars && \
    wget -q --no-check-certificate \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar \
    -O /root/.ivy2/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar
```

## Cache Ivy

### Emplacement du cache

- **Linux/Docker** : `/root/.ivy2/jars/` ou `~/.ivy2/jars/`
- **Windows** : `C:\Users\<username>\.ivy2\jars\`

### Structure du cache

```
~/.ivy2/
├── cache/          # Cache temporaire
├── jars/           # JARs téléchargés (utilisé par Spark)
└── local/          # Repository local
```

### Avantages du cache

- ✅ Évite les téléchargements répétés
- ✅ Fonctionne hors ligne une fois téléchargé
- ✅ Accélère le démarrage de Spark

## Résolution des Dépendances

### Dépendances transitives

Quand vous spécifiez `spark-sql-kafka-0-10_2.12:3.4.0`, Ivy télécharge aussi :
- `kafka-clients-3.4.0.jar` (dépendance de spark-sql-kafka)
- Autres dépendances nécessaires

### Gestion des conflits

Si plusieurs packages nécessitent des versions différentes d'une même dépendance, Ivy choisit généralement la version la plus récente compatible.

## Problèmes Courants et Solutions

### 1. Téléchargement lent ou échec

**Symptôme** : `Failed to find data source: kafka` après plusieurs tentatives

**Solutions** :
- Vérifier la connexion Internet
- Pré-télécharger le package dans le Dockerfile (comme nous l'avons fait)
- Utiliser un repository Maven local ou proxy

### 2. Version incompatible

**Symptôme** : Erreurs de compatibilité entre versions

**Solution** : Vérifier que la version du package correspond à la version de Spark :
- Spark 3.4.0 → `spark-sql-kafka-0-10_2.12:3.4.0`
- Spark 3.3.0 → `spark-sql-kafka-0-10_2.12:3.3.0`

### 3. Cache corrompu

**Symptôme** : Erreurs étranges malgré un package téléchargé

**Solution** : Nettoyer le cache Ivy :
```bash
rm -rf ~/.ivy2/cache
rm -rf ~/.ivy2/jars/*
```

## Autres Packages Spark Disponibles

### Packages courants

- **Delta Lake** : `io.delta:delta-core_2.12:2.4.0`
- **Kafka** : `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0`
- **Avro** : `org.apache.spark:spark-avro_2.12:3.4.0`
- **MongoDB** : `org.mongodb.spark:mongo-spark-connector_2.12:3.0.1`

### Format pour plusieurs packages

```python
.config("spark.jars.packages", 
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
    "io.delta:delta-core_2.12:2.4.0"
)
```

## Références

- [Documentation Spark - Packages](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management)
- [Maven Central Repository](https://repo1.maven.org/maven2/)
- [Ivy Documentation](https://ant.apache.org/ivy/)

## Résumé

| Concept | Description |
|---------|-------------|
| **Maven** | Système de gestion de dépendances Java |
| **Ivy** | Gestionnaire de dépendances utilisé par Spark |
| **Repository** | Dépôt centralisé (Maven Central) |
| **Coordonnées** | `groupId:artifactId:version` |
| **Cache** | `~/.ivy2/jars/` pour éviter les téléchargements répétés |
| **Téléchargement** | Automatique lors de la première utilisation |
