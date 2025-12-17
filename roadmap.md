# Roadmap - Projet SmartTech : Streaming de Données IoT

## Vue d'ensemble du projet

**Contexte** : SmartTech traite en continu des données IoT provenant de capteurs installés dans des bâtiments intelligents (température, humidité, consommation d'énergie, détection d'anomalies).

**Objectifs** :
- Détecter rapidement des comportements anormaux
- Alimenter des tableaux de bord en temps réel
- Historiser les données pour l'analyse

**Stack technique** : Apache Spark Structured Streaming, Apache Kafka, Delta Lake (on premise)

---

## Partie 1 : Veille et Recherche (1 jour)

### 📚 Objectif
Réaliser une veille approfondie sur les concepts fondamentaux du streaming avec Spark Structured Streaming et préparer une synthèse structurée.

### Tâches

- [x] **1.1 Concepts fondamentaux du streaming structuré**
  - [x] Comprendre le principe du streaming structuré
  - [x] Étudier la différence entre batch et streaming processing
  - [x] Analyser le modèle de programmation unifié (Dataset/DataFrame API)

- [x] **1.2 Lecture et écriture de flux**
  - [x] Explorer les sources supportées (Kafka, fichiers, socket, etc.)
  - [x] Comprendre les sinks disponibles (Delta Lake, fichiers, console, etc.)
  - [x] Étudier les options de configuration pour chaque source/sink

- [x] **1.3 Modes de sortie (Output Modes)**
  - [x] Append mode : ajout de nouvelles lignes uniquement
  - [x] Update mode : mise à jour des lignes modifiées
  - [x] Complete mode : réécriture complète de la table de sortie
  - [x] Comprendre les cas d'usage de chaque mode

- [x] **1.4 Tolérance aux pannes et checkpointing**
  - [x] Comprendre le mécanisme de checkpointing
  - [x] Étudier les Write-Ahead Logs (WAL)
  - [x] Analyser les garanties exactly-once vs at-least-once
  - [x] Configurer les chemins de checkpoint

- [x] **1.5 Triggers**
  - [x] ProcessingTime trigger : traitement périodique
  - [x] Once trigger : traitement unique
  - [x] Continuous trigger : traitement continu (low-latency)
  - [x] Choisir le trigger adapté selon les besoins de latence

- [x] **1.6 Fenêtres temporelles (Windowing)**
  - [x] Comprendre le concept de windowing
  - [x] Étudier les watermarks pour gérer les données tardives
  - [x] Analyser les agrégations sur fenêtres glissantes/fixées
  - [x] Cas d'usage IoT : agrégations par fenêtre temporelle

- [x] **1.7 Architecture Médaillon (Medallion Architecture)**
  - [x] **Bronze** : données brutes ingérées
  - [x] **Silver** : données nettoyées et validées
  - [x] **Gold** : données agrégées et optimisées pour l'analyse
  - [x] Comprendre le flux de données entre les niveaux
  - [x] Adapter cette architecture au cas SmartTech

- [x] **1.8 Synthèse et documentation**
  - [x] Rédiger une synthèse claire et structurée (PDF)
  - [x] Expliquer l'utilité de chaque concept dans le contexte IoT SmartTech
  - [x] Inclure des schémas/diagrammes pour illustrer les concepts
  - [x] Préparer la présentation orale

---

## Partie 2 : Mise en pratique - Pipeline de streaming (2 jours)

### 🏗️ Objectif
Implémenter deux pipelines de streaming progressives pour traiter les données IoT de SmartTech.

---

### Phase 2.1 : Pipeline simple - JSON vers Delta Lake Bronze (Jour 1)

#### Objectif
Implémenter une première pipeline permettant de lire un flux JSON, appliquer des transformations basiques et écrire dans Delta Lake (niveau Bronze).

#### Tâches

- [x] **2.1.1 Configuration de l'environnement**
  - [x] Installer et configurer Apache Spark (Docker)
  - [x] Installer Delta Lake (dans Dockerfile)
  - [x] Préparer l'environnement de développement (Jupyter/PySpark)
  - [x] Créer la structure de répertoires (checkpoints, données)

- [x] **2.1.2 Préparation des données de test**
  - [x] Créer ou télécharger un jeu de données IoT (sensor_data)
  - [x] Structurer les données JSON avec les champs pertinents :
    - `sensor_id`, `timestamp`, `temperature`, `humidity`, `energy_consumption`, `anomaly_detected`, etc.
  - [x] Simuler un flux continu de fichiers JSON (script `scripts/generate_sensor_data.py`)

- [x] **2.1.3 Lecture du flux JSON**
  - [x] Configurer la source de lecture (format JSON)
  - [x] Définir le schéma des données
  - [x] Configurer les options de lecture (maxFilesPerTrigger, etc.)

- [x] **2.1.4 Transformations basiques**
  - [x] Nettoyage des données :
    - Gérer les valeurs nulles
    - Valider les plages de valeurs (température, humidité)
  - [x] Filtrage :
    - Filtrer les données invalides
    - Optionnel : filtrer par type de capteur
  - [x] Projection :
    - Sélectionner les colonnes pertinentes
    - Ajouter des colonnes calculées si nécessaire

- [x] **2.1.5 Écriture dans Delta Lake (Bronze)**
  - [x] Configurer l'écriture vers Delta Lake
  - [x] Définir le chemin de stockage (niveau Bronze)
  - [x] Configurer le mode d'écriture (append)
  - [x] Gérer les partitions si nécessaire

- [x] **2.1.6 Configuration de la tolérance aux pannes**
  - [x] Définir un chemin de checkpoint dédié
  - [x] Configurer les options de checkpointing
  - [x] Tester la récupération après une panne simulée

- [x] **2.1.7 Notebook de démonstration**
  - [x] Créer un notebook structuré et documenté
  - [x] Inclure des explications pour chaque étape
  - [x] Ajouter des visualisations des données si pertinent
  - [x] Tester l'exécution complète de la pipeline

---

### Phase 2.2 : Pipeline avancée avec Kafka (Jour 2)

#### Objectif
Mettre en place un flux de données continu avec Kafka, consommer avec Spark Structured Streaming, transformer et écrire dans Delta Lake (niveau Silver).

#### Tâches

- [x] **2.2.1 Installation et configuration de Kafka**
  - [x] Installer Apache Kafka (on premise)
  - [x] Démarrer Zookeeper et Kafka broker
  - [x] Créer un topic dédié pour les données IoT
  - [x] Configurer les partitions et la réplication
  - [x] Vérifier la connectivité Kafka

- [x] **2.2.2 Comprendre les concepts Kafka**
  - [x] **Offsets** : comprendre le mécanisme de suivi de position dans le topic
  - [x] **Partitions** : comprendre la distribution des données et le parallélisme
  - [x] **Consumer Groups** : comprendre la consommation distribuée et le rééquilibrage
  - [x] Documenter l'intérêt d'un message broker dans une architecture temps réel

- [x] **2.2.3 Développement du simulateur de capteurs (Producer)**
  - [x] Créer un script Python pour produire des messages dans Kafka
  - [x] Simuler des données IoT réalistes :
    - Générer des mesures de température, humidité, consommation d'énergie
    - Inclure des timestamps cohérents
    - Optionnel : simuler des anomalies périodiques
  - [x] Configurer le producer Kafka avec les bonnes pratiques
  - [x] Tester la production de messages

- [x] **2.2.4 Configuration de Spark pour Kafka**
  - [x] Ajouter la dépendance `spark-sql-kafka` au projet
  - [x] Configurer la connexion aux brokers Kafka
  - [x] Comprendre les options de configuration Kafka (bootstrap.servers, subscribe, etc.)

- [x] **2.2.5 Lecture du flux Kafka avec Spark**
  - [x] Configurer la source Kafka dans Spark Structured Streaming
  - [x] S'abonner au topic approprié
  - [x] Parser les messages JSON reçus
  - [x] Gérer les offsets (startingOffsets, etc.)

- [x] **2.2.6 Transformations et normalisation**
  - [x] Nettoyer les données (plus approfondi que Bronze)
  - [x] Normaliser les formats de données
  - [x] Valider la cohérence des données
  - [x] Enrichir les données si nécessaire
  - [x] Appliquer des transformations métier spécifiques

- [x] **2.2.7 Écriture dans Delta Lake (Silver)**
  - [x] Configurer l'écriture vers Delta Lake (niveau Silver)
  - [x] Définir le chemin de stockage distinct du Bronze
  - [x] Configurer le mode d'écriture approprié
  - [x] Optimiser les partitions pour les requêtes

- [x] **2.2.8 Configuration avancée**
  - [x] Configurer les checkpoints pour la pipeline Kafka
  - [x] Gérer les offsets Kafka via les checkpoints
  - [x] Configurer les triggers appropriés (ProcessingTime)
  - [x] Tester la tolérance aux pannes

- [x] **2.2.9 Notebook de démonstration**
  - [x] Créer un notebook structuré et documenté
  - [x] Documenter le rôle de Kafka dans l'architecture
  - [x] Expliquer les offsets, partitions, consumer groups
  - [x] Inclure des métriques de monitoring si possible
  - [x] Tester l'exécution complète de la pipeline

- [x] **2.2.10 Documentation et scripts**
  - [x] Documenter le script de production de données (simulateur)
  - [x] Créer un README avec les instructions d'utilisation
  - [x] Documenter les configurations nécessaires
  - [x] Préparer la démonstration orale

---

## Livrables finaux

### Partie 1 : Veille
- [x] Synthèse PDF claire et structurée (docs/*.md)
- [x] Présentation orale préparée

### Partie 2 : Mise en pratique
- [x] Notebook 2.1 : Pipeline Bronze (JSON → Delta Bronze) - Données brutes
- [x] Notebook 2.2 : Pipeline Silver (Kafka → Delta Silver) - Données nettoyées
- [x] Notebook 2.3 : Pipeline Gold (Silver → Delta Gold) - Données agrégées
- [x] Script simulateur de capteurs Kafka (scripts/kafka_sensor_producer.py)
- [x] Architecture Médaillon stricte (Bronze brut → Silver transformé → Gold agrégé)
- [x] README avec instructions d'utilisation
- [x] Présentation orale avec démonstration

---

## Critères de performance

- ✅ Mise en œuvre correcte de Spark Structured Streaming (lecture, écriture, checkpoints)
- ✅ Structuration claire des notebooks et du code
- ✅ Compréhension du rôle d'un message broker dans un système temps réel
- ✅ Documentation complète et professionnelle
- ✅ Démonstration fonctionnelle des pipelines

---

## Ressources de référence

- [Documentation Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Documentation Kafka Integration](https://spark.apache.org/docs/latest/streaming-structured-streaming-kafka-integration.html)
- [Documentation Apache Spark](https://archive.apache.org/dist/spark/docs/)
- [Exemples Spark Streaming](https://github.com/apache/spark/tree/master/examples/src/main/python/streaming)
- [Documentation Delta Lake](https://docs.delta.io/)

---

## Notes importantes

- **Environnement** : On premise (pas de cloud)
- **Format des données** : JSON pour les capteurs IoT
- **Architecture** : Médaillon (Bronze → Silver → Gold)
- **Focus** : Compréhension approfondie des concepts et mise en pratique progressive

