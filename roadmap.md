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

- [ ] **1.1 Concepts fondamentaux du streaming structuré**
  - [ ] Comprendre le principe du streaming structuré
  - [ ] Étudier la différence entre batch et streaming processing
  - [ ] Analyser le modèle de programmation unifié (Dataset/DataFrame API)

- [ ] **1.2 Lecture et écriture de flux**
  - [ ] Explorer les sources supportées (Kafka, fichiers, socket, etc.)
  - [ ] Comprendre les sinks disponibles (Delta Lake, fichiers, console, etc.)
  - [ ] Étudier les options de configuration pour chaque source/sink

- [ ] **1.3 Modes de sortie (Output Modes)**
  - [ ] Append mode : ajout de nouvelles lignes uniquement
  - [ ] Update mode : mise à jour des lignes modifiées
  - [ ] Complete mode : réécriture complète de la table de sortie
  - [ ] Comprendre les cas d'usage de chaque mode

- [ ] **1.4 Tolérance aux pannes et checkpointing**
  - [ ] Comprendre le mécanisme de checkpointing
  - [ ] Étudier les Write-Ahead Logs (WAL)
  - [ ] Analyser les garanties exactly-once vs at-least-once
  - [ ] Configurer les chemins de checkpoint

- [ ] **1.5 Triggers**
  - [ ] ProcessingTime trigger : traitement périodique
  - [ ] Once trigger : traitement unique
  - [ ] Continuous trigger : traitement continu (low-latency)
  - [ ] Choisir le trigger adapté selon les besoins de latence

- [ ] **1.6 Fenêtres temporelles (Windowing)**
  - [ ] Comprendre le concept de windowing
  - [ ] Étudier les watermarks pour gérer les données tardives
  - [ ] Analyser les agrégations sur fenêtres glissantes/fixées
  - [ ] Cas d'usage IoT : agrégations par fenêtre temporelle

- [ ] **1.7 Architecture Médaillon (Medallion Architecture)**
  - [ ] **Bronze** : données brutes ingérées
  - [ ] **Silver** : données nettoyées et validées
  - [ ] **Gold** : données agrégées et optimisées pour l'analyse
  - [ ] Comprendre le flux de données entre les niveaux
  - [ ] Adapter cette architecture au cas SmartTech

- [ ] **1.8 Synthèse et documentation**
  - [ ] Rédiger une synthèse claire et structurée (PDF)
  - [ ] Expliquer l'utilité de chaque concept dans le contexte IoT SmartTech
  - [ ] Inclure des schémas/diagrammes pour illustrer les concepts
  - [ ] Préparer la présentation orale

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

- [ ] **2.2.1 Installation et configuration de Kafka**
  - [ ] Installer Apache Kafka (on premise)
  - [ ] Démarrer Zookeeper et Kafka broker
  - [ ] Créer un topic dédié pour les données IoT
  - [ ] Configurer les partitions et la réplication
  - [ ] Vérifier la connectivité Kafka

- [ ] **2.2.2 Comprendre les concepts Kafka**
  - [ ] **Offsets** : comprendre le mécanisme de suivi de position dans le topic
  - [ ] **Partitions** : comprendre la distribution des données et le parallélisme
  - [ ] **Consumer Groups** : comprendre la consommation distribuée et le rééquilibrage
  - [ ] Documenter l'intérêt d'un message broker dans une architecture temps réel

- [ ] **2.2.3 Développement du simulateur de capteurs (Producer)**
  - [ ] Créer un script Python pour produire des messages dans Kafka
  - [ ] Simuler des données IoT réalistes :
    - Générer des mesures de température, humidité, consommation d'énergie
    - Inclure des timestamps cohérents
    - Optionnel : simuler des anomalies périodiques
  - [ ] Configurer le producer Kafka avec les bonnes pratiques
  - [ ] Tester la production de messages

- [ ] **2.2.4 Configuration de Spark pour Kafka**
  - [ ] Ajouter la dépendance `spark-sql-kafka` au projet
  - [ ] Configurer la connexion aux brokers Kafka
  - [ ] Comprendre les options de configuration Kafka (bootstrap.servers, subscribe, etc.)

- [ ] **2.2.5 Lecture du flux Kafka avec Spark**
  - [ ] Configurer la source Kafka dans Spark Structured Streaming
  - [ ] S'abonner au topic approprié
  - [ ] Parser les messages JSON reçus
  - [ ] Gérer les offsets (startingOffsets, etc.)

- [ ] **2.2.6 Transformations et normalisation**
  - [ ] Nettoyer les données (plus approfondi que Bronze)
  - [ ] Normaliser les formats de données
  - [ ] Valider la cohérence des données
  - [ ] Enrichir les données si nécessaire
  - [ ] Appliquer des transformations métier spécifiques

- [ ] **2.2.7 Écriture dans Delta Lake (Silver)**
  - [ ] Configurer l'écriture vers Delta Lake (niveau Silver)
  - [ ] Définir le chemin de stockage distinct du Bronze
  - [ ] Configurer le mode d'écriture approprié
  - [ ] Optimiser les partitions pour les requêtes

- [ ] **2.2.8 Configuration avancée**
  - [ ] Configurer les checkpoints pour la pipeline Kafka
  - [ ] Gérer les offsets Kafka via les checkpoints
  - [ ] Configurer les triggers appropriés (ProcessingTime)
  - [ ] Tester la tolérance aux pannes

- [ ] **2.2.9 Notebook de démonstration**
  - [ ] Créer un notebook structuré et documenté
  - [ ] Documenter le rôle de Kafka dans l'architecture
  - [ ] Expliquer les offsets, partitions, consumer groups
  - [ ] Inclure des métriques de monitoring si possible
  - [ ] Tester l'exécution complète de la pipeline

- [ ] **2.2.10 Documentation et scripts**
  - [ ] Documenter le script de production de données (simulateur)
  - [ ] Créer un README avec les instructions d'utilisation
  - [ ] Documenter les configurations nécessaires
  - [ ] Préparer la démonstration orale

---

## Livrables finaux

### Partie 1 : Veille
- [ ] Synthèse PDF claire et structurée
- [ ] Présentation orale préparée

### Partie 2 : Mise en pratique
- [ ] Notebook 2.1 : Pipeline simple (JSON → Delta Bronze)
- [ ] Notebook 2.2 : Pipeline avancée (Kafka → Delta Silver)
- [ ] Script simulateur de capteurs Kafka
- [ ] README avec instructions d'utilisation
- [ ] Présentation orale avec démonstration

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

