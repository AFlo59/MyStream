# Synthèse de la veille - Streaming avec Spark Structured Streaming

## Vue d'ensemble

Ce document synthétise les concepts fondamentaux du streaming avec Spark Structured Streaming dans le contexte du projet SmartTech. Il présente une vue d'ensemble des concepts étudiés et leur application pratique.

## Résumé des concepts clés

### 1. Streaming structuré : Un modèle unifié

**Concept principal** : Structured Streaming permet de traiter des flux de données avec la même API que le batch processing, simplifiant ainsi le développement et la maintenance.

**Application SmartTech** :
- Utilisation de l'API DataFrame/Dataset familière
- Traitement continu des données IoT sans réécrire le code batch
- Courbe d'apprentissage réduite pour l'équipe

**Référence** : [docs/01_concepts_fondamentaux_streaming.md](01_concepts_fondamentaux_streaming.md)

### 2. Sources et sinks : Flexibilité d'intégration

**Concept principal** : Support de nombreuses sources (fichiers, Kafka, socket) et sinks (Delta Lake, fichiers, console) pour s'adapter à différents besoins.

**Application SmartTech** :
- **Phase 2.1** : Fichiers JSON → Delta Lake Bronze (simplicité)
- **Phase 2.2** : Kafka → Delta Lake Silver (performance et scalabilité)

**Référence** : [docs/02_lecture_ecriture_flux.md](02_lecture_ecriture_flux.md)

### 3. Modes de sortie : Adapter le comportement

**Concept principal** : Trois modes (Append, Update, Complete) pour différents cas d'usage selon les besoins de mise à jour.

**Application SmartTech** :
- **Bronze** : Append (ingestion brute)
- **Silver** : Update (données nettoyées avec corrections possibles)
- **Gold** : Complete (agrégations globales)

**Référence** : [docs/03_modes_sortie.md](03_modes_sortie.md)

### 4. Tolérance aux pannes : Fiabilité garantie

**Concept principal** : Checkpointing et Write-Ahead Logs garantissent exactly-once processing, aucune perte de données.

**Application SmartTech** :
- Checkpoints dédiés par pipeline (`/checkpoints/bronze`, `/checkpoints/silver`)
- Reprise automatique après panne
- Gestion automatique des offsets Kafka

**Référence** : [docs/04_tolerance_pannes_checkpointing.md](04_tolerance_pannes_checkpointing.md)

### 5. Triggers : Contrôler la latence

**Concept principal** : Trois types de triggers (ProcessingTime, Once, Continuous) pour contrôler quand les données sont traitées.

**Application SmartTech** :
- **Bronze** : ProcessingTime 10s (fiabilité > latence)
- **Silver** : ProcessingTime 5s (latence réduite pour dashboards)
- **Anomalies** : Continuous 1s (latence minimale pour alertes)

**Référence** : [docs/05_triggers.md](05_triggers.md)

### 6. Fenêtres temporelles : Analyser dans le temps

**Concept principal** : Windowing et watermarks permettent d'agréger des données sur des périodes de temps et de gérer les données tardives.

**Application SmartTech** :
- Température moyenne par capteur (fenêtre de 5 minutes)
- Consommation énergétique horaire par bâtiment
- Détection d'anomalies sur fenêtres glissantes

**Référence** : [docs/06_fenetres_temporelles.md](06_fenetres_temporelles.md)

### 7. Architecture Médaillon : Organisation des données

**Concept principal** : Trois niveaux (Bronze, Silver, Gold) pour organiser les données selon leur qualité et leur usage.

**Application SmartTech** :
- **Bronze** : Données brutes ingérées (Phase 2.1)
- **Silver** : Données nettoyées et normalisées (Phase 2.2)
- **Gold** : Données agrégées pour l'analyse (futur)

**Référence** : [docs/07_architecture_medaillon.md](07_architecture_medaillon.md)

## Architecture proposée pour SmartTech

```
┌─────────────────────────────────────────────────────────┐
│                    SOURCES DE DONNÉES                    │
│  ┌──────────────┐              ┌──────────────┐        │
│  │  Fichiers    │              │    Kafka     │        │
│  │    JSON      │              │   (Phase 2.2)│        │
│  └──────┬───────┘              └──────┬───────┘        │
└─────────┼──────────────────────────────┼───────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                  │
│  ┌──────────────────────────────────────────────┐      │
│  │  Pipeline Bronze (Phase 2.1)                 │      │
│  │  - Lecture JSON                              │      │
│  │  - Transformations basiques                  │      │
│  │  - Mode: Append                              │      │
│  │  - Trigger: 10s                             │      │
│  └──────────────────┬───────────────────────────┘      │
│                     │                                    │
│  ┌──────────────────▼───────────────────────────┐      │
│  │  Pipeline Silver (Phase 2.2)                │      │
│  │  - Lecture Kafka                             │      │
│  │  - Nettoyage approfondi                     │      │
│  │  - Mode: Update                              │      │
│  │  - Trigger: 5s                              │      │
│  └──────────────────┬──────────────────────────┘      │
└───────────────────────┼─────────────────────────────────┘
                        │
          ┌─────────────┴─────────────┐
          ▼                           ▼
┌──────────────────┐        ┌──────────────────┐
│  DELTA LAKE      │        │  DELTA LAKE      │
│     BRONZE       │        │     SILVER      │
│  (Données brutes)│        │ (Données propres)│
└──────────────────┘        └──────────────────┘
```

## Utilité dans le contexte IoT SmartTech

### Détection rapide d'anomalies

**Problème** : Les capteurs IoT génèrent des données en continu, nécessitant une détection en temps réel.

**Solution** :
- Streaming continu avec latence faible (5-10 secondes)
- Fenêtres temporelles pour détecter les patterns anormaux
- Watermarks pour gérer les données tardives

**Bénéfice** : Détection immédiate des problèmes (température excessive, consommation anormale)

### Tableaux de bord en temps réel

**Problème** : Les utilisateurs ont besoin de visualisations à jour.

**Solution** :
- Pipeline Silver avec Update mode
- Trigger fréquent (5 secondes)
- Données nettoyées prêtes pour la consommation

**Bénéfice** : Dashboards toujours à jour avec données de qualité

### Historisation efficace

**Problème** : Stocker toutes les données IoT sans perte, de manière efficace.

**Solution** :
- Architecture Médaillon avec Delta Lake
- Bronze pour l'historique complet
- Silver pour les données analysables
- Gold pour les agrégations (futur)

**Bénéfice** : Historique complet préservé, optimisé pour les requêtes

## Points clés à retenir

### 1. Simplicité

- Même API que le batch processing
- Pas besoin de réapprendre un nouveau framework
- Code réutilisable entre batch et streaming

### 2. Fiabilité

- Exactly-once processing garanti
- Tolérance aux pannes intégrée
- Aucune perte de données

### 3. Performance

- Traitement distribué par Spark
- Optimisations automatiques (adaptive execution)
- Latence configurable selon les besoins

### 4. Flexibilité

- Support de nombreuses sources et sinks
- Architecture évolutive (Médaillon)
- Adaptable aux besoins changeants

## Recommandations pour l'implémentation

### Phase 2.1 (Bronze)

1. ✅ Commencer simple : Fichiers JSON
2. ✅ Mode Append pour l'ingestion
3. ✅ Checkpointing obligatoire
4. ✅ Trigger 10s pour équilibrer latence/fiabilité

### Phase 2.2 (Silver)

1. ✅ Intégrer Kafka pour la scalabilité
2. ✅ Mode Update pour les corrections
3. ✅ Nettoyage approfondi des données
4. ✅ Trigger 5s pour latence réduite

### Phase 3 (Gold - futur)

1. 🔮 Agrégations temporelles (horaires, quotidiennes)
2. 🔮 Optimisations pour les requêtes analytiques
3. 🔮 Mode Complete pour les agrégations globales
4. 🔮 Schémas dénormalisés pour performance

## Conclusion

Spark Structured Streaming offre une solution complète et robuste pour le traitement de données IoT en streaming. L'architecture Médaillon permet d'organiser les données de manière progressive, de la réception brute (Bronze) aux données analytiques optimisées (Gold).

Les concepts étudiés (sources/sinks, modes de sortie, checkpointing, triggers, windowing, architecture Médaillon) sont tous applicables et complémentaires dans le contexte SmartTech, permettant de construire une solution scalable et fiable pour traiter les données IoT en temps réel.

## Références complètes

1. [Concepts fondamentaux](01_concepts_fondamentaux_streaming.md)
2. [Lecture et écriture de flux](02_lecture_ecriture_flux.md)
3. [Modes de sortie](03_modes_sortie.md)
4. [Tolérance aux pannes](04_tolerance_pannes_checkpointing.md)
5. [Triggers](05_triggers.md)
6. [Fenêtres temporelles](06_fenetres_temporelles.md)
7. [Architecture Médaillon](07_architecture_medaillon.md)

## Sources principales

- [Apache Spark - Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/streaming-structured-streaming-kafka-integration.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks Documentation](https://docs.databricks.com/)

