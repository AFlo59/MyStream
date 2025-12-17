#!/bin/bash
# Script d'initialisation pour lancer automatiquement le producteur Kafka
# Ce script attend que Kafka soit prêt avant de lancer le producer
# 
# IMPORTANT : Ce script ne doit PAS utiliser 'set -e' car il est lancé en arrière-plan
# et un échec ne doit pas arrêter le conteneur principal

# Ne pas utiliser set -e pour éviter d'arrêter le conteneur en cas d'erreur
# set -e

echo "🚀 Initialisation du producteur Kafka..."

# Configuration
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
KAFKA_TOPIC_IOT="${KAFKA_TOPIC_IOT:-sensor-data-iot}"
DATA_DIR="${DATA_DIR:-/opt/spark/data}"
SCRIPT_DIR="${SCRIPT_DIR:-/opt/spark/scripts}"
MAX_WAIT_TIME=60  # Temps maximum d'attente en secondes
WAIT_INTERVAL=5   # Intervalle entre les vérifications

# Fonction pour vérifier si Kafka est prêt
wait_for_kafka() {
    echo "⏳ Attente que Kafka soit prêt..."
    local elapsed=0
    
    while [ $elapsed -lt $MAX_WAIT_TIME ]; do
        # Vérifier si Kafka répond sur le port 29092 (interne Docker)
        if nc -z kafka 29092 2>/dev/null || timeout 2 bash -c "cat < /dev/null > /dev/tcp/kafka/29092" 2>/dev/null; then
            echo "✓ Kafka est prêt"
            return 0
        fi
        
        echo "   ⏳ Kafka n'est pas encore prêt... (${elapsed}s/${MAX_WAIT_TIME}s)"
        sleep $WAIT_INTERVAL
        elapsed=$((elapsed + WAIT_INTERVAL))
    done
    
    echo "⚠️  Timeout : Kafka n'est pas prêt après ${MAX_WAIT_TIME} secondes"
    echo "   Le producteur sera lancé quand même..."
    return 1
}

# Attendre que Kafka soit prêt
wait_for_kafka || true

# Attendre quelques secondes supplémentaires pour que le topic soit créé
echo "⏳ Attente de 5 secondes supplémentaires pour la création du topic..."
sleep 5

# Lancer le producteur Kafka en mode continu
echo "🚀 Lancement du producteur Kafka..."
echo "   - Broker : ${KAFKA_BOOTSTRAP_SERVERS}"
echo "   - Topic : ${KAFKA_TOPIC_IOT}"
echo "   - Data Dir : ${DATA_DIR}"
echo "   - Mode : Continu (production infinie)"
echo ""

# Exporter les variables d'environnement pour le script Python
export KAFKA_BOOTSTRAP_SERVERS
export KAFKA_TOPIC_IOT
export DATA_DIR
export CONTINUOUS_MODE=true
export NUM_SENSORS="${NUM_SENSORS:-10}"
export MESSAGES_PER_SENSOR="${MESSAGES_PER_SENSOR:-100}"
export INTERVAL_SECONDS="${INTERVAL_SECONDS:-1.0}"

# Lancer le script Python (sans exec pour permettre le background)
cd "${SCRIPT_DIR}"
python3 kafka_sensor_producer.py
