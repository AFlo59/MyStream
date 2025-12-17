#!/bin/bash
# Script pour créer le topic Kafka pour les données IoT

KAFKA_TOPIC=${KAFKA_TOPIC_IOT:-"sensor-data-iot"}
KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVERS:-"localhost:9092"}
PARTITIONS=${KAFKA_PARTITIONS:-3}
REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-1}

# Charger les variables depuis .env si disponible
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    KAFKA_TOPIC=${KAFKA_TOPIC_IOT:-$KAFKA_TOPIC}
    KAFKA_BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVERS:-$KAFKA_BOOTSTRAP_SERVER}
fi

echo "📝 Création du topic Kafka : $KAFKA_TOPIC"
echo "   Broker : $KAFKA_BOOTSTRAP_SERVER"
echo "   Partitions : $PARTITIONS"
echo "   Réplication : $REPLICATION_FACTOR"

docker exec -it kafka kafka-topics \
    --create \
    --topic "$KAFKA_TOPIC" \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists

if [ $? -eq 0 ]; then
    echo "✅ Topic créé avec succès"
    echo ""
    echo "📊 Vérification du topic :"
    docker exec -it kafka kafka-topics \
        --describe \
        --topic "$KAFKA_TOPIC" \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER"
else
    echo "❌ Erreur lors de la création du topic"
    exit 1
fi
