# Script PowerShell pour créer le topic Kafka pour les données IoT

$KAFKA_TOPIC = if ($env:KAFKA_TOPIC_IOT) { $env:KAFKA_TOPIC_IOT } else { "sensor-data-iot" }
$KAFKA_BOOTSTRAP_SERVER = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "localhost:9092" }
$PARTITIONS = if ($env:KAFKA_PARTITIONS) { $env:KAFKA_PARTITIONS } else { "3" }
$REPLICATION_FACTOR = if ($env:KAFKA_REPLICATION_FACTOR) { $env:KAFKA_REPLICATION_FACTOR } else { "1" }

Write-Host "📝 Création du topic Kafka : $KAFKA_TOPIC" -ForegroundColor Cyan
Write-Host "   Broker : $KAFKA_BOOTSTRAP_SERVER"
Write-Host "   Partitions : $PARTITIONS"
Write-Host "   Réplication : $REPLICATION_FACTOR"

docker exec -it kafka kafka-topics `
    --create `
    --topic "$KAFKA_TOPIC" `
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" `
    --partitions "$PARTITIONS" `
    --replication-factor "$REPLICATION_FACTOR" `
    --if-not-exists

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Topic créé avec succès" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 Vérification du topic :" -ForegroundColor Cyan
    docker exec -it kafka kafka-topics `
        --describe `
        --topic "$KAFKA_TOPIC" `
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER"
} else {
    Write-Host "❌ Erreur lors de la création du topic" -ForegroundColor Red
    exit 1
}
