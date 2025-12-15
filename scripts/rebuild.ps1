# Script PowerShell pour rebuild complet de l'environnement Docker

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "SmartTech Streaming - Rebuild Complet" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Arrêter et supprimer les conteneurs et volumes
Write-Host ""
Write-Host "📦 Arrêt et suppression des conteneurs..." -ForegroundColor Yellow
docker-compose down -v

# Supprimer l'image existante si elle existe
Write-Host ""
Write-Host "🗑️  Suppression de l'image existante..." -ForegroundColor Yellow
docker rmi smarttech-spark:latest 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "   (Image n'existe pas encore)" -ForegroundColor Gray
}

# Nettoyer le cache Docker (optionnel mais recommandé)
Write-Host ""
Write-Host "🧹 Nettoyage du cache Docker..." -ForegroundColor Yellow
docker system prune -f

# Rebuild l'image sans cache avec docker build directement
Write-Host ""
Write-Host "🔨 Construction de l'image Docker (sans cache)..." -ForegroundColor Yellow
docker build -t smarttech-spark:latest --no-cache .

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Image construite avec succès" -ForegroundColor Green
} else {
    Write-Host "❌ Erreur lors de la construction de l'image" -ForegroundColor Red
    exit 1
}

# Démarrer les services
Write-Host ""
Write-Host "🚀 Démarrage des services..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Services démarrés" -ForegroundColor Green
} else {
    Write-Host "❌ Erreur lors du démarrage des services" -ForegroundColor Red
    exit 1
}

# Attendre que les services soient prêts
Write-Host ""
Write-Host "⏳ Attente du démarrage complet des services..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Vérifier le statut
Write-Host ""
Write-Host "📊 Statut des services :" -ForegroundColor Yellow
docker-compose ps

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "✅ Rebuild terminé !" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Interfaces disponibles :" -ForegroundColor Cyan
Write-Host "  - Spark Master UI : http://localhost:8080"
Write-Host "  - Spark Worker UI : http://localhost:8081"
Write-Host "  - Jupyter Notebook : http://localhost:8888"
Write-Host ""

