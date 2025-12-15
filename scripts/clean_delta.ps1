# Script PowerShell pour nettoyer les données Delta Lake et les checkpoints

Write-Host "🧹 Nettoyage des données Delta Lake et checkpoints..." -ForegroundColor Yellow

# Chemins par défaut (peuvent être surchargés par variables d'environnement)
$DELTA_BRONZE_PATH = if ($env:DELTA_BRONZE_PATH) { $env:DELTA_BRONZE_PATH } else { "./delta/bronze" }
$CHECKPOINT_PATH = if ($env:CHECKPOINT_BRONZE_PATH) { $env:CHECKPOINT_BRONZE_PATH } else { "./checkpoints/bronze" }

Write-Host "⚠️  ATTENTION : Cette opération supprime toutes les données existantes !" -ForegroundColor Red
Write-Host "   - Delta Bronze : $DELTA_BRONZE_PATH"
Write-Host "   - Checkpoints : $CHECKPOINT_PATH"

$confirmation = Read-Host "Voulez-vous continuer ? (oui/non)"
if ($confirmation -eq "oui") {
    # Supprimer la table Delta Bronze
    if (Test-Path $DELTA_BRONZE_PATH) {
        Remove-Item -Recurse -Force $DELTA_BRONZE_PATH
        Write-Host "✓ Table Delta Bronze supprimée : $DELTA_BRONZE_PATH" -ForegroundColor Green
    } else {
        Write-Host "ℹ️  Table Delta Bronze n'existe pas encore : $DELTA_BRONZE_PATH" -ForegroundColor Gray
    }
    
    # Supprimer les checkpoints
    if (Test-Path $CHECKPOINT_PATH) {
        Remove-Item -Recurse -Force $CHECKPOINT_PATH
        Write-Host "✓ Checkpoints supprimés : $CHECKPOINT_PATH" -ForegroundColor Green
    } else {
        Write-Host "ℹ️  Checkpoints n'existent pas encore : $CHECKPOINT_PATH" -ForegroundColor Gray
    }
    
    Write-Host ""
    Write-Host "✅ Nettoyage terminé - Vous pouvez repartir de zéro" -ForegroundColor Green
} else {
    Write-Host "❌ Nettoyage annulé" -ForegroundColor Red
}
