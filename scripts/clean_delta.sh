#!/bin/bash
# Script pour nettoyer les données Delta Lake et les checkpoints

echo "🧹 Nettoyage des données Delta Lake et checkpoints..."

# Chemins par défaut (peuvent être surchargés par variables d'environnement)
DELTA_BRONZE_PATH=${DELTA_BRONZE_PATH:-"./delta/bronze"}
CHECKPOINT_PATH=${CHECKPOINT_BRONZE_PATH:-"./checkpoints/bronze"}

echo "⚠️  ATTENTION : Cette opération supprime toutes les données existantes !"
echo "   - Delta Bronze : $DELTA_BRONZE_PATH"
echo "   - Checkpoints : $CHECKPOINT_PATH"

read -p "Voulez-vous continuer ? (oui/non) " -n 3 -r
echo
if [[ $REPLY =~ ^[Oo][Uu][Ii]$ ]]
then
    # Supprimer la table Delta Bronze
    if [ -d "$DELTA_BRONZE_PATH" ]; then
        rm -rf "$DELTA_BRONZE_PATH"
        echo "✓ Table Delta Bronze supprimée : $DELTA_BRONZE_PATH"
    else
        echo "ℹ️  Table Delta Bronze n'existe pas encore : $DELTA_BRONZE_PATH"
    fi
    
    # Supprimer les checkpoints
    if [ -d "$CHECKPOINT_PATH" ]; then
        rm -rf "$CHECKPOINT_PATH"
        echo "✓ Checkpoints supprimés : $CHECKPOINT_PATH"
    else
        echo "ℹ️  Checkpoints n'existent pas encore : $CHECKPOINT_PATH"
    fi
    
    echo ""
    echo "✅ Nettoyage terminé - Vous pouvez repartir de zéro"
else
    echo "❌ Nettoyage annulé"
fi
