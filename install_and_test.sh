#!/bin/bash

echo "🚀 Installation et Test - CityFlow Analytics"
echo "============================================="

# Activer l'environnement virtuel
echo ""
echo "📦 Activation de l'environnement virtuel..."
source venv/bin/activate

# Installer les dépendances
echo ""
echo "📥 Installation des dépendances..."
pip install -q python-dotenv pymongo || {
    echo "❌ Erreur lors de l'installation des dépendances"
    echo "Essayez manuellement : pip install python-dotenv pymongo"
    exit 1
}

echo "✅ Dépendances installées"

# Vérifier que MongoDB est en cours d'exécution
echo ""
echo "🔍 Vérification de MongoDB..."
if command -v mongosh &> /dev/null; then
    if mongosh --eval "db.version()" --quiet &> /dev/null; then
        echo "✅ MongoDB est en cours d'exécution"
    else
        echo "⚠️  MongoDB n'est pas en cours d'exécution"
        echo ""
        echo "Pour démarrer MongoDB:"
        echo "  macOS: brew services start mongodb-community"
        echo "  Linux: sudo systemctl start mongodb"
        echo "  Docker: docker run -d -p 27017:27017 --name mongodb mongo:latest"
        echo ""
        read -p "Voulez-vous continuer sans MongoDB? (les données ne seront pas sauvegardées en base) [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
else
    echo "⚠️  MongoDB n'est pas installé ou mongosh n'est pas dans le PATH"
    echo ""
    echo "Installation MongoDB:"
    echo "  macOS: brew install mongodb-community"
    echo "  Linux: sudo apt-get install mongodb"
    echo ""
    read -p "Voulez-vous continuer sans MongoDB? (les données ne seront pas sauvegardées en base) [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Lancer le traitement
echo ""
echo "🚀 Lancement du traitement..."
echo "============================================="
python processors/main.py

# Vérifier les résultats
echo ""
echo "============================================="
echo "📊 Vérification des résultats..."
echo ""

# Compter les fichiers générés
JSON_COUNT=$(find output/$(date +%Y-%m-%d) -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
echo "✅ Fichiers JSON générés: $JSON_COUNT"

# Vérifier MongoDB si disponible
if command -v mongosh &> /dev/null && mongosh --eval "db.version()" --quiet &> /dev/null 2>&1; then
    echo ""
    echo "🗄️  Vérification MongoDB..."
    
    METRICS_COUNT=$(mongosh cityflow-db --quiet --eval "db['cityflow-metrics'].countDocuments()" 2>/dev/null || echo "0")
    CORR_COUNT=$(mongosh cityflow-db --quiet --eval "db['cityflow-daily-correlations'].countDocuments()" 2>/dev/null || echo "0")
    REPORTS_COUNT=$(mongosh cityflow-db --quiet --eval "db['cityflow-daily-reports'].countDocuments()" 2>/dev/null || echo "0")
    
    echo "  • cityflow-metrics: $METRICS_COUNT document(s)"
    echo "  • cityflow-daily-correlations: $CORR_COUNT document(s)"
    echo "  • cityflow-daily-reports: $REPORTS_COUNT document(s)"
    
    if [ "$METRICS_COUNT" -gt "0" ] || [ "$CORR_COUNT" -gt "0" ] || [ "$REPORTS_COUNT" -gt "0" ]; then
        echo ""
        echo "✅ Données sauvegardées dans MongoDB!"
        echo ""
        echo "Pour voir les données:"
        echo "  mongosh cityflow-db"
        echo "  > db.getCollectionNames()"
        echo "  > db['cityflow-metrics'].findOne()"
    else
        echo ""
        echo "⚠️  Aucune donnée trouvée dans MongoDB"
    fi
fi

echo ""
echo "============================================="
echo "✅ Traitement terminé!"
echo "============================================="

