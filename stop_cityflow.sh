#!/bin/bash
# Script pour arrêter l'API et le Dashboard CityFlow

echo "🛑 Arrêt de CityFlow Analytics..."
echo ""

# Arrêter l'API
echo "🔌 Arrêt de l'API FastAPI..."
pkill -f "api.main"
if [ $? -eq 0 ]; then
    echo "   ✓ API arrêtée"
else
    echo "   ℹ️  Aucun processus API trouvé"
fi

# Arrêter Streamlit
echo "🔌 Arrêt du Dashboard Streamlit..."
pkill -f "streamlit_app.app"
if [ $? -eq 0 ]; then
    echo "   ✓ Dashboard arrêté"
else
    echo "   ℹ️  Aucun processus Dashboard trouvé"
fi

echo ""
echo "✅ CityFlow Analytics arrêté"
echo ""

# Afficher les processus restants
REMAINING=$(ps aux | grep -E "api.main|streamlit_app.app" | grep -v grep | wc -l)
if [ $REMAINING -gt 0 ]; then
    echo "⚠️  Processus restants détectés:"
    ps aux | grep -E "api.main|streamlit_app.app" | grep -v grep
    echo ""
    echo "Pour forcer l'arrêt: pkill -9 -f 'api.main|streamlit_app.app'"
fi

