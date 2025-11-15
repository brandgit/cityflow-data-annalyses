#!/bin/bash
# Script pour arrêter l'API et le Dashboard CityFlow

echo "Arret de CityFlow Analytics..."
echo ""

# Arrêter l'API
echo "Arret de l'API FastAPI..."
pkill -f "api.main"
if [ $? -eq 0 ]; then
    echo "   API arretee"
else
    echo "   Aucun processus API trouve"
fi

# Arrêter Streamlit
echo "Arret du Dashboard Streamlit..."
pkill -f "streamlit_app.app"
if [ $? -eq 0 ]; then
    echo "   Dashboard arrete"
else
    echo "   Aucun processus Dashboard trouve"
fi

echo ""
echo "CityFlow Analytics arrete"
echo ""

# Afficher les processus restants
REMAINING=$(ps aux | grep -E "api.main|streamlit_app.app" | grep -v grep | wc -l)
if [ $REMAINING -gt 0 ]; then
    echo "Processus restants detectes :"
    ps aux | grep -E "api.main|streamlit_app.app" | grep -v grep
    echo ""
    echo "Pour forcer l'arret: pkill -9 -f 'api.main|streamlit_app.app'"
fi

