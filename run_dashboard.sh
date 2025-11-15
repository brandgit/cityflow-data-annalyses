#!/bin/bash
# Script pour lancer le dashboard CityFlow Analytics

echo "Demarrage du Dashboard CityFlow Analytics..."
echo ""

# Activer l'environnement virtuel si disponible
if [ -d "venv" ]; then
    echo "Activation de l'environnement virtuel..."
    source venv/bin/activate
fi

# Vérifier que Streamlit est installé
if ! command -v streamlit &> /dev/null; then
    echo "Streamlit n'est pas installe."
    echo "Installation de Streamlit..."
    pip install streamlit>=1.32.0
fi

# Vérifier que l'API est accessible
echo "Verification de l'API..."
if curl -s http://localhost:8000/health > /dev/null; then
    echo "API accessible sur http://localhost:8000"
else
    echo "L'API n'est pas accessible sur http://localhost:8000"
    echo "Assurez-vous de lancer l'API d'abord : python -m api.main"
    echo ""
    read -p "Continuer quand même ? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "Lancement du dashboard Streamlit..."
echo "URL : http://localhost:8501"
echo ""
echo "Pour arreter : Ctrl+C"
echo ""

streamlit run streamlit_app/app.py

