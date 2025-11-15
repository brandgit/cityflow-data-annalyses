#!/bin/bash
# Script complet : Traitement → API → Dashboard
# Lance le pipeline CityFlow Analytics de bout en bout

echo "================================================================"
echo "    CityFlow Analytics - Pipeline Complet"
echo "================================================================"
echo ""

# Activer l'environnement virtuel
if [ -d "venv" ]; then
    echo "Activation de l'environnement virtuel..."
    source venv/bin/activate
else
    echo "Environnement virtuel non trouve."
fi

echo ""

# Vérifier les dépendances
echo "Verification des dependances..."
MISSING_DEPS=0

if ! python -c "import pandas" 2>/dev/null; then
    echo "   pandas manquant"
    MISSING_DEPS=1
fi

if ! python -c "import fastapi" 2>/dev/null; then
    echo "   fastapi manquant"
    MISSING_DEPS=1
fi

if ! python -c "import streamlit" 2>/dev/null; then
    echo "   streamlit manquant"
    MISSING_DEPS=1
fi

if [ $MISSING_DEPS -eq 1 ]; then
    echo ""
echo "Installation des dependances manquantes..."
    pip install -r requirements.txt
    echo ""
fi

echo "Dependances OK"
echo ""

# ============================================================================
# ÉTAPE 1 : TRAITEMENT DES DONNÉES
# ============================================================================

echo "----------------------------------------------------------------"
echo "  ETAPE 1/3 : Traitement des donnees"
echo "----------------------------------------------------------------"
echo ""

echo "Lancement du traitement des donnees (processors)..."
if [ -n "$1" ]; then
    echo "   Date renseignee : $1"
fi
echo ""

if [ -n "$1" ]; then
    python -m processors.main "$1" 2>&1
else
    python -m processors.main 2>&1
fi

PROCESSOR_EXIT_CODE=$?

echo ""
if [ $PROCESSOR_EXIT_CODE -eq 0 ]; then
    echo "Traitement termine avec succes"
else
    echo "Erreur lors du traitement (code: $PROCESSOR_EXIT_CODE)"
    echo ""
    read -p "Continuer quand même ? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo "Attente de 3 secondes avant de lancer les services..."
sleep 3
echo ""

# ============================================================================
# ÉTAPE 2 : API FASTAPI
# ============================================================================

echo "----------------------------------------------------------------"
echo "  ETAPE 2/3 : Lancement de l'API FastAPI"
echo "----------------------------------------------------------------"
echo ""

# Arrêter l'API si elle tourne déjà
echo "Arret de l'ancienne API..."
pkill -f "api.main" 2>/dev/null
sleep 2

# Lancer l'API en arrière-plan
echo "Lancement de l'API (port 8000)..."
nohup python -m api.main > /dev/null 2>&1 &
API_PID=$!

echo "   API lancee (PID: $API_PID)"

# Attendre que l'API démarre
echo "   Verification du demarrage..."
sleep 5

if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "   API accessible sur http://localhost:8000"
else
    echo "   L'API ne repond pas encore (peut prendre quelques secondes)"
fi

echo ""
sleep 2

# ============================================================================
# ÉTAPE 3 : DASHBOARD STREAMLIT
# ============================================================================

echo "----------------------------------------------------------------"
echo "  ETAPE 3/3 : Lancement du Dashboard Streamlit"
echo "----------------------------------------------------------------"
echo ""

# Arrêter Streamlit s'il tourne déjà
echo "Arret de l'ancien dashboard..."
pkill -f "streamlit_app.app" 2>/dev/null
sleep 2

# Lancer Streamlit en arrière-plan
echo "Lancement du Dashboard (port 8501)..."
nohup streamlit run streamlit_app/app.py --server.port 8501 --server.address 0.0.0.0 > /dev/null 2>&1 &
STREAMLIT_PID=$!

echo "   Dashboard lance (PID: $STREAMLIT_PID)"

echo ""
sleep 2

# ============================================================================
# RÉSUMÉ FINAL
# ============================================================================

echo ""
echo "================================================================"
echo "  PIPELINE CITYFLOW ANALYTICS DEMARRE AVEC SUCCES !"
echo "================================================================"
echo ""

# Obtenir l'IP publique si sur EC2
PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null)

echo "URLs d'acces :"
echo ""
if [ -n "$PUBLIC_IP" ]; then
    echo "   API FastAPI (Public):"
    echo "      http://$PUBLIC_IP:8000"
    echo "      http://$PUBLIC_IP:8000/docs (Documentation interactive)"
    echo ""
    echo "   Dashboard Streamlit (Public):"
    echo "      http://$PUBLIC_IP:8501"
else
    echo "   API FastAPI (Local):"
    echo "      http://localhost:8000"
    echo "      http://localhost:8000/docs (Documentation interactive)"
    echo ""
    echo "   Dashboard Streamlit (Local):"
    echo "      http://localhost:8501"
fi

echo ""
echo "Pour arreter les services :"
echo "   ./stop_cityflow.sh"
echo ""

echo "Pour verifier l'etat :"
echo "   ./status_cityflow.sh"
echo ""

echo "Processus en cours :"
echo "   - API PID:        $API_PID"
echo "   - Streamlit PID:  $STREAMLIT_PID"
echo ""

echo "================================================================"
echo "  Pret a l'emploi ! Ouvrez votre navigateur."
echo "================================================================"
echo ""

