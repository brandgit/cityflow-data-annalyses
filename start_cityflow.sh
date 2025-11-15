#!/bin/bash
# Script pour lancer l'API et le Dashboard CityFlow en arrière-plan

echo "Demarrage de CityFlow Analytics..."
echo ""

# Activer l'environnement virtuel si disponible
if [ -d "venv" ]; then
    echo "Activation de l'environnement virtuel..."
    source venv/bin/activate
else
    echo "Environnement virtuel non trouve. Assurez-vous que les dependances sont installees."
fi

echo ""

# Vérifier les dépendances
echo "Verification des dependances..."
if ! python -c "import fastapi" 2>/dev/null; then
    echo "FastAPI non installe. Installation..."
    pip install fastapi "uvicorn[standard]"
fi

if ! python -c "import streamlit" 2>/dev/null; then
    echo "Streamlit non installe. Installation..."
    pip install streamlit
fi

echo "Dependances OK"
echo ""

# Pas de logs (économie d'espace disque)

# Arrêter les processus existants
echo "Arret des processus existants..."
pkill -f "api.main" 2>/dev/null
pkill -f "streamlit_app.app" 2>/dev/null
sleep 2

# Lancer l'API en arrière-plan
echo "Lancement de l'API FastAPI (port 8000)..."
nohup python -m api.main > /dev/null 2>&1 &
API_PID=$!
echo "   API lancee (PID: $API_PID)"

# Attendre que l'API démarre
echo "Attente du demarrage de l'API..."
sleep 5

# Vérifier que l'API est bien lancée
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "   API accessible sur http://localhost:8000"
else
    echo "   L'API ne repond pas encore (peut prendre quelques secondes)"
fi

echo ""

# Lancer Streamlit en arrière-plan
echo "Lancement du Dashboard Streamlit (port 8501)..."
nohup streamlit run streamlit_app/app.py --server.port 8501 --server.address 0.0.0.0 > /dev/null 2>&1 &
STREAMLIT_PID=$!
echo "   Dashboard lance (PID: $STREAMLIT_PID)"

echo ""
echo "=========================================="
echo "CityFlow Analytics demarre avec succes !"
echo "=========================================="
echo ""
echo "API FastAPI:"
echo "   - Local:  http://localhost:8000"
echo "   - Public: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "IP_PUBLIQUE"):8000"
echo ""
echo "Dashboard Streamlit:"
echo "   - Local:  http://localhost:8501"
echo "   - Public: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "IP_PUBLIQUE"):8501"
echo ""
echo "Pour arreter les services:"
echo "   ./stop_cityflow.sh"
echo ""
echo "Processus en cours:"
echo "   - API PID:        $API_PID"
echo "   - Streamlit PID:  $STREAMLIT_PID"
echo ""

