#!/bin/bash
# Script pour lancer l'API et le Dashboard CityFlow en arrière-plan

echo "🚴 Démarrage de CityFlow Analytics..."
echo ""

# Activer l'environnement virtuel si disponible
if [ -d "venv" ]; then
    echo "📦 Activation de l'environnement virtuel..."
    source venv/bin/activate
else
    echo "⚠️  Environnement virtuel non trouvé. Assurez-vous que les dépendances sont installées."
fi

echo ""

# Vérifier les dépendances
echo "🔍 Vérification des dépendances..."
if ! python -c "import fastapi" 2>/dev/null; then
    echo "❌ FastAPI non installé. Installation..."
    pip install fastapi "uvicorn[standard]"
fi

if ! python -c "import streamlit" 2>/dev/null; then
    echo "❌ Streamlit non installé. Installation..."
    pip install streamlit
fi

echo "✅ Dépendances OK"
echo ""

# Pas de logs (économie d'espace disque)

# Arrêter les processus existants
echo "🛑 Arrêt des processus existants..."
pkill -f "api.main" 2>/dev/null
pkill -f "streamlit_app.app" 2>/dev/null
sleep 2

# Lancer l'API en arrière-plan
echo "🚀 Lancement de l'API FastAPI (port 8000)..."
nohup python -m api.main > /dev/null 2>&1 &
API_PID=$!
echo "   ✓ API lancée (PID: $API_PID)"

# Attendre que l'API démarre
echo "⏳ Attente du démarrage de l'API..."
sleep 5

# Vérifier que l'API est bien lancée
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "   ✅ API accessible sur http://localhost:8000"
else
    echo "   ⚠️  L'API ne répond pas encore (peut prendre quelques secondes)"
fi

echo ""

# Lancer Streamlit en arrière-plan
echo "🎨 Lancement du Dashboard Streamlit (port 8501)..."
nohup streamlit run streamlit_app/app.py --server.port 8501 --server.address 0.0.0.0 > /dev/null 2>&1 &
STREAMLIT_PID=$!
echo "   ✓ Dashboard lancé (PID: $STREAMLIT_PID)"

echo ""
echo "=========================================="
echo "✅ CityFlow Analytics démarré avec succès !"
echo "=========================================="
echo ""
echo "📡 API FastAPI:"
echo "   • Local:  http://localhost:8000"
echo "   • Public: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "IP_PUBLIQUE"):8000"
echo ""
echo "🎨 Dashboard Streamlit:"
echo "   • Local:  http://localhost:8501"
echo "   • Public: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "IP_PUBLIQUE"):8501"
echo ""
echo "🛑 Pour arrêter les services:"
echo "   ./stop_cityflow.sh"
echo ""
echo "💡 Processus en cours:"
echo "   • API PID:        $API_PID"
echo "   • Streamlit PID:  $STREAMLIT_PID"
echo ""

