#!/bin/bash
# Script pour vérifier l'état de CityFlow Analytics

echo "📊 État de CityFlow Analytics"
echo "=============================="
echo ""

# Vérifier l'API
echo "🔍 API FastAPI (port 8000):"
API_PID=$(pgrep -f "api.main")
if [ -n "$API_PID" ]; then
    echo "   ✅ Processus actif (PID: $API_PID)"
    if curl -s http://localhost:8000/health > /dev/null 2>&1; then
        echo "   ✅ API accessible"
    else
        echo "   ⚠️  Processus actif mais API non accessible"
    fi
else
    echo "   ❌ Processus non actif"
fi

echo ""

# Vérifier Streamlit
echo "🔍 Dashboard Streamlit (port 8501):"
STREAMLIT_PID=$(pgrep -f "streamlit_app.app")
if [ -n "$STREAMLIT_PID" ]; then
    echo "   ✅ Processus actif (PID: $STREAMLIT_PID)"
    if curl -s http://localhost:8501 > /dev/null 2>&1; then
        echo "   ✅ Dashboard accessible"
    else
        echo "   ⚠️  Processus actif mais Dashboard non accessible"
    fi
else
    echo "   ❌ Processus non actif"
fi

echo ""
echo "=============================="
echo ""

# Afficher les URLs
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null)
if [ -n "$PUBLIC_IP" ]; then
    echo "🌐 URLs publiques (EC2):"
    echo "   • API:       http://$PUBLIC_IP:8000"
    echo "   • Dashboard: http://$PUBLIC_IP:8501"
else
    echo "🌐 URLs locales:"
    echo "   • API:       http://localhost:8000"
    echo "   • Dashboard: http://localhost:8501"
fi

echo ""
echo "📝 Logs:"
echo "   • API:       tail -f logs/api.log"
echo "   • Dashboard: tail -f logs/streamlit.log"
echo ""

