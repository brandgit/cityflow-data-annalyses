"""
Application Streamlit principale pour CityFlow Analytics Dashboard.
"""

import streamlit as st
from datetime import datetime, timedelta
import requests
from typing import Optional
import os

# Configuration de la page
st.set_page_config(
    page_title="CityFlow Analytics Dashboard",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# URL de l'API
API_URL = os.getenv("API_URL", "http://localhost:8000")

# Style CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
</style>
""", unsafe_allow_html=True)

def check_api_health() -> bool:
    """Vérifie que l'API est accessible."""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def fetch_available_dates() -> list:
    """Récupère les dates disponibles depuis l'API."""
    try:
        response = requests.get(f"{API_URL}/metrics", timeout=10)
        if response.status_code == 200:
            return response.json().get("dates", [])
        return []
    except:
        return []

def fetch_metrics(date: str) -> Optional[dict]:
    """Récupère les métriques pour une date donnée."""
    try:
        response = requests.get(f"{API_URL}/metrics/{date}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        st.error(f"Erreur lors de la récupération des métriques : {e}")
        return None

def fetch_correlations(date: str) -> Optional[dict]:
    """Récupère les corrélations pour une date donnée."""
    try:
        response = requests.get(f"{API_URL}/correlations/{date}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        st.error(f"Erreur lors de la récupération des corrélations : {e}")
        return None

def fetch_reports(date: str) -> Optional[dict]:
    """Récupère les rapports pour une date donnée."""
    try:
        response = requests.get(f"{API_URL}/reports/{date}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        st.error(f"Erreur lors de la récupération des rapports : {e}")
        return None

# ============================================================================
# Interface principale
# ============================================================================

st.markdown('<h1 class="main-header">🚴 CityFlow Analytics Dashboard</h1>', unsafe_allow_html=True)

# Vérification de l'API
with st.sidebar:
    st.title("⚙️ Configuration")
    
    # Status de l'API
    st.subheader("📡 État de l'API")
    if check_api_health():
        st.success("✅ API connectée")
        st.text(f"URL: {API_URL}")
    else:
        st.error("❌ API non accessible")
        st.text(f"URL: {API_URL}")
        st.info("💡 Assurez-vous que l'API est lancée :\n```bash\npython -m api.main\n```")
        st.stop()
    
    st.divider()
    
    # Sélection de la date
    st.subheader("📅 Sélection de la date")
    
    # Récupérer les dates disponibles
    with st.spinner("Chargement des dates..."):
        available_dates = fetch_available_dates()
    
    if not available_dates:
        st.warning("Aucune date disponible")
        selected_date = st.date_input(
            "Date",
            value=datetime.now().date(),
            help="Aucune donnée trouvée pour cette date"
        )
        selected_date = selected_date.strftime("%Y-%m-%d")
    else:
        # Trier les dates et prendre la plus récente par défaut
        available_dates.sort(reverse=True)
        selected_date = st.selectbox(
            "Date disponible",
            options=available_dates,
            help=f"{len(available_dates)} date(s) disponible(s)"
        )
    
    st.divider()
    
    # Informations
    st.subheader("ℹ️ Informations")
    st.info("""
    **Navigation :**
    - 📊 Métriques : 18 indicateurs
    - 🔗 Corrélations : Analyses croisées
    - 📈 Rapports : Synthèses quotidiennes
    """)

# ============================================================================
# Onglets principaux
# ============================================================================

tab1, tab2, tab3 = st.tabs(["📊 Métriques", "🔗 Corrélations", "📈 Rapports"])

# ============================================================================
# ONGLET 1 : MÉTRIQUES
# ============================================================================

with tab1:
    st.header(f"📊 Métriques CityFlow - {selected_date}")
    
    with st.spinner("Chargement des métriques..."):
        metrics_data = fetch_metrics(selected_date)
    
    if not metrics_data:
        st.warning("Aucune métrique disponible pour cette date.")
    else:
        metrics_count = metrics_data.get("metrics_count", 0)
        st.success(f"✅ {metrics_count} métriques disponibles")
        
        # Catégorisation des métriques
        flux_metrics = ["debit_horaire", "debit_journalier", "dmja"]
        temporal_metrics = ["profil_jour_type", "heures_pointe", "evolution_hebdomadaire", "ratio_weekend_semaine"]
        performance_metrics = ["taux_disponibilite", "top_compteurs", "compteurs_faible_activite", "compteurs_defaillants"]
        geo_metrics = ["densite_par_zone", "corridors_cyclables"]
        alert_metrics = ["congestion_cyclable", "anomalies"]
        chantier_metrics = ["chantiers_actifs", "score_criticite_chantiers"]
        qualite_metrics = ["qualite_service"]
        
        # Affichage par catégorie
        categories = {
            "🚦 Métriques de Flux": flux_metrics,
            "⏰ Profils Temporels": temporal_metrics,
            "📈 Performance Compteurs": performance_metrics,
            "🗺️ Géographie": geo_metrics,
            "🚨 Alertes": alert_metrics,
            "🚧 Chantiers": chantier_metrics,
            "✨ Qualité de Service": qualite_metrics
        }
        
        for category_name, metric_names in categories.items():
            with st.expander(category_name, expanded=False):
                for metric_data in metrics_data.get("metrics", []):
                    metric_name = metric_data.get("metric_name")
                    if metric_name in metric_names:
                        st.subheader(f"📌 {metric_name}")
                        
                        # Afficher les données
                        data = metric_data.get("data", [])
                        if isinstance(data, list) and len(data) > 0:
                            import pandas as pd
                            df = pd.DataFrame(data)
                            
                            col1, col2 = st.columns([1, 3])
                            with col1:
                                st.metric("Nombre de lignes", len(df))
                            with col2:
                                st.dataframe(df.head(10), use_container_width=True)
                            
                            if st.button(f"Voir toutes les données - {metric_name}", key=f"btn_{metric_name}"):
                                st.dataframe(df, use_container_width=True)
                        else:
                            st.info(f"Données: {data}")
                        
                        st.divider()

# ============================================================================
# ONGLET 2 : CORRÉLATIONS
# ============================================================================

with tab2:
    st.header(f"🔗 Corrélations - {selected_date}")
    
    with st.spinner("Chargement des corrélations..."):
        corr_data = fetch_correlations(selected_date)
    
    if not corr_data:
        st.warning("Aucune corrélation disponible pour cette date.")
    else:
        corr_count = corr_data.get("correlations_count", 0)
        st.success(f"✅ {corr_count} corrélation(s) disponible(s)")
        
        for corr in corr_data.get("correlations", []):
            corr_name = corr.get("correlation_name", "Inconnue")
            st.subheader(f"🔗 {corr_name}")
            
            data = corr.get("data", [])
            if isinstance(data, list) and len(data) > 0:
                import pandas as pd
                df = pd.DataFrame(data)
                
                col1, col2 = st.columns([1, 3])
                with col1:
                    st.metric("Nombre de lignes", len(df))
                with col2:
                    st.dataframe(df.head(10), use_container_width=True)
                
                if st.button(f"Voir toutes les données - {corr_name}", key=f"corr_{corr_name}"):
                    st.dataframe(df, use_container_width=True)
            else:
                st.info(f"Données: {data}")
            
            st.divider()

# ============================================================================
# ONGLET 3 : RAPPORTS
# ============================================================================

with tab3:
    st.header(f"📈 Rapports - {selected_date}")
    
    with st.spinner("Chargement des rapports..."):
        reports_data = fetch_reports(selected_date)
    
    if not reports_data:
        st.warning("Aucun rapport disponible pour cette date.")
    else:
        reports_count = reports_data.get("reports_count", 0)
        st.success(f"✅ {reports_count} rapport(s) disponible(s)")
        
        for report in reports_data.get("reports", []):
            report_type = report.get("report_type", "Inconnu")
            st.subheader(f"📄 {report_type}")
            
            report_data = report.get("report", {})
            
            # Afficher le contenu du rapport
            if isinstance(report_data, dict):
                for key, value in report_data.items():
                    with st.expander(f"📋 {key}", expanded=True):
                        if isinstance(value, (list, dict)):
                            st.json(value)
                        else:
                            st.write(value)
            else:
                st.json(report_data)
            
            st.divider()

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #888;'>
    <p>🚴 CityFlow Analytics Dashboard v1.0.0</p>
    <p>Données en temps réel depuis DynamoDB via API FastAPI</p>
</div>
""", unsafe_allow_html=True)

