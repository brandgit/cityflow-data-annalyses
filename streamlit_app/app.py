"""
CityFlow Analytics Dashboard - Version Professionnelle
Application Streamlit avec visualisations interactives
"""

import streamlit as st
from datetime import datetime, timedelta
import requests
from typing import Optional, Dict, List
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
        font-size: 3.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f77b4 0%, #ff7f0e 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stAlert {
        border-radius: 0.5rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

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
        st.error(f"Erreur : {e}")
        return None

def fetch_metric_by_name(date: str, metric_name: str) -> Optional[dict]:
    """Récupère une métrique spécifique."""
    try:
        response = requests.get(f"{API_URL}/metrics/{date}/{metric_name}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def fetch_correlations(date: str) -> Optional[dict]:
    """Récupère les corrélations."""
    try:
        response = requests.get(f"{API_URL}/correlations/{date}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def fetch_reports(date: str) -> Optional[dict]:
    """Récupère les rapports."""
    try:
        response = requests.get(f"{API_URL}/reports/{date}", timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/bicycle.png", width=80)
    st.title("⚙️ Configuration")
    
    # Status de l'API
    st.subheader("📡 État de l'API")
    if check_api_health():
        st.success("✅ API connectée")
    else:
        st.error("❌ API non accessible")
        st.stop()
    
    st.divider()
    
    # Sélection de la date
    st.subheader("📅 Sélection de la date")
    with st.spinner("Chargement..."):
        available_dates = fetch_available_dates()
    
    if not available_dates:
        selected_date = st.date_input("Date", value=datetime.now().date())
        selected_date = selected_date.strftime("%Y-%m-%d")
    else:
        available_dates.sort(reverse=True)
        selected_date = st.selectbox("Date", options=available_dates)
    
    st.divider()
    st.caption("🚴 CityFlow Analytics v2.0")

# ============================================================================
# EN-TÊTE
# ============================================================================

st.markdown('<h1 class="main-header">🚴 CityFlow Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown(f'<p class="sub-header">📅 Analyse du {selected_date}</p>', unsafe_allow_html=True)

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================

with st.spinner("🔄 Chargement des données..."):
    metrics_data = fetch_metrics(selected_date)

if not metrics_data or metrics_data.get("metrics_count", 0) == 0:
    st.warning("⚠️ Aucune donnée disponible pour cette date")
    st.stop()

# ============================================================================
# KPIs PRINCIPAUX
# ============================================================================

st.header("📊 Indicateurs Clés")

# Récupérer les métriques spécifiques
top_compteurs = fetch_metric_by_name(selected_date, "top_compteurs")
anomalies = fetch_metric_by_name(selected_date, "anomalies")
congestion = fetch_metric_by_name(selected_date, "congestion_cyclable")

col1, col2, col3, col4 = st.columns(4)

with col1:
    metrics_count = metrics_data.get("metrics_count", 0)
    st.metric(
        label="📈 Métriques Calculées",
        value=f"{metrics_count}/18",
        delta="Complet" if metrics_count == 18 else f"{18-metrics_count} manquantes"
    )

with col2:
    if top_compteurs and top_compteurs.get("data"):
        df_top = pd.DataFrame(top_compteurs["data"])
        # Chercher la colonne de débit disponible
        debit_col = None
        if "debit_total" in df_top.columns:
            debit_col = "debit_total"
        elif "dmja" in df_top.columns:
            debit_col = "dmja"
        elif "debit_moyen" in df_top.columns:
            debit_col = "debit_moyen"
        
        if not df_top.empty and debit_col:
            total_debit = int(df_top[debit_col].sum())
            st.metric(
                label="🚴 Débit Total",
                value=f"{total_debit:,}",
                delta="vélos"
            )
        else:
            st.metric(label="🚴 Débit Total", value="N/A")
    else:
        st.metric(label="🚴 Débit Total", value="N/A")

with col3:
    if anomalies and anomalies.get("data"):
        nb_anomalies = len(anomalies["data"])
        st.metric(
            label="🚨 Anomalies Détectées",
            value=nb_anomalies,
            delta="alertes" if nb_anomalies > 0 else "Aucune"
        )
    else:
        st.metric(label="🚨 Anomalies", value="0")

with col4:
    if congestion and congestion.get("data"):
        nb_congestions = len(congestion["data"])
        st.metric(
            label="🔴 Congestions",
            value=nb_congestions,
            delta="zones"
        )
    else:
        st.metric(label="🔴 Congestions", value="0")

st.divider()

# ============================================================================
# ONGLETS PRINCIPAUX
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Vue d'ensemble", "🚴 Flux Vélos", "🚨 Alertes", "🔗 Corrélations", "📄 Rapports"])

# ============================================================================
# TAB 1: VUE D'ENSEMBLE
# ============================================================================

with tab1:
    st.header("📊 Vue d'Ensemble")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top 10 Compteurs")
        if top_compteurs and top_compteurs.get("data"):
            df_top = pd.DataFrame(top_compteurs["data"])
            if not df_top.empty:
                # Déterminer la colonne de valeur disponible
                value_col = None
                if "debit_total" in df_top.columns:
                    value_col = "debit_total"
                elif "dmja" in df_top.columns:
                    value_col = "dmja"
                elif "debit_moyen" in df_top.columns:
                    value_col = "debit_moyen"
                
                if value_col and "compteur_id" in df_top.columns:
                    # Graphique en barres
                    fig = px.bar(
                        df_top.head(10),
                        x=value_col,
                        y="compteur_id",
                        orientation="h",
                        title="Top 10 Compteurs les plus actifs",
                        labels={value_col: value_col.replace("_", " ").title(), "compteur_id": "Compteur"},
                        color=value_col,
                        color_continuous_scale="Blues"
                    )
                    fig.update_layout(height=500, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    with st.expander("📊 Voir les données"):
                        st.dataframe(df_top.head(10), use_container_width=True)
            else:
                st.info("Aucune donnée disponible")
        else:
            st.info("Données non disponibles")
    
    with col2:
        st.subheader("⏰ Heures de Pointe")
        heures_pointe = fetch_metric_by_name(selected_date, "heures_pointe")
        if heures_pointe and heures_pointe.get("data"):
            df_heures = pd.DataFrame(heures_pointe["data"])
            if not df_heures.empty and "heure" in df_heures.columns:
                # Trouver la colonne de débit
                value_col = None
                for col in ["debit_moyen", "debit_total", "comptage"]:
                    if col in df_heures.columns:
                        value_col = col
                        break
                
                if value_col:
                    # Graphique en ligne
                    fig = px.line(
                        df_heures,
                        x="heure",
                        y=value_col,
                        title="Distribution Horaire du Trafic",
                        labels={"heure": "Heure", value_col: value_col.replace("_", " ").title()},
                        markers=True
                    )
                    fig.update_layout(height=500)
                    fig.update_traces(line_color="#1f77b4", line_width=3)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    with st.expander("📊 Voir les données"):
                        st.dataframe(df_heures, use_container_width=True)
            else:
                st.info("Aucune donnée disponible")
        else:
            st.info("Données non disponibles")
    
    st.divider()
    
    # Répartition géographique
    st.subheader("🗺️ Densité par Zone")
    densite = fetch_metric_by_name(selected_date, "densite_par_zone")
    if densite and densite.get("data"):
        df_densite = pd.DataFrame(densite["data"])
        if not df_densite.empty:
            # Trouver les colonnes appropriées
            value_col = None
            for col in ["debit_total", "comptage_total", "nombre_passages"]:
                if col in df_densite.columns:
                    value_col = col
                    break
            
            zone_col = None
            for col in ["arrondissement", "zone", "secteur"]:
                if col in df_densite.columns:
                    zone_col = col
                    break
            
            if value_col and zone_col:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Graphique en camembert
                    fig = px.pie(
                        df_densite,
                        values=value_col,
                        names=zone_col,
                        title="Répartition du Trafic par Zone",
                        hole=0.4
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Graphique en barres horizontales
                    fig = px.bar(
                        df_densite.sort_values(value_col, ascending=True).tail(15),
                        x=value_col,
                        y=zone_col,
                        orientation="h",
                        title="Top 15 Zones par Trafic",
                        labels={value_col: value_col.replace("_", " ").title(), zone_col: zone_col.title()},
                        color=value_col,
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                with st.expander("📊 Voir les données"):
                    st.dataframe(df_densite, use_container_width=True)
        else:
            st.info("Aucune donnée géographique disponible")
    else:
        st.info("Données non disponibles")

# ============================================================================
# TAB 2: FLUX VÉLOS
# ============================================================================

with tab2:
    st.header("🚴 Analyse des Flux Vélos")
    
    # Débit journalier
    st.subheader("📈 Débit Journalier")
    debit_j = fetch_metric_by_name(selected_date, "debit_journalier")
    if debit_j and debit_j.get("data"):
        df_debit = pd.DataFrame(debit_j["data"])
        if not df_debit.empty and len(df_debit) > 0:
            # Heatmap par compteur et date
            if "date" in df_debit.columns and "compteur_id" in df_debit.columns:
                # Prendre les top 20 compteurs
                top20 = df_debit.groupby("compteur_id")["debit_journalier"].sum().nlargest(20).index
                df_top20 = df_debit[df_debit["compteur_id"].isin(top20)]
                
                pivot = df_top20.pivot_table(
                    values="debit_journalier",
                    index="compteur_id",
                    columns="date",
                    aggfunc="mean"
                )
                
                fig = px.imshow(
                    pivot,
                    labels=dict(x="Date", y="Compteur", color="Débit"),
                    title="Heatmap - Débit Journalier (Top 20 Compteurs)",
                    aspect="auto",
                    color_continuous_scale="RdYlGn"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Distribution du débit
            fig = px.histogram(
                df_debit,
                x="debit_journalier",
                nbins=50,
                title="Distribution du Débit Journalier",
                labels={"debit_journalier": "Débit Journalier"},
                color_discrete_sequence=["#1f77b4"]
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistiques
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Moyenne", f"{df_debit['debit_journalier'].mean():.0f}")
            with col2:
                st.metric("Médiane", f"{df_debit['debit_journalier'].median():.0f}")
            with col3:
                st.metric("Max", f"{df_debit['debit_journalier'].max():.0f}")
            with col4:
                st.metric("Min", f"{df_debit['debit_journalier'].min():.0f}")
        else:
            st.info("Aucune donnée disponible")
    else:
        st.info("Données non disponibles")

# ============================================================================
# TAB 3: ALERTES
# ============================================================================

with tab3:
    st.header("🚨 Alertes et Anomalies")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⚠️ Anomalies Détectées")
        if anomalies and anomalies.get("data"):
            df_anomalies = pd.DataFrame(anomalies["data"])
            if not df_anomalies.empty:
                st.metric("Nombre d'anomalies", len(df_anomalies))
                
                # Graphique par type
                if "type_anomalie" in df_anomalies.columns:
                    fig = px.pie(
                        df_anomalies,
                        names="type_anomalie",
                        title="Répartition par Type d'Anomalie",
                        hole=0.3,
                        color_discrete_sequence=px.colors.sequential.Reds_r
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Top anomalies
                if "zscore" in df_anomalies.columns:
                    df_top_anomalies = df_anomalies.nlargest(10, "zscore")
                    fig = px.bar(
                        df_top_anomalies,
                        x="zscore",
                        y="compteur_id",
                        orientation="h",
                        title="Top 10 Anomalies (Z-Score)",
                        color="zscore",
                        color_continuous_scale="Reds"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("📋 Détails des Anomalies"):
                    st.dataframe(df_anomalies, use_container_width=True)
            else:
                st.success("✅ Aucune anomalie détectée")
        else:
            st.success("✅ Aucune anomalie")
    
    with col2:
        st.subheader("🔴 Zones de Congestion")
        if congestion and congestion.get("data"):
            df_congestion = pd.DataFrame(congestion["data"])
            if not df_congestion.empty:
                st.metric("Zones congestionnées", len(df_congestion))
                
                # Graphique des dépassements
                if "depassement_pct" in df_congestion.columns:
                    df_top_cong = df_congestion.nlargest(10, "depassement_pct")
                    fig = px.bar(
                        df_top_cong,
                        x="depassement_pct",
                        y="compteur_id",
                        orientation="h",
                        title="Top 10 Congestions (% Dépassement)",
                        color="depassement_pct",
                        color_continuous_scale="Oranges"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("📋 Détails des Congestions"):
                    st.dataframe(df_congestion, use_container_width=True)
            else:
                st.success("✅ Aucune congestion")
        else:
            st.success("✅ Aucune congestion")

# ============================================================================
# TAB 4: CORRÉLATIONS
# ============================================================================

with tab4:
    st.header("🔗 Corrélations")
    
    with st.spinner("Chargement des corrélations..."):
        corr_data = fetch_correlations(selected_date)
    
    if not corr_data:
        st.warning("Aucune corrélation disponible")
    else:
        for corr in corr_data.get("correlations", []):
            corr_name = corr.get("correlation_name", "Inconnue")
            st.subheader(f"🔗 {corr_name}")
            
            data = corr.get("data", [])
            if isinstance(data, list) and len(data) > 0:
                df_corr = pd.DataFrame(data)
                
                # Visualisation selon les colonnes disponibles
                if "correlation" in df_corr.columns:
                    fig = px.bar(
                        df_corr.head(20),
                        x=df_corr.columns[0],
                        y="correlation",
                        title=f"Corrélation - {corr_name}",
                        color="correlation",
                        color_continuous_scale="RdBu"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("📊 Voir les données"):
                    st.dataframe(df_corr, use_container_width=True)
            else:
                st.info("Aucune donnée de corrélation")

# ============================================================================
# TAB 5: RAPPORTS
# ============================================================================

with tab5:
    st.header("📄 Rapports Quotidiens")
    
    with st.spinner("Chargement des rapports..."):
        reports_data = fetch_reports(selected_date)
    
    if not reports_data or not reports_data.get("reports"):
        st.warning("Aucun rapport disponible pour cette date")
    else:
        reports_list = reports_data.get("reports", [])
        
        # Créer des sous-onglets pour chaque type de rapport
        if len(reports_list) > 0:
            report_types = [r.get("report_type", "Inconnu") for r in reports_list]
            report_tabs = st.tabs([f"📋 {rt.replace('_', ' ').title()}" for rt in report_types])
            
            for idx, (report_tab, report_item) in enumerate(zip(report_tabs, reports_list)):
                with report_tab:
                    report_type = report_item.get("report_type", "Inconnu")
                    report_content = report_item.get("report", {})
                    timestamp = report_item.get("timestamp", "N/A")
                    
                    st.subheader(f"📄 {report_type.replace('_', ' ').title()}")
                    st.caption(f"Généré le : {timestamp}")
                    
                    # Afficher le contenu selon le type
                    if isinstance(report_content, dict):
                        # Afficher les métriques clés
                        if "summary" in report_content or "metadata" in report_content:
                            cols = st.columns(3)
                            metrics_shown = 0
                            for key, value in report_content.items():
                                if isinstance(value, (int, float)) and metrics_shown < 3:
                                    with cols[metrics_shown]:
                                        st.metric(
                                            label=key.replace("_", " ").title(),
                                            value=f"{value:,.0f}" if isinstance(value, (int, float)) else value
                                        )
                                        metrics_shown += 1
                        
                        # Afficher le contenu complet dans un expander
                        with st.expander("📊 Détails complets du rapport", expanded=True):
                            # Formatter le JSON de manière lisible
                            st.json(report_content)
                    
                    elif isinstance(report_content, list):
                        # Si c'est une liste, l'afficher comme DataFrame
                        df_report = pd.DataFrame(report_content)
                        if not df_report.empty:
                            st.dataframe(df_report, use_container_width=True)
                        else:
                            st.info("Rapport vide")
                    
                    else:
                        # Affichage générique
                        st.write(report_content)

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #888; padding: 2rem;'>
    <p style='font-size: 1.2rem;'>🚴 <b>CityFlow Analytics Dashboard</b> v2.0</p>
    <p>Visualisation en temps réel des données de mobilité cyclable à Paris</p>
    <p>Données : DynamoDB | API : FastAPI | Dashboard : Streamlit</p>
</div>
""", unsafe_allow_html=True)
