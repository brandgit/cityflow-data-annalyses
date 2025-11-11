"""
CityFlow Analytics Dashboard - Version Professionnelle
Application Streamlit pour visualiser les données de mobilité cyclable à Paris
"""

import streamlit as st
from datetime import datetime
import requests
from typing import Optional, Dict
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# Configuration
st.set_page_config(
    page_title="CityFlow Analytics",
    page_icon="🚴",
    layout="wide",
    initial_sidebar_state="expanded"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")

# CSS personnalisé
st.markdown("""
<style>
    .main-title {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1f77b4, #2ca02c);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0;
    }
    .subtitle {
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FONCTIONS API
# ============================================================================

def check_api() -> bool:
    try:
        r = requests.get(f"{API_URL}/health", timeout=5)
        return r.status_code == 200
    except:
        return False

def get_dates() -> list:
    try:
        r = requests.get(f"{API_URL}/metrics", timeout=10)
        return r.json().get("dates", []) if r.status_code == 200 else []
    except:
        return []

def get_metric(date: str, name: str) -> Optional[dict]:
    try:
        r = requests.get(f"{API_URL}/metrics/{date}/{name}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_all_metrics(date: str) -> Optional[dict]:
    try:
        r = requests.get(f"{API_URL}/metrics/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_correlations(date: str) -> Optional[dict]:
    try:
        r = requests.get(f"{API_URL}/correlations/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_reports(date: str) -> Optional[dict]:
    try:
        r = requests.get(f"{API_URL}/reports/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

# ============================================================================
# HEADER
# ============================================================================

st.markdown("<h1 class='main-title'>🚴 CityFlow Analytics</h1>", unsafe_allow_html=True)
st.markdown("<p class='subtitle'>Analyse en temps réel de la mobilité cyclable à Paris</p>", unsafe_allow_html=True)

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.title("⚙️ Configuration")
    
    # API Status
    if check_api():
        st.success("✅ API connectée")
    else:
        st.error("❌ API déconnectée")
        st.stop()
    
    # Sélection de date
    dates = get_dates()
    if not dates:
        st.error("Aucune donnée disponible")
        st.stop()
    
    selected_date = st.selectbox("📅 Date", sorted(dates, reverse=True))
    
    st.divider()
    st.caption("CityFlow Analytics v2.0")

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================

with st.spinner("📥 Chargement des données..."):
    all_data = get_all_metrics(selected_date)
    correlations_data = get_correlations(selected_date)
    reports_data = get_reports(selected_date)

if not all_data:
    st.error("Aucune métrique disponible")
    st.stop()

# Extraire les métriques
metrics = {m["metric_name"]: m["data"] for m in all_data.get("metrics", [])}

# ============================================================================
# KPIS GLOBAUX
# ============================================================================

st.header("📊 Vue d'Ensemble")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📈 Métriques", f"{len(metrics)}/18")

with col2:
    anomalies = len(metrics.get("anomalies", []))
    st.metric("🚨 Anomalies", anomalies, delta="alertes" if anomalies > 0 else "Aucune")

with col3:
    congestions = len(metrics.get("congestion_cyclable", []))
    st.metric("🔴 Congestions", congestions, delta="zones")

with col4:
    df_top = pd.DataFrame(metrics.get("top_compteurs", []))
    if not df_top.empty:
        total_compteurs = len(df_top)
        st.metric("🚴 Compteurs actifs", total_compteurs)
    else:
        st.metric("🚴 Compteurs", "N/A")

st.divider()

# ============================================================================
# ONGLETS PRINCIPAUX
# ============================================================================

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Analyse Flux", 
    "🚨 Alertes & Anomalies",
    "🔗 Corrélations",
    "📄 Rapports"
])

# ============================================================================
# TAB 1: ANALYSE DES FLUX
# ============================================================================

with tab1:
    st.header("📊 Analyse des Flux Cyclables")
    
    # Top Compteurs
    df_top = pd.DataFrame(metrics.get("top_compteurs", []))
    if not df_top.empty:
        st.subheader("🏆 Top 20 Compteurs les Plus Actifs")
        
        # Trouver la colonne de valeur
        value_col = "dmja" if "dmja" in df_top.columns else ("debit_total" if "debit_total" in df_top.columns else "debit_moyen")
        
        if value_col in df_top.columns and "compteur_id" in df_top.columns:
            fig = px.bar(
                df_top.head(20).sort_values(value_col, ascending=True),
                y="compteur_id",
                x=value_col,
                orientation="h",
                title=f"Top 20 Compteurs par {value_col.upper()}",
                labels={value_col: value_col.replace("_", " ").title(), "compteur_id": "Compteur"},
                color=value_col,
                color_continuous_scale="Viridis",
                height=600
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(df_top.head(20), use_container_width=True)
    
    st.divider()
    
    # Heures de pointe
    df_heures = pd.DataFrame(metrics.get("heures_pointe", []))
    if not df_heures.empty and "heure" in df_heures.columns:
        st.subheader("⏰ Profil Horaire du Trafic")
        
        value_col = None
        for col in ["debit_moyen", "debit_total", "comptage"]:
            if col in df_heures.columns:
                value_col = col
                break
        
        if value_col:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_heures["heure"],
                y=df_heures[value_col],
                mode='lines+markers',
                name='Débit',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(31, 119, 180, 0.2)'
            ))
            fig.update_layout(
                title="Évolution du trafic cyclable sur 24h",
                xaxis_title="Heure de la journée",
                yaxis_title=value_col.replace("_", " ").title(),
                hovermode='x unified',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Débit journalier (heatmap)
    df_debit = pd.DataFrame(metrics.get("debit_journalier", []))
    if not df_debit.empty and "compteur_id" in df_debit.columns and "debit_journalier" in df_debit.columns:
        st.subheader("📅 Débit Journalier par Compteur")
        
        # Top 15 compteurs
        top_15 = df_debit.groupby("compteur_id")["debit_journalier"].sum().nlargest(15).index
        df_filtered = df_debit[df_debit["compteur_id"].isin(top_15)]
        
        if "date" in df_filtered.columns:
            pivot = df_filtered.pivot_table(
                values="debit_journalier",
                index="compteur_id",
                columns="date",
                aggfunc="sum"
            )
            
            fig = px.imshow(
                pivot,
                labels=dict(x="Date", y="Compteur", color="Débit"),
                title="Heatmap du Débit Journalier (Top 15 Compteurs)",
                color_continuous_scale="RdYlGn",
                aspect="auto",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Graphique en barres si pas de colonne date
            df_sum = df_filtered.groupby("compteur_id")["debit_journalier"].sum().reset_index()
            fig = px.bar(
                df_sum.sort_values("debit_journalier", ascending=True),
                y="compteur_id",
                x="debit_journalier",
                orientation="h",
                title="Débit Journalier Total par Compteur",
                color="debit_journalier",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Densité par zone
    df_densite = pd.DataFrame(metrics.get("densite_par_zone", []))
    if not df_densite.empty:
        st.subheader("🗺️ Répartition Géographique")
        
        # Trouver les colonnes
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
                fig = px.pie(
                    df_densite.head(15),
                    values=value_col,
                    names=zone_col,
                    title="Répartition du Trafic",
                    hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    df_densite.sort_values(value_col, ascending=False).head(15),
                    x=zone_col,
                    y=value_col,
                    title="Top 15 Zones",
                    color=value_col,
                    color_continuous_scale="Blues"
                )
                st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# TAB 2: ALERTES & ANOMALIES
# ============================================================================

with tab2:
    st.header("🚨 Alertes et Détection d'Anomalies")
    
    # Anomalies
    df_anomalies = pd.DataFrame(metrics.get("anomalies", []))
    if not df_anomalies.empty:
        st.subheader("🔍 Anomalies Détectées")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Nombre d'anomalies", len(df_anomalies))
            
            if "type_anomalie" in df_anomalies.columns:
                type_counts = df_anomalies["type_anomalie"].value_counts()
                fig = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title="Types d'Anomalies",
                    hole=0.3
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if "zscore" in df_anomalies.columns and "compteur_id" in df_anomalies.columns:
                top_10 = df_anomalies.nlargest(10, "zscore")
                fig = px.bar(
                    top_10,
                    x="zscore",
                    y="compteur_id",
                    orientation="h",
                    title="Top 10 Anomalies (Z-score)",
                    color="zscore",
                    color_continuous_scale="Reds"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("📋 Voir toutes les anomalies"):
            st.dataframe(df_anomalies, use_container_width=True)
    else:
        st.success("✅ Aucune anomalie détectée")
    
    st.divider()
    
    # Congestions
    df_congestion = pd.DataFrame(metrics.get("congestion_cyclable", []))
    if not df_congestion.empty:
        st.subheader("🔴 Zones de Congestion")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Zones congestionnées", len(df_congestion))
        
        with col2:
            if "depassement_pct" in df_congestion.columns:
                avg_depassement = df_congestion["depassement_pct"].mean()
                st.metric("Dépassement moyen", f"{avg_depassement:.1f}%")
        
        if "depassement_pct" in df_congestion.columns and "compteur_id" in df_congestion.columns:
            top_15 = df_congestion.nlargest(15, "depassement_pct")
            fig = px.bar(
                top_15.sort_values("depassement_pct"),
                y="compteur_id",
                x="depassement_pct",
                orientation="h",
                title="Top 15 Congestions (% Dépassement)",
                color="depassement_pct",
                color_continuous_scale="Oranges",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("📋 Voir toutes les congestions"):
            st.dataframe(df_congestion, use_container_width=True)
    else:
        st.success("✅ Aucune congestion détectée")

# ============================================================================
# TAB 3: CORRÉLATIONS
# ============================================================================

with tab3:
    st.header("🔗 Analyse des Corrélations")
    
    if not correlations_data or not correlations_data.get("correlations"):
        st.info("Aucune corrélation disponible pour cette date")
    else:
        for corr_item in correlations_data["correlations"]:
            corr_name = corr_item.get("correlation_name", "Inconnue")
            corr_data = corr_item.get("data", [])
            
            st.subheader(f"🔗 {corr_name.replace('_', ' ').title()}")
            
            if isinstance(corr_data, list) and len(corr_data) > 0:
                df_corr = pd.DataFrame(corr_data)
                
                # Afficher selon les colonnes disponibles
                if "correlation" in df_corr.columns:
                    # Graphique de corrélation
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=df_corr.iloc[:, 0],
                        y=df_corr["correlation"],
                        marker=dict(
                            color=df_corr["correlation"],
                            colorscale="RdBu",
                            cmin=-1,
                            cmax=1,
                            colorbar=dict(title="Corrélation")
                        )
                    ))
                    fig.update_layout(
                        title=f"Valeurs de Corrélation - {corr_name}",
                        xaxis_title=df_corr.columns[0],
                        yaxis_title="Coefficient de Corrélation",
                        yaxis=dict(range=[-1, 1]),
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Afficher les données
                with st.expander("📊 Voir les données brutes"):
                    st.dataframe(df_corr, use_container_width=True)
            else:
                st.info(f"Aucune donnée pour {corr_name}")
            
            st.divider()

# ============================================================================
# TAB 4: RAPPORTS
# ============================================================================

with tab4:
    st.header("📄 Rapports Quotidiens")
    
    if not reports_data or not reports_data.get("reports"):
        st.info("Aucun rapport disponible")
    else:
        reports_list = reports_data["reports"]
        
        for report_item in reports_list:
            report_type = report_item.get("report_type", "Inconnu")
            report_content = report_item.get("report", {})
            timestamp = report_item.get("timestamp", "N/A")
            
            with st.expander(f"📋 {report_type.replace('_', ' ').title()}", expanded=True):
                st.caption(f"Généré le : {timestamp}")
                
                if isinstance(report_content, dict):
                    # Afficher métriques clés
                    cols = st.columns(4)
                    metrics_shown = 0
                    for key, value in report_content.items():
                        if isinstance(value, (int, float)) and metrics_shown < 4:
                            with cols[metrics_shown]:
                                st.metric(
                                    key.replace("_", " ").title(),
                                    f"{value:,.0f}" if isinstance(value, (int, float)) else value
                                )
                                metrics_shown += 1
                    
                    # JSON complet
                    st.json(report_content)
                
                elif isinstance(report_content, list):
                    df = pd.DataFrame(report_content)
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                else:
                    st.write(report_content)

# ============================================================================
# FOOTER
# ============================================================================

st.divider()
st.markdown("""
<div style='text-align: center; color: #888; padding: 1rem;'>
    <p>🚴 <b>CityFlow Analytics Dashboard v2.0</b></p>
    <p>Données : DynamoDB | API : FastAPI | Visualisation : Streamlit</p>
</div>
""", unsafe_allow_html=True)
