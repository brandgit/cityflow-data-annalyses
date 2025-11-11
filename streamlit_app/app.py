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
import pydeck as pdk
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

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'Ensemble",
    "🚴 Flux Vélos", 
    "🚨 Alertes & Anomalies",
    "🔗 Corrélations",
    "📄 Rapports"
])

# ============================================================================
# TAB 1: VUE D'ENSEMBLE
# ============================================================================

with tab1:
    st.header("📊 Vue d'Ensemble Globale")
    
    # Top Compteurs résumé
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
    
    st.divider()
    
    # CARTOGRAPHIE
    st.subheader("🗺️ Cartographie Interactive - Compteurs Vélo Paris")
    
    # Essayer de récupérer les données avec coordonnées
    df_map = None
    
    # Chercher dans densite_par_zone
    df_densite_map = pd.DataFrame(metrics.get("densite_par_zone", []))
    if not df_densite_map.empty and "latitude" in df_densite_map.columns and "longitude" in df_densite_map.columns:
        df_map = df_densite_map.copy()
    
    # Sinon chercher dans top_compteurs
    if df_map is None or df_map.empty:
        df_top_map = pd.DataFrame(metrics.get("top_compteurs", []))
        if not df_top_map.empty and "latitude" in df_top_map.columns and "longitude" in df_top_map.columns:
            df_map = df_top_map.copy()
    
    # Sinon chercher dans debit_journalier
    if df_map is None or df_map.empty:
        df_debit_map = pd.DataFrame(metrics.get("debit_journalier", []))
        if not df_debit_map.empty and "latitude" in df_debit_map.columns and "longitude" in df_debit_map.columns:
            df_map = df_debit_map.copy()
    
    if df_map is not None and not df_map.empty:
        # Nettoyer les données
        df_map = df_map.dropna(subset=["latitude", "longitude"])
        
        # Trouver la colonne de valeur pour la taille/couleur
        value_col = None
        for col in ["debit_total", "dmja", "debit_journalier", "debit_moyen"]:
            if col in df_map.columns:
                value_col = col
                break
        
        if value_col and len(df_map) > 0:
            # Limiter à 200 points pour la performance
            df_map = df_map.nlargest(200, value_col) if len(df_map) > 200 else df_map
            
            # Option 1: PyDeck (3D, plus impressionnant)
            try:
                # Normaliser les valeurs pour la hauteur
                max_val = df_map[value_col].max()
                df_map["height"] = (df_map[value_col] / max_val * 1000).fillna(0)
                
                layer = pdk.Layer(
                    "HexagonLayer",
                    df_map,
                    get_position=["longitude", "latitude"],
                    auto_highlight=True,
                    elevation_scale=50,
                    pickable=True,
                    elevation_range=[0, 1000],
                    extruded=True,
                    coverage=0.8,
                    radius=100,
                    get_fill_color="[255, (1 - height / 1000) * 255, 0]",
                )
                
                view_state = pdk.ViewState(
                    longitude=2.3522,
                    latitude=48.8566,
                    zoom=11,
                    pitch=50,
                    bearing=0
                )
                
                deck = pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    tooltip={"text": f"Compteur\nDébit: {{{value_col}}}"}
                )
                
                st.pydeck_chart(deck)
            except Exception as e:
                # Fallback: Plotly Mapbox (2D, plus simple)
                fig = px.scatter_mapbox(
                    df_map,
                    lat="latitude",
                    lon="longitude",
                    size=value_col,
                    color=value_col,
                    hover_name="compteur_id" if "compteur_id" in df_map.columns else None,
                    hover_data=[value_col],
                    color_continuous_scale="RdYlGn",
                    size_max=20,
                    zoom=11,
                    height=600,
                    title="Répartition Géographique des Compteurs Vélo"
                )
                
                fig.update_layout(
                    mapbox_style="open-street-map",
                    mapbox=dict(center=dict(lat=48.8566, lon=2.3522)),
                )
                
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📍 Coordonnées GPS disponibles mais sans données de débit")
    else:
        st.warning("📍 Pas de coordonnées GPS disponibles dans les données")
        st.caption("Les données de compteurs doivent contenir les colonnes 'latitude' et 'longitude'")

# ============================================================================
# TAB 2: FLUX VÉLOS DÉTAILLÉ
# ============================================================================

with tab2:
    st.header("🚴 Analyse Détaillée des Flux Vélos")
    
    # Débit journalier détaillé
    df_debit = pd.DataFrame(metrics.get("debit_journalier", []))
    if not df_debit.empty:
        st.subheader("📅 Débit Journalier par Compteur")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if "debit_journalier" in df_debit.columns:
                st.metric("Débit Moyen", f"{df_debit['debit_journalier'].mean():,.0f}")
        with col2:
            if "debit_journalier" in df_debit.columns:
                st.metric("Débit Max", f"{df_debit['debit_journalier'].max():,.0f}")
        with col3:
            if "compteur_id" in df_debit.columns:
                st.metric("Compteurs Actifs", len(df_debit["compteur_id"].unique()))
        
        # Heatmap ou graphique selon données
        if "compteur_id" in df_debit.columns and "debit_journalier" in df_debit.columns:
            top_20 = df_debit.groupby("compteur_id")["debit_journalier"].sum().nlargest(20).index
            df_filtered = df_debit[df_debit["compteur_id"].isin(top_20)]
            
            if "date" in df_filtered.columns:
                # Heatmap si dates disponibles
                pivot = df_filtered.pivot_table(
                    values="debit_journalier",
                    index="compteur_id",
                    columns="date",
                    aggfunc="sum"
                )
                
                fig = px.imshow(
                    pivot,
                    labels=dict(x="Date", y="Compteur", color="Débit"),
                    title="Heatmap du Débit Journalier (Top 20 Compteurs)",
                    color_continuous_scale="RdYlGn",
                    aspect="auto",
                    height=600
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Barres sinon
                df_sum = df_filtered.groupby("compteur_id")["debit_journalier"].sum().reset_index()
                fig = px.bar(
                    df_sum.sort_values("debit_journalier", ascending=True),
                    y="compteur_id",
                    x="debit_journalier",
                    orientation="h",
                    title="Débit Journalier Total (Top 20 Compteurs)",
                    color="debit_journalier",
                    color_continuous_scale="Blues",
                    height=600
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Statistiques détaillées
        with st.expander("📊 Statistiques Détaillées"):
            st.dataframe(df_debit.describe(), use_container_width=True)
    
    st.divider()
    
    # DMJA (Débit Moyen Journalier Annuel)
    df_dmja = pd.DataFrame(metrics.get("dmja", []))
    if not df_dmja.empty:
        st.subheader("📈 DMJA (Débit Moyen Journalier Annuel)")
        
        if "dmja" in df_dmja.columns and "compteur_id" in df_dmja.columns:
            top_15 = df_dmja.nlargest(15, "dmja")
            
            fig = px.bar(
                top_15.sort_values("dmja", ascending=True),
                y="compteur_id",
                x="dmja",
                orientation="h",
                title="Top 15 Compteurs par DMJA",
                labels={"dmja": "DMJA", "compteur_id": "Compteur"},
                color="dmja",
                color_continuous_scale="Greens",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Compteurs défaillants et faible activité
    col1, col2 = st.columns(2)
    
    with col1:
        df_defaillants = pd.DataFrame(metrics.get("compteurs_defaillants", []))
        if not df_defaillants.empty:
            st.subheader("⚠️ Compteurs Défaillants")
            st.metric("Nombre", len(df_defaillants))
            with st.expander("Voir la liste"):
                st.dataframe(df_defaillants, use_container_width=True)
        else:
            st.success("✅ Aucun compteur défaillant")
    
    with col2:
        df_faible = pd.DataFrame(metrics.get("compteurs_faible_activite", []))
        if not df_faible.empty:
            st.subheader("📉 Faible Activité")
            st.metric("Nombre", len(df_faible))
            with st.expander("Voir la liste"):
                st.dataframe(df_faible, use_container_width=True)
        else:
            st.success("✅ Tous les compteurs sont actifs")
    
    st.divider()
    
    # Ratio weekend/semaine
    df_ratio = pd.DataFrame(metrics.get("ratio_weekend_semaine", []))
    if not df_ratio.empty:
        st.subheader("📅 Ratio Weekend / Semaine")
        
        if "ratio" in df_ratio.columns and "compteur_id" in df_ratio.columns:
            fig = px.scatter(
                df_ratio.head(30),
                x="compteur_id",
                y="ratio",
                size="ratio",
                color="ratio",
                title="Ratio Weekend/Semaine par Compteur",
                labels={"ratio": "Ratio", "compteur_id": "Compteur"},
                color_continuous_scale="RdYlGn",
                height=400
            )
            fig.add_hline(y=1, line_dash="dash", line_color="red", annotation_text="Équilibre")
            st.plotly_chart(fig, use_container_width=True)
            
            st.caption("Ratio > 1 : Plus d'activité le weekend | Ratio < 1 : Plus d'activité en semaine")
    
    st.divider()
    
    # Débit horaire
    df_horaire = pd.DataFrame(metrics.get("debit_horaire", []))
    if not df_horaire.empty:
        st.subheader("⏱️ Débit Horaire Détaillé")
        
        if "heure" in df_horaire.columns:
            value_col = None
            for col in ["debit_horaire", "debit_moyen", "comptage"]:
                if col in df_horaire.columns:
                    value_col = col
                    break
            
            if value_col:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_horaire["heure"],
                    y=df_horaire[value_col],
                    mode='lines+markers',
                    line=dict(color='#2ca02c', width=2),
                    marker=dict(size=6),
                    fill='tonexty'
                ))
                fig.update_layout(
                    title="Évolution Horaire du Trafic",
                    xaxis_title="Heure",
                    yaxis_title=value_col.replace("_", " ").title(),
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Profil jour type
    df_profil = pd.DataFrame(metrics.get("profil_jour_type", []))
    if not df_profil.empty:
        st.subheader("📅 Profil Jour Type")
        
        if "jour_type" in df_profil.columns:
            value_col = None
            for col in ["debit_moyen", "comptage_moyen", "trafic"]:
                if col in df_profil.columns:
                    value_col = col
                    break
            
            if value_col:
                fig = px.bar(
                    df_profil,
                    x="jour_type",
                    y=value_col,
                    title="Comparaison des Profils de Circulation",
                    labels={"jour_type": "Type de Jour", value_col: value_col.replace("_", " ").title()},
                    color=value_col,
                    color_continuous_scale="Blues"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Taux de disponibilité
    df_dispo = pd.DataFrame(metrics.get("taux_disponibilite", []))
    if not df_dispo.empty:
        st.subheader("✅ Taux de Disponibilité des Compteurs")
        
        if "taux_disponibilite" in df_dispo.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                avg_dispo = df_dispo["taux_disponibilite"].mean()
                st.metric("Disponibilité Moyenne", f"{avg_dispo:.1f}%")
            
            with col2:
                compteurs_ok = len(df_dispo[df_dispo["taux_disponibilite"] >= 90])
                st.metric("Compteurs > 90%", compteurs_ok)
            
            if "compteur_id" in df_dispo.columns:
                fig = px.histogram(
                    df_dispo,
                    x="taux_disponibilite",
                    nbins=20,
                    title="Distribution du Taux de Disponibilité",
                    labels={"taux_disponibilite": "Taux de Disponibilité (%)"},
                    color_discrete_sequence=["#2ca02c"]
                )
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Corridors cyclables
    df_corridors = pd.DataFrame(metrics.get("corridors_cyclables", []))
    if not df_corridors.empty:
        st.subheader("🛣️ Corridors Cyclables Principaux")
        
        if "corridor" in df_corridors.columns or "axe" in df_corridors.columns:
            corridor_col = "corridor" if "corridor" in df_corridors.columns else "axe"
            value_col = None
            for col in ["trafic_total", "debit_moyen", "frequence"]:
                if col in df_corridors.columns:
                    value_col = col
                    break
            
            if value_col:
                top_10 = df_corridors.nlargest(10, value_col)
                fig = px.bar(
                    top_10.sort_values(value_col, ascending=True),
                    y=corridor_col,
                    x=value_col,
                    orientation="h",
                    title="Top 10 Corridors Cyclables",
                    color=value_col,
                    color_continuous_scale="Greens"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Évolution hebdomadaire
    df_hebdo = pd.DataFrame(metrics.get("evolution_hebdomadaire", []))
    if not df_hebdo.empty:
        st.subheader("📊 Évolution Hebdomadaire")
        
        if "semaine" in df_hebdo.columns or "jour" in df_hebdo.columns:
            x_col = "semaine" if "semaine" in df_hebdo.columns else "jour"
            value_col = None
            for col in ["trafic_total", "debit_moyen", "comptage"]:
                if col in df_hebdo.columns:
                    value_col = col
                    break
            
            if value_col:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_hebdo[x_col],
                    y=df_hebdo[value_col],
                    mode='lines+markers',
                    line=dict(color='#ff7f0e', width=3),
                    marker=dict(size=8)
                ))
                fig.update_layout(
                    title="Tendance Hebdomadaire du Trafic",
                    xaxis_title=x_col.title(),
                    yaxis_title=value_col.replace("_", " ").title(),
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# TAB 3: ALERTES & ANOMALIES
# ============================================================================

with tab3:
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
    
    st.divider()
    
    # Chantiers actifs
    df_chantiers = pd.DataFrame(metrics.get("chantiers_actifs", []))
    if not df_chantiers.empty:
        st.subheader("🚧 Chantiers Actifs")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Nombre de Chantiers", len(df_chantiers))
        
        with col2:
            if "arrondissement" in df_chantiers.columns:
                most_impacted = df_chantiers["arrondissement"].mode()[0] if not df_chantiers["arrondissement"].mode().empty else "N/A"
                st.metric("Arrondissement le + impacté", most_impacted)
        
        if "arrondissement" in df_chantiers.columns:
            chantiers_by_arr = df_chantiers["arrondissement"].value_counts().head(10)
            fig = px.bar(
                x=chantiers_by_arr.values,
                y=chantiers_by_arr.index,
                orientation="h",
                title="Chantiers par Arrondissement",
                labels={"x": "Nombre de Chantiers", "y": "Arrondissement"},
                color=chantiers_by_arr.values,
                color_continuous_scale="Oranges"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("📋 Liste des chantiers"):
            st.dataframe(df_chantiers, use_container_width=True)
    
    st.divider()
    
    # Score criticité chantiers
    df_criticite = pd.DataFrame(metrics.get("score_criticite_chantiers", []))
    if not df_criticite.empty:
        st.subheader("⚠️ Criticité des Chantiers")
        
        if "score_criticite" in df_criticite.columns:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_score = df_criticite["score_criticite"].mean()
                st.metric("Score Moyen", f"{avg_score:.1f}")
            
            with col2:
                max_score = df_criticite["score_criticite"].max()
                st.metric("Score Max", f"{max_score:.1f}")
            
            with col3:
                high_crit = len(df_criticite[df_criticite["score_criticite"] >= 70])
                st.metric("Chantiers Critiques", high_crit)
            
            # Top 10 chantiers critiques
            if "chantier_id" in df_criticite.columns or "arrondissement" in df_criticite.columns:
                id_col = "chantier_id" if "chantier_id" in df_criticite.columns else "arrondissement"
                top_10 = df_criticite.nlargest(10, "score_criticite")
                
                fig = px.bar(
                    top_10.sort_values("score_criticite", ascending=True),
                    y=id_col,
                    x="score_criticite",
                    orientation="h",
                    title="Top 10 Chantiers les Plus Critiques",
                    color="score_criticite",
                    color_continuous_scale="Reds"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Qualité de service
    df_qualite = pd.DataFrame(metrics.get("qualite_service", []))
    if not df_qualite.empty:
        st.subheader("✨ Qualité de Service")
        
        if "qualite" in df_qualite.columns or "score" in df_qualite.columns:
            quality_col = "qualite" if "qualite" in df_qualite.columns else "score"
            
            col1, col2 = st.columns(2)
            
            with col1:
                if pd.api.types.is_numeric_dtype(df_qualite[quality_col]):
                    avg_quality = df_qualite[quality_col].mean()
                    st.metric("Score Moyen de Qualité", f"{avg_quality:.1f}%")
            
            with col2:
                if "service" in df_qualite.columns:
                    st.metric("Services Évalués", len(df_qualite))
            
            # Graphique
            if "service" in df_qualite.columns or "ligne" in df_qualite.columns:
                service_col = "service" if "service" in df_qualite.columns else "ligne"
                
                fig = px.bar(
                    df_qualite.head(15),
                    x=service_col,
                    y=quality_col,
                    title="Qualité de Service par Ligne/Service",
                    color=quality_col,
                    color_continuous_scale="RdYlGn"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("📊 Détails"):
                st.dataframe(df_qualite, use_container_width=True)

# ============================================================================
# TAB 4: CORRÉLATIONS
# ============================================================================

with tab4:
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
# TAB 5: RAPPORTS
# ============================================================================

with tab5:
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
