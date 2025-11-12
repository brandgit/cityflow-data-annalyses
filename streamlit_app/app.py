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

# Configuration de l'URL de l'API
# En production sur EC2, utiliser l'IP publique
# En local, utiliser localhost
API_URL = os.getenv("API_URL", "http://51.44.214.181:8000")

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
    """Liste des dates disponibles pour les métriques."""
    try:
        r = requests.get(f"{API_URL}/metrics", timeout=10)
        return r.json().get("dates", []) if r.status_code == 200 else []
    except:
        return []

def get_metric_names() -> list:
    """Liste de tous les noms de métriques disponibles."""
    try:
        r = requests.get(f"{API_URL}/metrics/names", timeout=10)
        return r.json().get("metric_names", []) if r.status_code == 200 else []
    except:
        return []

def get_correlation_dates() -> list:
    """Liste des dates disponibles pour les corrélations."""
    try:
        r = requests.get(f"{API_URL}/correlations", timeout=10)
        return r.json().get("dates", []) if r.status_code == 200 else []
    except:
        return []

def get_report_dates() -> list:
    """Liste des dates disponibles pour les rapports."""
    try:
        r = requests.get(f"{API_URL}/reports", timeout=10)
        return r.json().get("dates", []) if r.status_code == 200 else []
    except:
        return []

def get_metric(date: str, name: str) -> Optional[dict]:
    """Récupère une métrique spécifique pour une date."""
    try:
        r = requests.get(f"{API_URL}/metrics/{date}/{name}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_all_metrics(date: str) -> Optional[dict]:
    """Récupère toutes les métriques pour une date."""
    try:
        r = requests.get(f"{API_URL}/metrics/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_correlations(date: str) -> Optional[dict]:
    """Récupère les corrélations pour une date."""
    try:
        r = requests.get(f"{API_URL}/correlations/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_reports(date: str) -> Optional[dict]:
    """Récupère les rapports pour une date."""
    try:
        r = requests.get(f"{API_URL}/reports/{date}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def get_specific_report(date: str, report_type: str) -> Optional[dict]:
    """Récupère un rapport spécifique pour une date."""
    try:
        r = requests.get(f"{API_URL}/reports/{date}?report_type={report_type}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def safe_dataframe(data) -> pd.DataFrame:
    """Convertit des données en DataFrame de manière sécurisée."""
    if data is None or data == [] or data == {}:
        return pd.DataFrame()
    
    # Si c'est un dict (pas une liste de dicts)
    if isinstance(data, dict):
        # Vérifier si c'est un dict avec des valeurs scalaires
        if all(not isinstance(v, (list, dict)) for v in data.values()):
            # C'est un dict scalaire (ex: {"ratio": 1.5, "semaine": 1000})
            # Le convertir en une seule ligne
            try:
                return pd.DataFrame([data])
            except:
                return pd.DataFrame()
        else:
            # C'est un dict structuré (ex: {"lundi": [...], "mardi": [...]})
            # Essayer de le convertir directement
            try:
                df = pd.DataFrame(data)
                return df
            except:
                # Si échec, essayer de le reformater
                try:
                    return pd.DataFrame([data])
                except:
                    return pd.DataFrame()
    
    # Si c'est une liste
    try:
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()

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
    df_top = safe_dataframe(metrics.get("top_compteurs", []))
    if not df_top.empty:
        total_compteurs = len(df_top)
        st.metric("🚴 Compteurs actifs", total_compteurs)
    else:
        st.metric("🚴 Compteurs", "N/A")

st.divider()

# ============================================================================
# ONGLETS PRINCIPAUX
# ============================================================================

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Vue d'Ensemble",
    "🚴 Flux Vélos", 
    "🚨 Alertes & Anomalies",
    "🔗 Corrélations",
    "📄 Rapports",
    "🔍 API Explorer"
])

# ============================================================================
# TAB 1: VUE D'ENSEMBLE
# ============================================================================

with tab1:
    st.header("📊 Vue d'Ensemble Globale")
    
    # Top Compteurs résumé
    df_top = safe_dataframe(metrics.get("top_compteurs", []))
    if not df_top.empty:
        st.subheader("🏆 Top 20 Compteurs les Plus Actifs")
        st.markdown(
            """
            *Lecture rapide* :
            - **DMJA** = moyenne des passages quotidiens sur l’année ; plus la barre est longue, plus l’axe est fréquenté.
            - Les identifiants correspondent aux compteurs vélos installés sur le domaine public parisien.
            - Cette vue aide à repérer les axes prioritaires pour l’entretien et la sécurisation des pistes.
            """
        )
        
        # Trouver la colonne de valeur
        value_col = "dmja" if "dmja" in df_top.columns else ("debit_total" if "debit_total" in df_top.columns else "debit_moyen")
        
        if value_col in df_top.columns and "compteur_id" in df_top.columns:
            fig = px.bar(
                df_top.head(20).sort_values(value_col, ascending=True),
                y="compteur_id",
                x=value_col,
                orientation="h",
                title=f"Top 20 Compteurs par {value_col.upper()}",
                labels={value_col: f"{value_col.upper()}/j", "compteur_id": "Compteur"},
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
    df_heures = safe_dataframe(metrics.get("heures_pointe", []))
    if not df_heures.empty and "heure" in df_heures.columns:
        st.subheader("⏰ Profil Horaire du Trafic")
        st.markdown(
            """
            *À retenir* :
            - Les pointes du matin et du soir correspondent aux trajets domicile ↔ travail.
            - Le creux de milieu de journée reflète la baisse d’affluence hors heures de pointe.
            - Utilisez cette courbe pour caler la régulation (services Vélib’, interventions techniques, communication).
            """
        )
        
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
    df_debit = safe_dataframe(metrics.get("debit_journalier", []))
    if not df_debit.empty and "compteur_id" in df_debit.columns and "debit_journalier" in df_debit.columns:
        st.subheader("📅 Débit Journalier par Compteur")
        st.markdown(
            """
            *Comment lire la heatmap* :
            - Chaque ligne = un compteur du top 15 ; chaque colonne = un jour.
            - **Vert** = volume élevé, **rouge** = volume faible.
            - Les variations soudaines (taches rouges) aident à détecter des incidents, travaux ou conditions météo défavorables.
            """
        )
        
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
    df_densite = safe_dataframe(metrics.get("densite_par_zone", []))
    if not df_densite.empty:
        st.subheader("🗺️ Répartition Géographique")
        st.markdown(
            """
            *Objectif* :
            - Comparer la part de trafic par arrondissement / zone.
            - Le camembert met en évidence les zones dominantes ; l’histogramme détaille le top 15.
            - Utile pour équilibrer les efforts d’aménagement entre centre et périphérie.
            """
        )
        
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
    st.markdown(
        """
        *Conseils de lecture* :
        - Chaque point représente un compteur ; la couleur et la taille sont proportionnelles au trafic observé.
        - Zoomez / changez de mode (3D, 2D, heatmap) pour explorer les couloirs très fréquentés ou vérifier l’équilibre centre ↔ périphérie.
        - Combinez cette carte avec la heatmap pour identifier les zones régulières vs. celles sujettes aux coupures.
        """
    )
    
    # Essayer de récupérer les données avec coordonnées
    df_map = None
    
    # Chercher dans densite_par_zone
    df_densite_map = safe_dataframe(metrics.get("densite_par_zone", []))
    if not df_densite_map.empty and "latitude" in df_densite_map.columns and "longitude" in df_densite_map.columns:
        df_map = df_densite_map.copy()
    
    # Sinon chercher dans top_compteurs
    if df_map is None or df_map.empty:
        df_top_map = safe_dataframe(metrics.get("top_compteurs", []))
        if not df_top_map.empty and "latitude" in df_top_map.columns and "longitude" in df_top_map.columns:
            df_map = df_top_map.copy()
    
    # Sinon chercher dans debit_journalier
    if df_map is None or df_map.empty:
        df_debit_map = safe_dataframe(metrics.get("debit_journalier", []))
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
            # Limiter à 200 points pour garder de bonnes performances
            df_map = df_map.nlargest(200, value_col) if len(df_map) > 200 else df_map

            # Normaliser les valeurs pour rayon / couleur / hauteur
            min_val = df_map[value_col].min()
            max_val = df_map[value_col].max()
            amplitude = max(max_val - min_val, 1)
            df_map["value_norm"] = ((df_map[value_col] - min_val) / amplitude).fillna(0)
            df_map["height"] = (df_map["value_norm"] * 1200).clip(lower=0).fillna(0)
            df_map["radius"] = (df_map["value_norm"] * 120 + 30).fillna(30)

            st.markdown("**Type de visualisation :**")
            viz_type = st.radio(
                "Type de rendu cartographique",
                ["Points 3D", "Points 2D", "Heatmap"],
                horizontal=True,
                label_visibility="collapsed",
            )

            try:
                view_state = pdk.ViewState(
                    longitude=float(df_map["longitude"].mean()),
                    latitude=float(df_map["latitude"].mean()),
                    zoom=11,
                    pitch=45 if viz_type == "Points 3D" else 0,
                    bearing=0,
                )

                if viz_type == "Points 3D":
                    layers = [
                        pdk.Layer(
                            "ColumnLayer",
                            data=df_map,
                            get_position=["longitude", "latitude"],
                            get_elevation="height",
                            elevation_scale=1,
                            radius=60,
                            get_fill_color="[255, (1 - value_norm) * 180, 40, 200]",
                            pickable=True,
                            auto_highlight=True,
                        )
                    ]
                    legend = (
                        "La hauteur et la couleur des colonnes reflètent le niveau de trafic ("
                        f"{value_col})."
                    )

                elif viz_type == "Points 2D":
                    layers = [
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=df_map,
                            get_position=["longitude", "latitude"],
                            get_radius="radius",
                            radius_scale=2,
                            radius_min_pixels=4,
                            radius_max_pixels=40,
                            get_fill_color="[255, (1 - value_norm) * 150, 20, 220]",
                            pickable=True,
                            auto_highlight=True,
                        )
                    ]
                    legend = (
                        "Chaque cercle représente un compteur. Taille et couleur proportionnelles à "
                        f"{value_col}."
                    )

                else:  # Heatmap
                    layers = [
                        pdk.Layer(
                            "HeatmapLayer",
                            data=df_map,
                            get_position=["longitude", "latitude"],
                            aggregation=pdk.types.String("MEAN"),
                            get_weight=value_col,
                            radius_pixels=40,
                        )
                    ]
                    legend = (
                        "La chaleur met en évidence les zones où le niveau de trafic est le plus élevé."
                    )

                deck = pdk.Deck(
                    layers=layers,
                    initial_view_state=view_state,
                    tooltip={
                        "html": "<b>Compteur :</b> {compteur_id}<br/>"
                        f"<b>{value_col} :</b> {{{value_col}}}",
                        "style": {"backgroundColor": "#0f2537", "color": "#FFFFFF"},
                    },
                )
                st.pydeck_chart(deck)
                st.caption(f"ℹ️ {legend}")

            except Exception:
                # Fallback Plotly Mapbox
                fig = px.scatter_mapbox(
                    df_map,
                    lat="latitude",
                    lon="longitude",
                    size=value_col,
                    color=value_col,
                    hover_name="compteur_id" if "compteur_id" in df_map.columns else None,
                    hover_data=[value_col],
                    color_continuous_scale="RdYlGn",
                    size_max=30,
                    zoom=11,
                    height=600,
                    title="Répartition Géographique des Compteurs Vélo",
                )
                fig.update_layout(
                    mapbox_style="carto-darkmatter",
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
    df_debit = safe_dataframe(metrics.get("debit_journalier", []))
    if not df_debit.empty:
        st.subheader("📅 Débit Journalier par Compteur")
        st.markdown(
            """
            *Ce que montre ce graphique* :
            - Suivi jour par jour des passages sur chaque compteur.
            - Les zones rouges/vertes aident à repérer des anomalies ou des pics exceptionnels.
            - Pratique pour voir l'impact d'un événement (travaux, météo) sur la fréquentation.
            """
        )
        
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
    df_dmja = safe_dataframe(metrics.get("dmja", []))
    if not df_dmja.empty:
        st.subheader("📈 DMJA (Débit Moyen Journalier Annuel)")
        st.markdown(
            """
            *Lecture rapide* :
            - Le **DMJA** représente la moyenne quotidienne de passages sur l'année.
            - Permet de classer les axes selon leur fréquentation structurelle (hors saisonnalité ponctuelle).
            - Servez-vous-en pour prioriser les investissements sur les corridors les plus demandés.
            """
        )
        
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
        df_defaillants = safe_dataframe(metrics.get("compteurs_defaillants", []))
        if not df_defaillants.empty:
            st.subheader("⚠️ Compteurs Défaillants")
            st.markdown(
                """
                *À surveiller* :
                - Compteurs sans données pendant une longue période (défaut matériel, coupure réseau).
                - Permet de prioriser les tournées de maintenance.
                """
            )
            st.metric("Nombre", len(df_defaillants))
            with st.expander("Voir la liste"):
                st.dataframe(df_defaillants, use_container_width=True)
        else:
            st.success("✅ Aucun compteur défaillant")
    
    with col2:
        df_faible = safe_dataframe(metrics.get("compteurs_faible_activite", []))
        if not df_faible.empty:
            st.subheader("📉 Faible Activité")
            st.markdown(
                """
                *Interprétation* :
                - Compteurs largement en dessous de la médiane (baisse de fréquentation ou défaut capteur).
                - Aide à distinguer une tendance de fond d’un simple incident technique.
                """
            )
            st.metric("Nombre", len(df_faible))
            with st.expander("Voir la liste"):
                st.dataframe(df_faible, use_container_width=True)
        else:
            st.success("✅ Tous les compteurs sont actifs")
    
    st.divider()
    
    # Ratio weekend/semaine
    df_ratio = safe_dataframe(metrics.get("ratio_weekend_semaine", []))
    if not df_ratio.empty:
        st.subheader("📅 Ratio Weekend / Semaine")
        st.markdown(
            """
            *Comprendre le ratio* :
            - > 1 : trafic plus fort le week-end (loisirs/tourisme).
            - < 1 : trafic principalement en semaine (trajets domicile ↔ travail).
            - Utile pour ajuster les messages ou services ciblés (événements, communication).
            """
        )
        
        required_cols = {"debit_weekend", "debit_semaine", "ratio_weekend_semaine", "difference_pct"}
        if required_cols.issubset(df_ratio.columns):
            row = df_ratio.iloc[0]
            col1, col2, col3 = st.columns(3)
            col1.metric("Débit weekend", f"{row['debit_weekend']:,.0f}")
            col2.metric("Débit semaine", f"{row['debit_semaine']:,.0f}")
            col3.metric("Ratio W/E", f"{row['ratio_weekend_semaine']:.2f}", delta=f"{row['difference_pct']:.1f}%")
            
            gauge_max = max(2.0, row["ratio_weekend_semaine"] * 1.2)
            fig = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=row["ratio_weekend_semaine"],
                    gauge={
                        "axis": {"range": [0, gauge_max]},
                        "threshold": {"line": {"color": "red", "width": 4}, "value": 1},
                        "bar": {"color": "#2ca02c"},
                    },
                    title={"text": "Ratio Weekend / Semaine"},
                )
            )
            fig.update_layout(height=260)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Ratio > 1 : trafic plus fort le week-end · Ratio < 1 : trafic plus fort en semaine")
    
    st.divider()
    
    # Débit horaire
    df_horaire = safe_dataframe(metrics.get("debit_horaire", []))
    if not df_horaire.empty:
        st.subheader("⏱️ Débit Horaire Détaillé")
        st.markdown(
            """
            *À quoi ça sert ?* :
            - Compare les compteurs selon leur charge horaire moyenne.
            - Permet d’identifier les tronçons proches de la saturation ou très volatiles.
            """
        )
        
        horaire_cols = {"compteur_id", "debit_horaire_moyen", "debit_horaire_median", "debit_horaire_max"}
        if horaire_cols.issubset(df_horaire.columns):
            top_horaire = df_horaire.nlargest(20, "debit_horaire_moyen")
            fig = px.bar(
                top_horaire.sort_values("debit_horaire_moyen"),
                x="debit_horaire_moyen",
                y="compteur_id",
                orientation="h",
                color="debit_horaire_moyen",
                color_continuous_scale="Viridis",
                title="Top 20 Compteurs par Débit Horaire Moyen",
                labels={"debit_horaire_moyen": "Débit horaire moyen", "compteur_id": "Compteur"},
                height=520
            )
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Débit horaire moyen global", f"{df_horaire['debit_horaire_moyen'].mean():,.0f}")
            col2.metric("Débit horaire max", f"{df_horaire['debit_horaire_max'].max():,.0f}")
            col3.metric("Compteurs analysés", len(df_horaire))
    
    st.divider()
    
    # Profil jour type
    df_profil = safe_dataframe(metrics.get("profil_jour_type", []))
    if not df_profil.empty:
        st.subheader("📅 Profil Jour Type")
        st.markdown(
            """
            *Lecture* :
            - Heatmap = intensité par jour de la semaine / heure de la journée.
            - Permet de visualiser les périodes à forte affluence pour planifier la régulation ou la communication.
            """
        )
        
        if {"jour", "heure"}.issubset(df_profil.columns):
            value_col = "debit_moyen" if "debit_moyen" in df_profil.columns else None
            if value_col:
                pivot = df_profil.pivot_table(
                    values=value_col,
                    index="jour",
                    columns="heure",
                    aggfunc="mean"
                )
                fig = px.imshow(
                    pivot,
                    color_continuous_scale="YlOrBr",
                    labels=dict(x="Heure", y="Jour", color="Débit moyen"),
                    title="Heatmap du Profil Jour Type",
                    aspect="auto",
                    height=520
                )
                st.plotly_chart(fig, use_container_width=True)
                
                daily_avg = df_profil.groupby("jour")[value_col].mean().reset_index()
                fig_bar = px.bar(
                    daily_avg,
                    x="jour",
                    y=value_col,
                    color=value_col,
                    color_continuous_scale="Blues",
                    title="Débit moyen par jour de la semaine",
                    labels={value_col: "Débit moyen"}
                )
                st.plotly_chart(fig_bar, use_container_width=True)
    
    st.divider()
    
    # Taux de disponibilité
    df_dispo = safe_dataframe(metrics.get("taux_disponibilite", []))
    if not df_dispo.empty:
        st.subheader("✅ Taux de Disponibilité des Compteurs")
        st.markdown(
            """
            *Pourquoi regarder ce KPI* :
            - Mesure la fiabilité des compteurs (taux de temps où ils émettent des données).
            - Un taux faible doit déclencher une intervention pour éviter les trous dans l’analyse.
            """
        )
        
        col_name = "taux_disponibilite_pct" if "taux_disponibilite_pct" in df_dispo.columns else "taux_disponibilite"
        if col_name in df_dispo.columns:
            col1, col2 = st.columns(2)
            
            with col1:
                avg_dispo = df_dispo[col_name].mean()
                st.metric("Disponibilité moyenne", f"{avg_dispo:.1f}%")
            
            with col2:
                seuil = 90
                compteurs_ok = len(df_dispo[df_dispo[col_name] >= seuil])
                st.metric(f"Compteurs ≥ {seuil}%", compteurs_ok)
            
            fig = px.histogram(
                df_dispo,
                x=col_name,
                nbins=20,
                title="Distribution du taux de disponibilité",
                labels={col_name: "Taux de disponibilité (%)"},
                color_discrete_sequence=["#2ca02c"]
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Corridors cyclables
    df_corridors = safe_dataframe(metrics.get("corridors_cyclables", []))
    if not df_corridors.empty:
        st.subheader("🚲 Corridors Cyclables Principaux")
        st.markdown(
            """
            *Utilisation* :
            - Liste les axes les plus empruntés (DMJA élevé).
            - Aide à prioriser les actions de sécurisation, signalétique ou extension d’infrastructures.
            """
        )
        
        if {"dmja", "compteur_id"}.issubset(df_corridors.columns):
            top_corridors = df_corridors.nlargest(15, "dmja")
            fig = px.bar(
                top_corridors.sort_values("dmja"),
                x="dmja",
                y="compteur_id",
                orientation="h",
                title="Top Corridors Cyclables (DMJA)",
                labels={"dmja": "DMJA", "compteur_id": "Compteur"},
                color="dmja",
                color_continuous_scale="Greens",
                height=520
            )
            st.plotly_chart(fig, use_container_width=True)
            
            if {"latitude", "longitude"}.issubset(top_corridors.columns):
                fig_map = px.scatter_mapbox(
                    top_corridors,
                    lat="latitude",
                    lon="longitude",
                    size="dmja",
                    color="dmja",
                    hover_name="compteur_id",
                    color_continuous_scale="Viridis",
                    zoom=11,
                    height=450,
                    title="Localisation des corridors cyclables",
                )
                fig_map.update_layout(mapbox_style="carto-positron")
                st.plotly_chart(fig_map, use_container_width=True)
    
    st.divider()
    
    # Évolution hebdomadaire
    df_hebdo = safe_dataframe(metrics.get("evolution_hebdomadaire", []))
    if not df_hebdo.empty:
        st.subheader("📈 Évolution Hebdomadaire")
        st.markdown(
            """
            *Message clé* :
            - Suivi du volume total semaine par semaine.
            - Les barres bleues indiquent la croissance (%) pour repérer hausses ou baisses durables.
            """
        )
        
        if {"periode", "debit_total"}.issubset(df_hebdo.columns):
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(
                go.Scatter(
                    x=df_hebdo["periode"],
                    y=df_hebdo["debit_total"],
                    mode="lines+markers",
                    name="Débit total",
                    line=dict(color="#ff7f0e", width=3),
                    marker=dict(size=8),
                ),
                secondary_y=False,
            )
            
            if "taux_croissance_pct" in df_hebdo.columns:
                fig.add_trace(
                    go.Bar(
                        x=df_hebdo["periode"],
                        y=df_hebdo["taux_croissance_pct"],
                        name="Croissance (%)",
                        marker_color="#1f77b4",
                        opacity=0.4,
                    ),
                    secondary_y=True,
                )
                fig.update_yaxes(title_text="Croissance (%)", secondary_y=True)
            
            fig.update_layout(
                height=420,
                title="Tendance hebdomadaire du trafic",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            fig.update_xaxes(title_text="Semaine")
            fig.update_yaxes(title_text="Débit total", secondary_y=False)
            st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# TAB 3: ALERTES & ANOMALIES
# ============================================================================

with tab3:
    st.header("🚨 Alertes et Détection d'Anomalies")
    
    # Anomalies
    df_anomalies = safe_dataframe(metrics.get("anomalies", []))
    if not df_anomalies.empty:
        st.subheader("🔍 Anomalies Détectées")
        st.markdown(
            """
            *Ce que signifie une anomalie* :
            - Variation statistiquement inhabituelle (z-score élevé).
            - Peut traduire un événement exceptionnel, une panne partielle ou un afflux ponctuel.
            - Examinez les 10 plus forts z-scores pour prioriser les vérifications.
            """
        )
        
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
    df_congestion = safe_dataframe(metrics.get("congestion_cyclable", []))
    if not df_congestion.empty:
        st.subheader("🔴 Zones de Congestion")
        st.markdown(
            """
            *Lecture* :
            - Met en évidence les segments où le flux dépasse largement la moyenne (risque de saturation).
            - Utile pour déclencher une veille terrain ou ajuster la signalisation temporaire.
            """
        )
        
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
    df_chantiers = safe_dataframe(metrics.get("chantiers_actifs", []))
    if not df_chantiers.empty:
        st.subheader("🚧 Chantiers Actifs")
        st.markdown(
            """
            *Objectif* :
            - Liste les chantiers en cours impactant potentiellement la circulation vélo.
            - Permet d’identifier les arrondissements les plus touchés et d’ajuster la communication terrain.
            """
        )
        
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
    df_criticite = safe_dataframe(metrics.get("score_criticite_chantiers", []))
    if not df_criticite.empty:
        st.subheader("⚠️ Criticité des Chantiers")
        st.markdown(
            """
            *Interprétation* :
            - Score basé sur le nombre de chantiers en chaussée et la surface impactée.
            - Les valeurs élevées doivent déclencher une coordination renforcée avec les services travaux / circulation.
            """
        )
        
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
    df_qualite = safe_dataframe(metrics.get("qualite_service", []))
    if not df_qualite.empty:
        st.subheader("✨ Qualité de Service")
        st.markdown(
            """
            *Ce que mesure cet indicateur* :
            - Agrège les scores qualité (transport, exploitants…) fournis par les opérateurs.
            - Permet de détecter les lignes/services en dessous des attentes et de suivre l’évolution des pénalités éventuelles.
            """
        )
        
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
    st.markdown(
        """
        *Pourquoi c’est utile* :
        - Les corrélations quantifient la relation entre les flux vélo et d’autres facteurs (chantiers, météo, validations transports…).
        - Chaque section détaille la force du lien et l’évolution conjointe des métriques.
        """
    )
    
    if not correlations_data or not correlations_data.get("correlations"):
        st.info("Aucune corrélation disponible pour cette date")
    else:
        for corr_item in correlations_data["correlations"]:
            corr_name = corr_item.get("correlation_name", "Inconnue")
            corr_data = corr_item.get("data", [])
            
            st.subheader(f"🔗 {corr_name.replace('_', ' ').title()}")
            explanations = {
                "chantiers_velo": """
                    *Lecture* :
                    - Mesure comment les chantiers influent sur la fréquentation vélo.
                    - Compare les volumes de passages avec le nombre de chantiers actifs.
                    """,
                "meteo_velo": """
                    *Lecture* :
                    - Analyse l’effet de la météo (température, pluie) sur le trafic vélo.
                    - Permet d’anticiper les variations saisonnières ou les journées à risque.
                    """,
                "qualite_validations": """
                    *Lecture* :
                    - Croise les scores de qualité de service avec les validations billettiques (transports).
                    - Utile pour voir si une amélioration/dégradation de la qualité se traduit par un changement d’usage.
                    """,
            }
            generic_explanation = """
                *Lecture* :
                - Coefficient de corrélation proche de 1 : relation positive forte ; proche de -1 : relation inverse.
                - Aide à vérifier les liens entre le trafic vélo et d’autres facteurs (événements, météo, qualité…).
                """
            st.markdown(explanations.get(corr_name, generic_explanation))
            
            if isinstance(corr_data, list) and len(corr_data) > 0:
                df_corr = safe_dataframe(corr_data)

                if corr_name == "chantiers_velo" and not df_corr.empty:
                    st.caption("Impact des chantiers sur la fréquentation vélo (corrélation quotidienne).")

                    corr_value = df_corr.get("correlation_chantiers_velo")
                    if corr_value is not None and not corr_value.empty:
                        st.metric("Coefficient de corrélation", f"{corr_value.iloc[0]:.3f}")

                    if {"date", "total_velos", "nb_chantiers_actifs"}.issubset(df_corr.columns):
                        df_corr["date"] = pd.to_datetime(df_corr["date"])
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        fig.add_trace(
                            go.Scatter(
                                x=df_corr["date"],
                                y=df_corr["total_velos"],
                                mode="lines+markers",
                                name="Total vélos",
                                line=dict(color="#1f77b4"),
                            ),
                            secondary_y=False,
                        )
                        fig.add_trace(
                            go.Bar(
                                x=df_corr["date"],
                                y=df_corr["nb_chantiers_actifs"],
                                name="Chantiers actifs",
                                marker_color="#ff7f0e",
                                opacity=0.6,
                            ),
                            secondary_y=True,
                        )
                        fig.update_layout(
                            title="Trafic vélo vs Chantiers actifs",
                            height=420,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        )
                        fig.update_xaxes(title_text="Date")
                        fig.update_yaxes(title_text="Total vélos", secondary_y=False)
                        fig.update_yaxes(title_text="Chantiers actifs", secondary_y=True)
                        st.plotly_chart(fig, use_container_width=True)

                elif corr_name == "meteo_velo" and not df_corr.empty:
                    st.caption("Corrélation météo ↔ trafic vélo.")

                    corr_temp = df_corr.get("correlation_temperature")
                    corr_precip = df_corr.get("correlation_precipitation")
                    cols = st.columns(2)
                    if corr_temp is not None and not corr_temp.empty:
                        cols[0].metric("Corrélation température", f"{corr_temp.iloc[0]:.3f}")
                    if corr_precip is not None and not corr_precip.empty:
                        cols[1].metric("Corrélation précipitations", f"{corr_precip.iloc[0]:.3f}")

                    if {"date", "total_velos", "temperature_max", "precipitation"}.issubset(df_corr.columns):
                        df_corr["date"] = pd.to_datetime(df_corr["date"])
                        fig = make_subplots(specs=[[{"secondary_y": True}]])
                        fig.add_trace(
                            go.Scatter(
                                x=df_corr["date"],
                                y=df_corr["total_velos"],
                                mode="lines+markers",
                                name="Total vélos",
                                line=dict(color="#1f77b4"),
                            ),
                            secondary_y=False,
                        )
                        fig.add_trace(
                            go.Scatter(
                                x=df_corr["date"],
                                y=df_corr["temperature_max"],
                                mode="lines",
                                name="Température max (°C)",
                                line=dict(color="#d62728", dash="dash"),
                            ),
                            secondary_y=True,
                        )
                        fig.update_layout(
                            title="Trafic vélo vs Température",
                            height=420,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        )
                        fig.update_xaxes(title_text="Date")
                        fig.update_yaxes(title_text="Total vélos", secondary_y=False)
                        fig.update_yaxes(title_text="Température (°C)", secondary_y=True)
                        st.plotly_chart(fig, use_container_width=True)

                        fig_precip = go.Figure()
                        fig_precip.add_trace(
                            go.Bar(
                                x=df_corr["date"],
                                y=df_corr["precipitation"],
                                name="Précipitations (mm)",
                                marker_color="#17becf",
                            )
                        )
                        fig_precip.update_layout(
                            title="Précipitations quotidiennes",
                            height=320,
                            yaxis_title="mm",
                        )
                        st.plotly_chart(fig_precip, use_container_width=True)

                elif corr_name == "qualite_validations" and not df_corr.empty:
                    st.caption("Évolution de la qualité de service vs validations.")

                    if {"periode", "score_moyen_qualite"}.issubset(df_corr.columns):
                        fig = px.line(
                            df_corr,
                            x="periode",
                            y="score_moyen_qualite",
                            markers=True,
                            title="Score moyen de qualité de service",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Pas assez d'informations pour générer un visuel pertinent.")

                elif "correlation" in df_corr.columns:
                    fig = go.Figure()
                    fig.add_trace(
                        go.Bar(
                            x=df_corr.iloc[:, 0],
                            y=df_corr["correlation"],
                            marker=dict(
                                color=df_corr["correlation"],
                                colorscale="RdBu",
                                cmin=-1,
                                cmax=1,
                                colorbar=dict(title="Corrélation"),
                            ),
                        )
                    )
                    fig.update_layout(
                        title=f"Valeurs de Corrélation - {corr_name}",
                        xaxis_title=df_corr.columns[0],
                        yaxis_title="Coefficient de Corrélation",
                        yaxis=dict(range=[-1, 1]),
                        height=400,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                else:
                    st.info("Pas de structure exploitable pour un graphique. Données brutes affichées ci-dessous.")

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
    st.markdown(
        """
        *Contenu des rapports* :
        - Synthèse automatique des métriques calculées (qualité, volumes, alertes).
        - Utilisez ces blocs pour extraire rapidement des chiffres clés lors d’une présentation ou d’un reporting.
        """
    )
    
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
                    df = safe_dataframe(report_content)
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                else:
                    st.write(report_content)

# ============================================================================
# TAB 6: API EXPLORER
# ============================================================================

with tab6:
    st.header("🔍 API Explorer")
    st.markdown("Explorez tous les endpoints disponibles de l'API CityFlow Analytics")
    
    # Section 1: Informations générales
    st.subheader("📡 Informations API")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🌐 URL de l'API", API_URL.replace("http://", "").replace(":8000", ""))
        if check_api():
            st.success("✅ API Connectée")
        else:
            st.error("❌ API Déconnectée")
    
    with col2:
        metric_names = get_metric_names()
        st.metric("📊 Métriques disponibles", len(metric_names))
    
    with col3:
        dates = get_dates()
        st.metric("📅 Dates disponibles", len(dates))
    
    st.divider()
    
    # Section 2: Liste des endpoints
    st.subheader("🔗 Endpoints Disponibles")
    
    endpoints_data = {
        "Catégorie": [
            "Health", 
            "Métriques", "Métriques", "Métriques", "Métriques",
            "Corrélations", "Corrélations",
            "Rapports", "Rapports", "Rapports"
        ],
        "Endpoint": [
            "/health",
            "/metrics", "/metrics/names", "/metrics/{date}", "/metrics/{date}/{metric_name}",
            "/correlations", "/correlations/{date}",
            "/reports", "/reports/{date}", "/reports/{date}?report_type=..."
        ],
        "Description": [
            "État de santé de l'API",
            "Liste des dates (métriques)", "Liste des noms de métriques", 
            "Toutes les métriques d'une date", "Une métrique spécifique",
            "Liste des dates (corrélations)", "Corrélations d'une date",
            "Liste des dates (rapports)", "Tous les rapports d'une date", "Un rapport spécifique"
        ],
        "URL": [
            f"{API_URL}/health",
            f"{API_URL}/metrics", f"{API_URL}/metrics/names",
            f"{API_URL}/metrics/{{date}}", f"{API_URL}/metrics/{{date}}/{{metric_name}}",
            f"{API_URL}/correlations", f"{API_URL}/correlations/{{date}}",
            f"{API_URL}/reports", f"{API_URL}/reports/{{date}}", f"{API_URL}/reports/{{date}}?report_type=..."
        ]
    }
    
    df_endpoints = pd.DataFrame(endpoints_data)
    st.dataframe(df_endpoints, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Section 3: Testeur d'endpoint
    st.subheader("🧪 Testeur d'Endpoint")
    
    test_col1, test_col2 = st.columns([2, 1])
    
    with test_col1:
        endpoint_category = st.selectbox(
            "Catégorie",
            ["Métriques", "Corrélations", "Rapports"]
        )
    
    with test_col2:
        test_date = st.selectbox("Date", dates if dates else ["2025-11-11"])
    
    if endpoint_category == "Métriques":
        metric_option = st.radio(
            "Type de requête",
            ["Toutes les métriques", "Une métrique spécifique"]
        )
        
        if metric_option == "Toutes les métriques":
            if st.button("🚀 Tester /metrics/{date}"):
                with st.spinner("Chargement..."):
                    result = get_all_metrics(test_date)
                    if result:
                        st.success(f"✅ {result.get('metrics_count', 0)} métriques trouvées")
                        st.json(result)
                    else:
                        st.error("❌ Aucune donnée")
        else:
            metric_names = get_metric_names()
            selected_metric = st.selectbox("Métrique", metric_names if metric_names else ["debit_journalier"])
            
            if st.button(f"🚀 Tester /metrics/{test_date}/{selected_metric}"):
                with st.spinner("Chargement..."):
                    result = get_metric(test_date, selected_metric)
                    if result:
                        st.success("✅ Métrique trouvée")
                        st.json(result)
                    else:
                        st.error("❌ Métrique introuvable")
    
    elif endpoint_category == "Corrélations":
        if st.button(f"🚀 Tester /correlations/{test_date}"):
            with st.spinner("Chargement..."):
                result = get_correlations(test_date)
                if result:
                    st.success(f"✅ {result.get('correlations_count', 0)} corrélations trouvées")
                    st.json(result)
                else:
                    st.error("❌ Aucune corrélation")
    
    elif endpoint_category == "Rapports":
        report_option = st.radio(
            "Type de requête",
            ["Tous les rapports", "Un rapport spécifique"]
        )
        
        if report_option == "Tous les rapports":
            if st.button(f"🚀 Tester /reports/{test_date}"):
                with st.spinner("Chargement..."):
                    result = get_reports(test_date)
                    if result:
                        st.success(f"✅ {result.get('reports_count', 0)} rapports trouvés")
                        st.json(result)
                    else:
                        st.error("❌ Aucun rapport")
        else:
            report_type = st.selectbox(
                "Type de rapport",
                ["processing_report", "metrics_summary", "rapport_quotidien"]
            )
            
            if st.button(f"🚀 Tester /reports/{test_date}?report_type={report_type}"):
                with st.spinner("Chargement..."):
                    result = get_specific_report(test_date, report_type)
                    if result:
                        st.success("✅ Rapport trouvé")
                        st.json(result)
                    else:
                        st.error("❌ Rapport introuvable")
    
    st.divider()
    
    # Section 4: Documentation
    st.subheader("📚 Documentation")
    st.markdown(f"""
    **Documentation interactive Swagger :**  
    [{API_URL}/docs]({API_URL}/docs)
    
    **Liste complète des métriques CityFlow :**
    """)
    
    metric_names = get_metric_names()
    if metric_names:
        cols = st.columns(3)
        for idx, metric_name in enumerate(metric_names):
            with cols[idx % 3]:
                st.markdown(f"- `{metric_name}`")
    else:
        st.info("Liste des métriques non disponible")

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
