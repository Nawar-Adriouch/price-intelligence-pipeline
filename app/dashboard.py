import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
import os

# --- Configuration de la page Streamlit ---
st.set_page_config(
    page_title="Price Intelligence Dashboard",
    page_icon="📈",
    layout="wide"
)

# Application d'un style personnalisé inspiré du dark mode
st.markdown("""
    <style>
    .main {
        background-color: #121212;
        color: #FFFFFF;
    }
    .metric-card {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #333333;
    }
    </style>
""", unsafe_allow_html=True)

# --- Connexion à la base de données MySQL ---
@st.cache_resource
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", 3307)),
        database=os.getenv("MYSQL_DATABASE", "pricetracker"),
        user=os.getenv("MYSQL_USER", "tracker"),
        password=os.getenv("MYSQL_PASSWORD", "admin1234")
    )

def load_data():
    conn = get_db_connection()
    # Requête pour récupérer les derniers prix et les lier aux dimensions
    query = """
        SELECT 
            p.name AS product_name,
            p.category,
            s.name AS source_name,
            f.collected_at,
            f.price,
            f.discount_pct,
            f.currency
        FROM fact_price f
        JOIN dim_product p ON f.product_id = p.product_id
        JOIN dim_source s ON f.source_id = s.source_id
        ORDER BY f.collected_at DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Chargement des données
try:
    df = load_data()
except Exception as e:
    st.error(f"Erreur de connexion à la base de données : {e}")
    st.stop()

# --- En-tête du Dashboard ---
st.title("🛒 Price Intelligence & Tracking Dashboard")
st.markdown("Suivi en temps réel de l'évolution des prix et analyse comparative multi-sources.")

if df.empty:
    st.warning("Aucune donnée disponible dans la table `fact_price`. Lancez d'abord le pipeline !")
    st.stop()

# --- Barre latérale (Filtres) ---
st.sidebar.header("Filtres d'analyse")
selected_category = st.sidebar.selectbox("Filtrer par Catégorie", options=["Toutes"] + list(df['category'].dropna().unique()))
selected_product = st.sidebar.selectbox("Filtrer par Produit", options=["Tous"] + list(df['product_name'].unique()))

filtered_df = df.copy()
if selected_category != "Toutes":
    filtered_df = filtered_df[filtered_df['category'] == selected_category]
if selected_product != "Tous":
    filtered_df = filtered_df[filtered_df['product_name'] == selected_product]

# --- KPIs en haut de page ---
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_obs = len(filtered_df)
    st.metric(label="📊 Observations Totales", value=total_obs)

with col2:
    unique_products = filtered_df['product_name'].nunique()
    st.metric(label="📦 Produits Suivis", value=unique_products)

with col3:
    unique_sources = filtered_df['source_name'].nunique()
    st.metric(label="🌐 Sources Actives", value=unique_sources)

with col4:
    avg_price = filtered_df['price'].mean() if not filtered_df.empty else 0
    st.metric(label="💰 Prix Moyen Global", value=f"{avg_price:.2f} MAD")

st.markdown("---")

# --- Graphiques analytiques (Style Sombre / Plotly) ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📉 Évolution des prix par Source")
    if not filtered_df.empty:
        fig_evolution = px.line(
            filtered_df, 
            x="collected_at", 
            y="price", 
            color="source_name", 
            markers=True,
            title="Historique des prix dans le temps",
            template="plotly_dark"
        )
        st.plotly_chart(fig_evolution, use_container_width=True)
    else:
        st.info("Pas assez de données pour afficher l'évolution.")

with col_right:
    st.subheader("🏆 Comparatif : Quel site est le moins cher ?")
    if not filtered_df.empty:
        # Dernier prix connu par produit et par source
        latest_prices = filtered_df.sort_values('collected_at').groupby(['product_name', 'source_name'], as_index=False).last()
        fig_bar = px.bar(
            latest_prices, 
            x="product_name", 
            y="price", 
            color="source_name", 
            barmode="group",
            title="Dernier prix par produit et par site",
            template="plotly_dark"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Pas assez de données pour le comparatif.")

# --- Tableau détaillé des données ---
st.subheader("📋 Données brutes filtrées")
st.dataframe(filtered_df, use_container_width=True)