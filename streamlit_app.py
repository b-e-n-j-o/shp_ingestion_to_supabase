# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
from sqlalchemy import text
from utils_db import (
    get_engine, list_postgis_tables, table_overview, upload_shapefile_zip
)

st.set_page_config(page_title="PostGIS Browser + Uploader", layout="wide")

# Connexion
engine = get_engine()  # lit .env (SUPABASE_HOST, USER, PASSWORD, DB, PORT)

st.title("PostGIS (Supabase) — Browser & Shapefile Uploader")

# === Debug connexion ===
if st.checkbox("🔧 Debug connexion"):
    st.json({
        "host": os.getenv("SUPABASE_HOST"),
        "port": os.getenv("SUPABASE_PORT"),
        "db": os.getenv("SUPABASE_DB"),
        "user": os.getenv("SUPABASE_USER"),
    })
    try:
        with engine.connect() as conn:
            v = conn.execute(text("select version()")).scalar()
        st.success("Connexion OK")
        st.caption(v)
    except Exception as e:
        st.error("Échec de connexion")
        st.exception(e)

# === Tables géométriques ===
tables = list_postgis_tables(engine)  # [{schema, table, geom_column, geom_type, srid}]

if not tables:
    st.warning("Aucune table PostGIS détectée.")
else:
    # Créer un dictionnaire pour la recherche rapide
    table_dict = {t['table']: t for t in tables}
    table_names = list(table_dict.keys())
    
    # Menu déroulant avec recherche en temps réel
    def format_table_option(name):
        table_info = table_dict[name]
        return f"{name} ({table_info['geom_column']} :: {table_info['geom_type']}, SRID={table_info['srid']})"
    
    # Utiliser un selectbox avec recherche intégrée
    selected_table = st.selectbox(
        "🔍 Rechercher et sélectionner une table géométrique",
        options=table_names,
        format_func=format_table_option,
        placeholder="Commencez à taper pour rechercher...",
        help="Tapez pour filtrer les tables disponibles en temps réel",
        index=None  # Aucune sélection par défaut
    )
    
    if selected_table:
        tsel = table_dict[selected_table]
        with st.spinner("Chargement aperçu…"):
            meta = table_overview(engine, tsel["schema"], tsel["table"], tsel["geom_column"])

        c1, c2 = st.columns(2)
        c1.metric("Lignes", f"{meta['row_count']:,}")
        c2.write("**Colonnes**")
        c2.dataframe(pd.DataFrame(meta["columns"], columns=["column", "type"]), use_container_width=True)

        st.write("**Aperçu (WKT tronqué)**")
        st.dataframe(meta["preview"], use_container_width=True)

st.divider()

# === Upload Shapefile (.zip) → PostGIS ===
st.subheader("Importer un Shapefile (.zip) vers PostGIS")
up = st.file_uploader("Dépose un .zip contenant .shp/.shx/.dbf/.prj", type=["zip"])
default_schema = st.text_input("Schéma cible", value="public")
default_name = st.text_input("Nom de table cible (si vide → nom du .shp)", value="")

if st.button("Importer"):
    if not up:
        st.error("Veuillez déposer un fichier .zip.")
    else:
        with st.spinner("Import en cours…"):
            info = upload_shapefile_zip(engine, up, schema=default_schema or "public",
                                        table_name=default_name or None)
        st.success(
            f"Import réussi : {info['schema']}.{info['table']} "
            f"({info['geom_type']}, SRID={info['srid']}) — {info['rows']} lignes."
        )
