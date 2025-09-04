# -*- coding: utf-8 -*-
import os, io, zipfile, tempfile, re
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from shapely.geometry import GeometryCollection
from geoalchemy2 import Geometry

# Helper pour Streamlit Cloud (secrets) + .env local
try:
    import streamlit as st
    # si des secrets existent, on les exporte vers l'env (une fois)
    if hasattr(st, "secrets"):
        for k, v in st.secrets.items():
            if isinstance(v, (str, int, float, bool)):
                os.environ.setdefault(k, str(v))
        # ou si vous rangez dans une section [env] :
        if "env" in st.secrets:
            for k, v in st.secrets["env"].items():
                os.environ.setdefault(k, str(v))
except Exception:
    pass

# ✅ charge .env au moment de l'import (une seule fois)
try:
    import dotenv
    dotenv.load_dotenv()  # .env à la racine de ton projet
except Exception:
    pass

def _env(k, d=None): 
    v = os.getenv(k, d)
    return v.strip() if isinstance(v, str) else v

def get_engine():
    host = os.getenv("SUPABASE_HOST", "").strip()
    port = int(os.getenv("SUPABASE_PORT", "5432"))
    db   = os.getenv("SUPABASE_DB", "postgres").strip()
    user = os.getenv("SUPABASE_USER", "").strip()
    pwd  = os.getenv("SUPABASE_PASSWORD", "").strip()

    # Garde-fous typiques
    if not host:
        raise RuntimeError("SUPABASE_HOST manquant (vérifie le chargement de .env).")
    if "://" in host:
        raise RuntimeError("SUPABASE_HOST ne doit PAS contenir 'postgres://...' — mets juste le hostname.")
    if ":" in host:
        raise RuntimeError("Ne mets pas le port dans SUPABASE_HOST. Utilise SUPABASE_PORT séparément.")

    url = URL.create(
        "postgresql+psycopg",
        username=user,
        password=pwd,
        host=host,
        port=port,
        database=db,
        query={"sslmode": "require", "connect_timeout": "10"},
    )

    # Si tu passes en transaction mode (6543), tu peux ajouter :
    # connect_args={"prepare_threshold": 0}
    # return create_engine(url, pool_pre_ping=True, connect_args={"prepare_threshold": 0})

    return create_engine(url, pool_pre_ping=True)


def list_postgis_tables(engine):
    sql = """
    SELECT n.nspname AS schema,
           c.relname  AS table,
           a.attname  AS geom_column,
           postgis_typmod_type(a.atttypmod)  AS geom_type,
           postgis_typmod_srid(a.atttypmod)  AS srid
    FROM pg_attribute a
    JOIN pg_class     c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_type      t ON a.atttypid = t.oid
    WHERE t.typname = 'geometry'
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND c.relkind IN ('r','p','m','f','v')
      AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast','extensions')
    ORDER BY n.nspname, c.relname;
    """
    return pd.read_sql(sql, engine).to_dict(orient="records")

def table_overview(engine, schema, table, geom_col):
    # Colonnes
    cols = pd.read_sql(
        text("""SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema=:s AND table_name=:t
                ORDER BY ordinal_position"""),
        engine, params={"s": schema, "t": table}
    )
    # Nombre de lignes (robuste si l'objet n'est pas directement requêtable)
    try:
        n = pd.read_sql(
            text(f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"'), engine
        )["n"].iloc[0]
    except Exception:
        n = 0

    # Aperçu (WKT tronqué)
    q = f'''
    SELECT *, LEFT(ST_AsText("{geom_col}"), 120) AS wkt
    FROM "{schema}"."{table}" LIMIT 200
    '''
    preview = pd.read_sql(q, engine)
    return {"columns": cols.values.tolist(), "row_count": int(n), "preview": preview}

def _geom_family(gdf):
    fams = set(gdf.geom_type.dropna().str.upper())
    if fams & {"POLYGON","MULTIPOLYGON"}: return "MULTIPOLYGON"
    if fams & {"LINESTRING","MULTILINESTRING"}: return "MULTILINESTRING"
    if fams & {"POINT","MULTIPOINT"}: return "MULTIPOINT"
    return "GEOMETRY"

# --- helper robuste d'ouverture avec encodage ---
def _read_gdf_try_encodings(path, enc_hint=None):
    import geopandas as gpd
    trials = []
    if enc_hint:
        trials.append(enc_hint)
    trials += ["utf-8", "latin-1", "cp1252"]
    last_err = None
    for enc in trials:
        try:
            return gpd.read_file(path, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    # dernier recours : laisser le driver décider
    try:
        return gpd.read_file(path)
    except Exception as e2:
        raise RuntimeError(f"Impossible de lire le SHP (encodings testés: {trials}). Dernière erreur: {last_err}") from e2

def upload_shapefile_zip(engine, uploaded_file, schema="public", table_name=None, default_epsg=None):
    import logging
    import os, zipfile, tempfile, re
    from pathlib import Path
    import geopandas as gpd

    with tempfile.TemporaryDirectory() as tmp:
        zpath = os.path.join(tmp, "in.zip")
        with open(zpath, "wb") as f:
            f.write(uploaded_file.read())

        with zipfile.ZipFile(zpath) as z:
            z.extractall(tmp)
            # ✅ log le contenu extrait
            logging.warning("📦 Contenu du zip:")
            for name in z.namelist():
                logging.warning(f" - {name}")

        # ✅ cherche récursivement un .shp (insensible à la casse)
        shp_files = list(Path(tmp).rglob("*.shp")) + list(Path(tmp).rglob("*.SHP"))
        if not shp_files:
            raise RuntimeError("Shapefile (.shp) introuvable dans le .zip")
        shp = max(shp_files, key=lambda p: p.stat().st_size)

        if not table_name:
            base = shp.stem
            table_name = re.sub(r"[^A-Za-z0-9_]+", "_", base.lower())

        # ✅ encodage via .cpg si présent
        cpg = shp.with_suffix(".cpg")
        enc_hint = None
        if cpg.exists():
            raw = cpg.read_text(errors="ignore").strip().upper()
            map_enc = {
                "UTF-8": "utf-8", "UTF8": "utf-8",
                "LATIN1": "latin-1", "ISO-8859-1": "latin-1",
                "CP1252": "cp1252", "WINDOWS-1252": "cp1252",
            }
            enc_hint = map_enc.get(raw, raw.lower())

        # 🔎 lecture tolérante (UTF-8 → Latin-1 → CP1252)
        gdf = _read_gdf_try_encodings(shp, enc_hint=enc_hint)

        # CRS -> SRID
        srid = gdf.crs.to_epsg() if gdf.crs else None
        if srid is None and default_epsg:
            gdf.set_crs(epsg=int(default_epsg), inplace=True)
            srid = int(default_epsg)

        # géométrie s'appelle 'geom'
        gcol = gdf.geometry.name
        if gcol != "geom":
            gdf = gdf.rename(columns={gcol: "geom"}).set_geometry("geom")

        # --- écriture: typé + SRID quand on peut (meilleure perf/metadata)
        fam = _geom_family(gdf)
        dtype = {"geom": Geometry(geometry_type=fam if srid else "GEOMETRY", srid=srid)}
        gdf.to_postgis(table_name, engine, schema=schema, if_exists="replace", index=False, dtype=dtype)

        # --- post-traitements: PK + index spatial + stats
        with engine.begin() as conn:
            # PK
            conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN IF NOT EXISTS id bigserial'))
            # tenter d'ajouter la PK (ignore si déjà là)
            try:
                conn.execute(text(f'ALTER TABLE "{schema}"."{table_name}" ADD CONSTRAINT "{table_name}_pkey" PRIMARY KEY (id)'))
            except Exception:
                pass

            # géométrie strictement MULTI + 2D (evite types mixtes => mieux pour QGIS)
            if srid and fam in ("MULTIPOLYGON","MULTILINESTRING","MULTIPOINT"):
                conn.execute(text(
                    f'ALTER TABLE "{schema}"."{table_name}" '
                    f'ALTER COLUMN "geom" TYPE geometry({fam},{srid}) '
                    f'USING ST_Multi(ST_Force2D("geom"))'
                ))

            # index spatial + stats
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{table_name}_geom_gix" ON "{schema}"."{table_name}" USING GIST ("geom")'))
            conn.execute(text(f'ANALYZE "{schema}"."{table_name}"'))

        return {"schema": schema, "table": table_name, "rows": len(gdf), "geom_type": fam, "srid": srid}
