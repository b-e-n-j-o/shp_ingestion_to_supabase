#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingestion SHP -> Supabase (PostGIS), 1 table par couche d'un dossier.
- Conserve CRS d'origine + Z si présent (aucune "dégradation" des géométries).
- Crée index spatial et ANALYZE pour de bonnes perfs dans QGIS.
- (Optionnel) vues GeoJSON/4326 + RLS/GRANT comme avant.
"""

import os
import re
import sys
import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Iterable

import geopandas as gpd
gpd.options.io_engine = "pyogrio"  # lecture robuste (encodages/CPG)

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from geoalchemy2 import Geometry
from dotenv import load_dotenv

# ------------- CONFIG -------------
# Dossier par défaut (modifiable ou passé en argv[1])
RACINE_DONNEES = "/Users/benjaminbenoit/Downloads/cote_de_seuil_ppri"

PG_SCHEMA = "public"

# Comportement si la table existe déjà: "replace" (écrase) ou "append"
IF_EXISTS = "replace"

# Chunk d'insertion (utile pour grosses couches)
CHUNKSIZE = 5000

# Vues & policies (mêmes options qu’avant)
CREATE_GEOJSON_VIEW = False
CREATE_REPROJECTED_VIEW_4326 = False
APPLY_RLS_AND_GRANT = True

PG_NAME_MAXLEN = 63
# ----------------------------------

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def mk_engine_from_env() -> Engine:
    load_dotenv()
    host = os.getenv("SUPABASE_HOST")
    port = int(os.getenv("SUPABASE_PORT", "6543"))  # pooler recommandé
    db   = os.getenv("SUPABASE_DB", "postgres")
    user = os.getenv("SUPABASE_USER", "postgres")
    pwd  = os.getenv("SUPABASE_PASSWORD")
    if not (host and pwd):
        raise RuntimeError("Variables .env manquantes (SUPABASE_HOST / SUPABASE_PASSWORD).")

    # URL.create gère les mots de passe spéciaux + SSL
    url = URL.create(
        "postgresql+psycopg",
        username=user,
        password=pwd,
        host=host,
        port=port,
        database=db,
        query={"sslmode": "require"},
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_postgis_and_helpers(engine: Engine):
    """Active PostGIS + crée la fonction utilitaire create_geojson_view (si absente)."""
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        # on crée / met à jour directement la fonction, sans bloc DO imbriqué
        conn.execute(text("""
        CREATE OR REPLACE FUNCTION public.create_geojson_view(
          p_schema text,
          p_table  text,
          p_view   text DEFAULT NULL
        ) RETURNS void LANGUAGE plpgsql AS
        $$
        DECLARE
          v_view text := coalesce(p_view, 'v_' || p_table);
          cols   text;
          sql    text;
        BEGIN
          SELECT string_agg(quote_ident(column_name), ', ')
          INTO cols
          FROM information_schema.columns
          WHERE table_schema = p_schema
            AND table_name   = p_table
            AND column_name <> 'geom';
          IF cols IS NULL THEN
            RAISE EXCEPTION 'Table %.% introuvable ou sans colonnes', p_schema, p_table;
          END IF;
          sql := format(
            'CREATE OR REPLACE VIEW %I.%I AS
             SELECT %s,
                    st_asgeojson(t.geom)::json AS geometry
             FROM %I.%I t;',
            p_schema, v_view, cols, p_schema, p_table
          );
          EXECUTE sql;
        END;
        $$;
        """))

def list_shp_files(root: str) -> Iterable[str]:
    for p in Path(root).rglob("*.shp"):
        if not p.name.startswith("._"):  # artefacts macOS
            yield str(p)


def slugify(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name[:PG_NAME_MAXLEN]


def table_name_from_path(root: str, shp_path: str) -> str:
    rel = os.path.relpath(shp_path, root)
    stem = Path(rel).with_suffix("").as_posix()
    return slugify(stem)


def sanitize_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    cols, seen = [], set()
    for c in gdf.columns:
        if c == "geometry":
            cols.append(c); continue
        cc = slugify(c) or "col"
        base, i = cc, 2
        while cc in seen or cc == "geometry":
            cc = f"{base}_{i}"; i += 1
        seen.add(cc); cols.append(cc)
    return gdf.set_axis(cols, axis=1)


def detect_srid(gdf: gpd.GeoDataFrame) -> Optional[int]:
    if gdf.crs is None: return None
    try:
        epsg = gdf.crs.to_epsg()
        return int(epsg) if epsg else None
    except Exception:
        return None


def has_z_any(gdf: gpd.GeoDataFrame) -> bool:
    for geom in gdf.geometry:
        if geom is None: continue
        try:
            if getattr(geom, "has_z", False): return True
        except Exception:
            pass
    return False


def geom_type_for_column(gdf: gpd.GeoDataFrame, z: bool) -> Tuple[str, bool]:
    types = set([t.upper() for t in gdf.geom_type.dropna().unique()])
    norm = []
    for t in types:
        if t.endswith("ZM") or t.endswith("Z"):
            t = t.replace("ZM","").replace("Z","")
        norm.append(t)
    types = set(norm)
    if len(types) == 0:
        return ("GEOMETRYZ" if z else "GEOMETRY", True)
    if len(types) == 1:
        t = list(types)[0]
        return (f"{t}Z" if z else t, False)
    return ("GEOMETRYZ" if z else "GEOMETRY", True)


def read_shp_robust(path: str) -> gpd.GeoDataFrame:
    """
    Lecture robuste d'un SHP avec encodage tolérant.
    Essaie .cpg s'il existe, sinon utf-8 → latin-1 → cp1252.
    """
    cpg = Path(path).with_suffix(".cpg")
    enc_hint = None
    if cpg.exists():
        raw = cpg.read_text(errors="ignore").strip().upper()
        enc_map = {
            "UTF-8": "utf-8",
            "UTF8": "utf-8",
            "LATIN1": "latin-1",
            "ISO-8859-1": "latin-1",
            "CP1252": "cp1252",
            "WINDOWS-1252": "cp1252",
        }
        enc_hint = enc_map.get(raw, raw.lower())

    trials = [enc_hint, "utf-8", "latin-1", "cp1252"]
    for enc in filter(None, trials):
        try:
            return gpd.read_file(path, encoding=enc)
        except Exception:
            continue

    # dernier recours : laisse gpd décider
    return gpd.read_file(path)


def to_postgis(engine: Engine, gdf: gpd.GeoDataFrame, table: str, schema: str, srid: Optional[int], z: bool):
    gtype, mixed = geom_type_for_column(gdf, z)
    geom_dtype = Geometry(geometry_type=gtype, srid=srid, dimension=3 if z else 2)
    logging.info(f"-> table={schema}.{table}  geom={gtype}  srid={srid}  mixed={mixed}")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    gdf.to_postgis(
        name=table,
        con=engine,
        schema=schema,
        if_exists=IF_EXISTS,
        index=False,
        dtype={"geom": geom_dtype} if "geom" in gdf.columns else {"geometry": geom_dtype},
        chunksize=CHUNKSIZE
    )

    with engine.begin() as conn:
        # uniformiser le nom de la colonne
        cols = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema=:s AND table_name=:t
        """), {"s": schema, "t": table}).fetchall()
        colnames = {c[0] for c in cols}
        if "geometry" in colnames and "geom" not in colnames:
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" RENAME COLUMN "geometry" TO "geom"'))
        # index spatial + stats
        conn.execute(text(f'CREATE INDEX IF NOT EXISTS "{table}_geom_gix" ON "{schema}"."{table}" USING GIST("geom")'))
        # clef primaire (utile QGIS)
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS id bigserial'))
        try:
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD CONSTRAINT "{table}_pkey" PRIMARY KEY (id)'))
        except Exception:
            pass
        conn.execute(text(f'ANALYZE "{schema}"."{table}"'))


def create_views_and_policies(engine: Engine, schema: str, table: str):
    view_geojson = f"v_{table}"
    with engine.begin() as conn:
        # Vue GeoJSON
        if CREATE_GEOJSON_VIEW:
            conn.execute(text("SELECT public.create_geojson_view(:s,:t,:v)"),
                         {"s": schema, "t": table, "v": view_geojson})
        # Vue 4326
        if CREATE_REPROJECTED_VIEW_4326:
            view_reproj = f"v_{table}_4326"
            conn.execute(text(f"""
                CREATE OR REPLACE VIEW "{schema}"."{view_reproj}" AS
                SELECT *, ST_Transform(geom, 4326) AS geom_4326
                FROM "{schema}"."{table}";
            """))

        if APPLY_RLS_AND_GRANT:
            # activer RLS
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ENABLE ROW LEVEL SECURITY'))

            # créer la policy si absente (⚠️ sans DO/EXECUTE, sans paramètres)
            policy = f"read_{table}_anon"
            exists = conn.execute(text("""
                SELECT 1 FROM pg_policies
                WHERE schemaname = :s AND tablename = :t AND policyname = :p
            """), {"s": schema, "t": table, "p": policy}).first()

            if not exists:
                conn.execute(text(
                    f'CREATE POLICY "{policy}" ON "{schema}"."{table}" '
                    f'FOR SELECT TO anon USING (true)'
                ))

            # GRANT SELECT sur la vue GeoJSON (si créée)
            if CREATE_GEOJSON_VIEW:
                conn.execute(text(f'GRANT SELECT ON "{schema}"."{view_geojson}" TO anon'))


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else RACINE_DONNEES
    root = os.path.abspath(root)
    logging.info(f"Racine: {root}")

    engine = mk_engine_from_env()
    ensure_postgis_and_helpers(engine)

    errors = []
    for shp in list_shp_files(root):
        try:
            logging.info(f"=== {shp}")
            gdf = read_shp_robust(shp)
            if gdf.empty:
                logging.warning("  -> vide, on saute.")
                continue

            srid = detect_srid(gdf)
            if srid is None:
                logging.warning("  -> CRS inconnu (.prj manquant ?), stockage sans SRID explicite.")

            gdf = sanitize_columns(gdf)
            if "geometry" in gdf.columns:
                gdf = gdf.rename(columns={"geometry": "geom"})

            # ⚡️ rétablir la géométrie active sur 'geom'
            if "geom" in gdf.columns:
                gdf = gdf.set_geometry("geom")

            z = has_z_any(gdf)
            table = table_name_from_path(root, shp)

            to_postgis(engine, gdf, table, PG_SCHEMA, srid, z)
            create_views_and_policies(engine, PG_SCHEMA, table)

        except Exception as e:
            logging.exception(f"ERREUR {shp}: {e}")
            errors.append({"file": shp, "error": str(e)})

    if errors:
        out = Path("ingest_shp_errors.json")
        out.write_text(json.dumps(errors, ensure_ascii=False, indent=2))
        logging.warning(f"{len(errors)} erreur(s) – détails: {out}")
    logging.info("Terminé.")


if __name__ == "__main__":
    main()
