#!/usr/bin/env python3
import os
import json
import logging
import fiona
import geopandas as gpd
from fiona import listlayers
from pathlib import Path
from sqlalchemy import create_engine, text, bindparam
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
import shapely.geometry as geom

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")

# Configura conexão (echo desligado, somente erros do engine)
conn_str = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
)
engine = create_engine(conn_str, echo=False)
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


def carregar_geometria_com_progresso(gdb_path: Path, layer: str):
    with fiona.open(str(gdb_path), layer=layer) as src:
        total = len(src)
        print(f"🔍 Camada '{layer}' possui {total} feições")
        data = []
        for feat in tqdm(src, total=total, desc="Lendo feições", ncols=80):
            props = feat["properties"]
            if props.get("COD_ID") is not None and feat["geometry"]:
                data.append((props["COD_ID"], feat["geometry"]))
    return data


def main(
    gdb_path: Path,
    distribuidora: str,
    ano: int,
    prefixo: str,
    camada: str = "PONNOT",
    modo_debug: bool = False
):
    print(f"🔄 Iniciando PONNOT: {distribuidora} ({ano}), camada '{camada}'")
    print(f"🚨 DEBUG MODE: {modo_debug}")

    # 1) check layer
    layers = listlayers(str(gdb_path))
    if camada not in layers:
        print(f"❌ Camada '{camada}' não encontrada. Disponíveis: {layers}")
        return

    # 2) read features
    print(f"📥 Iniciando leitura de '{camada}'...")
    inicio_leitura = datetime.now()
    try:
        raw = carregar_geometria_com_progresso(gdb_path, camada)
    except Exception as e:
        print(f"❌ Erro na leitura: {e}")
        return
    print(f"📥 Leitura concluída: {len(raw)} feições em {(datetime.now() - inicio_leitura).total_seconds():.2f}s")

    # 3) to GeoDataFrame
    print("🛠️ Convertendo para GeoDataFrame...")
    inicio_conv = datetime.now()
    rows = [{"COD_ID": cid, "geometry": geom.shape(js)} for cid, js in raw]
    gdf = gpd.GeoDataFrame(rows, geometry="geometry")
    print(f"✅ Convertido em {(datetime.now() - inicio_conv).total_seconds():.2f}s")

    # 4) extract coords
    print("🔎 Extraindo coordenadas...")
    inicio_ext = datetime.now()
    coords = [json.dumps({"lat": row.geometry.y, "lng": row.geometry.x})
              for row in tqdm(gdf.itertuples(index=False), total=len(gdf), desc="Extraindo coord")]
    gdf["coordenadas"] = coords
    print(f"✅ Extração concluída em {(datetime.now() - inicio_ext).total_seconds():.2f}s")

    if modo_debug:
        print(gdf[["COD_ID", "coordenadas"]].head())
        return

    # 5) ensure column
    print("🔧 Garantindo coluna 'coordenadas'...")
    alter = text(f"ALTER TABLE {DB_SCHEMA}.unidade_consumidora ADD COLUMN IF NOT EXISTS coordenadas TEXT;")
    with engine.begin() as conn:
        try:
            conn.execute(alter)
            print("✅ Coluna garantida")
        except Exception as e:
            print(f"⚠️ Falha ao garantir coluna: {e}")

    # 6) batch UPDATE using expanding binds
    print("🚀 Atualizando coordenadas no banco (expanding binds + ARRAY)...")
    stmt = text(f"""
        UPDATE {DB_SCHEMA}.unidade_consumidora AS u
        SET coordenadas = v.coordenadas
        FROM (
          SELECT
            UNNEST(ARRAY[:cod_ids])   AS cod_id,
            UNNEST(ARRAY[:coords])    AS coordenadas
        ) AS v
        WHERE u.cod_id = v.cod_id
          AND (u.coordenadas IS NULL OR u.coordenadas = '{{}}')
    """).bindparams(
        bindparam("cod_ids", expanding=True),
        bindparam("coords", expanding=True)
    )

    cod_ids, coord_vals = zip(*[(row.COD_ID, row.coordenadas) for row in gdf.itertuples(index=False)])
    try:
        with engine.begin() as conn:
            conn.execute(stmt, {"cod_ids": list(cod_ids), "coords": list(coord_vals)})
        print(f"✅ UPDATE concluído em {(datetime.now() - inicio_ext).total_seconds():.2f}s")
    except Exception as e:
        print(f"❌ Erro no UPDATE em lote: {e}")
        return

    # 7) done
    print(f"📤 PONNOT finalizado para {distribuidora} ({ano})")


if __name__ == "__main__":
    pass
