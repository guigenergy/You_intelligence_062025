#!/usr/bin/env python3
import os
import json
import logging
import fiona
import geopandas as gpd
from fiona import listlayers
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
import shapely.geometry as geom
from io import StringIO

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")

# Conexão (echo desligado)
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

    # 1) Check layer
    layers = listlayers(str(gdb_path))
    if camada not in layers:
        print(f"❌ Camada '{camada}' não encontrada. Disponíveis: {layers}")
        return

    # 2) Read features
    print(f"📥 Iniciando leitura de '{camada}'...")
    t0 = datetime.now()
    raw = carregar_geometria_com_progresso(gdb_path, camada)
    print(f"📥 Leitura concluída: {len(raw)} feições em {(datetime.now()-t0).total_seconds():.2f}s")

    # 3) To GeoDataFrame
    print("🛠️ Convertendo para GeoDataFrame...")
    t1 = datetime.now()
    rows = [{"COD_ID": cid, "geometry": geom.shape(js)} for cid, js in raw]
    gdf = gpd.GeoDataFrame(rows, geometry="geometry")
    print(f"✅ Convertido em {(datetime.now()-t1).total_seconds():.2f}s")

    # 4) Extract coords
    print("🔎 Extraindo coordenadas...")
    t2 = datetime.now()
    coords_list = []
    for row in tqdm(gdf.itertuples(index=False), total=len(gdf), desc="Extraindo coord", ncols=80):
        coords_list.append(json.dumps({"lat": row.geometry.y, "lng": row.geometry.x}))
    gdf["coordenadas"] = coords_list
    print(f"✅ Extração concluída em {(datetime.now()-t2).total_seconds():.2f}s")

    if modo_debug:
        print(gdf[["COD_ID", "coordenadas"]].head())
        return

    # 5) Ensure column
    print("🔧 Garantindo coluna 'coordenadas'...")
    alter = text(f"ALTER TABLE {DB_SCHEMA}.unidade_consumidora ADD COLUMN IF NOT EXISTS coordenadas TEXT;")
    with engine.begin() as conn:
        conn.execute(alter)
        print("✅ Coluna garantida")

    # 6) Bulk UPDATE via temp table + COPY
    print("🚀 Atualizando coordenadas no banco (temp table + COPY)…")
    # abre raw_connection para usar cursor copy_from
    with engine.raw_connection() as raw_conn:
        cur = raw_conn.cursor()
        # cria temp table
        cur.execute(f"""
            CREATE TEMP TABLE temp_coords (
                cod_id      BIGINT,
                coordenadas TEXT
            ) ON COMMIT DROP;
        """)
        # prepara o buffer CSV (tab-delim)
        buf = StringIO()
        for cid, coord in zip(gdf["COD_ID"], gdf["coordenadas"]):
            # tab-separated, sem cabeçalho
            buf.write(f"{cid}\t{coord}\n")
        buf.seek(0)
        # injeta tudo de uma vez
        cur.copy_from(buf, "temp_coords", columns=("cod_id","coordenadas"), sep="\t")
        # faz o UPDATE via join
        cur.execute(f"""
            UPDATE {DB_SCHEMA}.unidade_consumidora AS u
            SET coordenadas = t.coordenadas
            FROM temp_coords AS t
            WHERE u.cod_id = t.cod_id
              AND (u.coordenadas IS NULL OR u.coordenadas = '{{}}');
        """)
        raw_conn.commit()
    print(f"✅ UPDATE em lote concluído em {(datetime.now()-t2).total_seconds():.2f}s")

    # 7) Done
    print(f"📤 PONNOT finalizado para {distribuidora} ({ano})")


if __name__ == "__main__":
    # Exemplo:
    # main(Path("data/downloads/ENEL_DISTRIBUICAO_RIO_2023.gdb"),
    #      "ENEL DISTRIBUIÇÃO RIO", 2023, "PONNOT")
    pass
