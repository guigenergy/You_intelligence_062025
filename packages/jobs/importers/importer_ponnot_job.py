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
    """
    Lê camada via Fiona retornando lista de tuplas (COD_ID, geometry), com barra de progresso.
    """
    with fiona.open(str(gdb_path), layer=layer) as src:
        total = len(src)
        print(f"🔍 Camada '{layer}' possui {total} feições")
        data = []
        for feat in tqdm(src, total=total, desc="Lendo feições", ncols=80):
            props = feat["properties"]
            geom_json = feat["geometry"]
            if props.get("COD_ID") is not None and geom_json:
                data.append((props["COD_ID"], geom_json))
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

    # 1) Verifica se a camada existe
    layers = listlayers(str(gdb_path))
    if camada not in layers:
        print(f"❌ Camada '{camada}' não encontrada. Disponíveis: {layers}")
        return

    # 2) Leitura com progresso
    print(f"📥 Iniciando leitura de '{camada}'...")
    t0 = datetime.now()
    raw = carregar_geometria_com_progresso(gdb_path, camada)
    print(f"📥 Leitura concluída: {len(raw)} feições em {(datetime.now()-t0).total_seconds():.2f}s")

    # 3) Conversão para GeoDataFrame
    print("🛠️ Convertendo para GeoDataFrame...")
    t1 = datetime.now()
    rows = [{'COD_ID': cid, 'geometry': geom.shape(js)} for cid, js in raw]
    gdf = gpd.GeoDataFrame(rows, geometry='geometry')
    print(f"✅ Convertido em {(datetime.now()-t1).total_seconds():.2f}s")

    # 4) Extração de coordenadas
    print("🔎 Extraindo coordenadas...")
    t2 = datetime.now()
    coords_list = []
    for row in tqdm(gdf.itertuples(index=False), total=len(gdf), desc="Extraindo coord", ncols=80):
        coords_list.append(json.dumps({'lat': row.geometry.y, 'lng': row.geometry.x}))
    gdf['coordenadas'] = coords_list
    print(f"✅ Extração concluída em {(datetime.now()-t2).total_seconds():.2f}s")

    if modo_debug:
        print(gdf[['COD_ID','coordenadas']].head())
        return

    # 5) Atualiza direto em lead em blocos
    print("🚀 Preparando atualização direta em lead (batch)...")
    cod_ids = [row.COD_ID for row in gdf.itertuples(index=False)]
    lats = [row.geometry.y for row in gdf.itertuples(index=False)]
    lngs = [row.geometry.x for row in gdf.itertuples(index=False)]

    batch_size = 20000
    total = len(cod_ids)
    for offset in range(0, total, batch_size):
        batch_cids = cod_ids[offset: offset + batch_size]
        batch_lats = lats[offset: offset + batch_size]
        batch_lngs = lngs[offset: offset + batch_size]

        stmt = text(f"""
            UPDATE {DB_SCHEMA}.lead AS l
            SET latitude = v.lat,
                longitude = v.lng
            FROM (
                SELECT
                    u.lead_id,
                    unnest(:lat_vals) AS lat,
                    unnest(:lng_vals) AS lng
                FROM {DB_SCHEMA}.unidade_consumidora u
                WHERE u.cod_id = ANY(:cod_ids)
            ) AS v
            WHERE l.id = v.lead_id
              AND (l.latitude IS NULL OR l.longitude IS NULL);
        """).bindparams(
            bindparam('cod_ids', expanding=True),
            bindparam('lat_vals', expanding=True),
            bindparam('lng_vals', expanding=True)
        )
        with engine.begin() as conn:
            conn.execute(stmt, {
                'cod_ids': batch_cids,
                'lat_vals': batch_lats,
                'lng_vals': batch_lngs
            })
        print(f"  • Lead bloco {offset+1}-{min(offset+batch_size, total)} atualizado")

    print("✅ Todas as latitudes/longitudes em lead atualizadas com sucesso!")

    # 6) Conclusão
    print(f"📤 PONNOT finalizado para {distribuidora} ({ano}) e leads georreferenciados")

if __name__ == '__main__':
    pass
