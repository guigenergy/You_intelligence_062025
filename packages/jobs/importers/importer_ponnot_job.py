#!/usr/bin/env python3
import os
import json
import fiona
import geopandas as gpd
from fiona import listlayers
from pathlib import Path
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
import shapely.geometry as geom

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")

# Configura conexão
conn_str = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
)
engine = create_engine(conn_str)


def carregar_geometria_com_progresso(gdb_path: Path, layer: str):
    """
    Lê camada via Fiona retornando lista de tuplas (COD_ID, geometry), com barra de progresso.
    """
    with fiona.open(str(gdb_path), layer=layer) as src:
        total = len(src)
        print(f"🔍 Camada '{layer}' possui {total} feições")
        data = []
        for feat in tqdm(src, total=total, desc="Lendo feições", ncols=80):
            props = feat['properties']
            geom_json = feat['geometry']
            if props.get('COD_ID') is not None and geom_json:
                data.append((props['COD_ID'], geom_json))
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
    inicio_leitura = datetime.now()
    try:
        raw = carregar_geometria_com_progresso(gdb_path, camada)
    except Exception as e:
        print(f"❌ Erro na leitura: {e}")
        return
    dur_leitura = (datetime.now() - inicio_leitura).total_seconds()
    print(f"📥 Leitura concluída: {len(raw)} feições em {dur_leitura:.2f}s")

    # 3) Conversão para GeoDataFrame
    print("🛠️ Convertendo para GeoDataFrame...")
    inicio_conv = datetime.now()
    rows = []
    for cod_id, geom_json in raw:
        shape = geom.shape(geom_json)
        rows.append({'COD_ID': cod_id, 'geometry': shape})
    gdf = gpd.GeoDataFrame(rows, geometry='geometry')
    print(f"✅ Convertido em {(datetime.now() - inicio_conv).total_seconds():.2f}s")

    # 4) Extração de coordenadas
    print("🔎 Extraindo coordenadas...")
    inicio_ext = datetime.now()
    total = len(gdf)
    coords = []
    for row in tqdm(gdf.itertuples(index=False), total=total, desc="Extraindo coord", ncols=80):
        coords.append(json.dumps({'lat': row.geometry.y, 'lng': row.geometry.x}))
    gdf['coordenadas'] = coords
    print(f"✅ Extração concluída em {(datetime.now() - inicio_ext).total_seconds():.2f}s")

    if modo_debug:
        print(gdf[['COD_ID', 'coordenadas']].head())
        return

    # 5) Garante coluna 'coordenadas'
    print("🔧 Garantindo coluna 'coordenadas'...")
    alter = text(
        f"ALTER TABLE {DB_SCHEMA}.unidade_consumidora "
        "ADD COLUMN IF NOT EXISTS coordenadas TEXT;"
    )
    with engine.begin() as conn:
        try:
            conn.execute(alter)
            print("✅ Coluna garantida")
        except Exception as e:
            print(f"⚠️ Falha ao garantir coluna: {e}")

    # 6) UPDATE em lote usando expanding binds
    print("🚀 Atualizando coordenadas no banco (expanding binds)...")
    stmt = text(f"""
        UPDATE {DB_SCHEMA}.unidade_consumidora AS u
        SET coordenadas = v.coordenadas
        FROM (
          SELECT
            UNNEST(:cod_ids::bigint[])   AS cod_id,
            UNNEST(:coords::text[])       AS coordenadas
        ) AS v
        WHERE u.cod_id = v.cod_id
          AND (u.coordenadas IS NULL OR u.coordenadas = '{{}}')
    """).bindparams(
        bindparam('cod_ids', expanding=True),
        bindparam('coords', expanding=True)
    )

    # Prepara arrays para o expanding
    cod_ids, coord_vals = zip(*[(row.COD_ID, row.coordenadas) for row in gdf.itertuples(index=False)])
    inicio_db = datetime.now()
    with engine.begin() as conn:
        conn.execute(stmt, {'cod_ids': cod_ids, 'coords': coord_vals})
    dur_db = (datetime.now() - inicio_db).total_seconds()
    print(f"✅ UPDATE concluído em {dur_db:.2f}s")

    # 7) Conclusão
    print(f"📤 PONNOT finalizado para {distribuidora} ({ano})")


if __name__ == "__main__":
    # Exemplo de chamada:
    # main(Path("data/downloads/ENEL_DISTRIBUICAO_RIO_2023.gdb"), "ENEL DISTRIBUIÇÃO RIO", 2023, "PONNOT")
    pass
