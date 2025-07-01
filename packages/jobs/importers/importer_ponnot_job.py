# File: packages/jobs/importers/importer_ponnot_job.py

import os
import json
import geopandas as gpd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")

conn_str = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
)
engine = create_engine(conn_str)


def main(
    gdb_path: Path,
    distribuidora: str,
    ano: int,
    prefixo: str,                  # <— novo parâmetro
    camada: str = "PONNOT",
    modo_debug: bool = False
):
    print(f"🚨 DEBUG MODE ({camada}): {modo_debug}")
    print(f"📍 Lendo camada {camada} da distribuidora {distribuidora} ({ano})")

    try:
        gdf = gpd.read_file(gdb_path, layer=camada)
        gdf = gdf[["COD_ID", "geometry"]].dropna()

        gdf["lat"] = gdf.geometry.y
        gdf["lng"] = gdf.geometry.x
        gdf["coordenadas"] = gdf.apply(
            lambda r: json.dumps({"lat": r["lat"], "lng": r["lng"]}),
            axis=1
        )

        if modo_debug:
            print(f"[DEBUG PONNOT] Feições lidas: {len(gdf)}")
            return

        with engine.begin() as conn:
            for _, row in gdf.iterrows():
                conn.execute(f"""
                    UPDATE {DB_SCHEMA}.unidade_consumidora
                       SET coordenadas = %s
                     WHERE cod_id = %s
                       AND (coordenadas IS NULL OR coordenadas = '{{}}')
                """, (row["coordenadas"], row["COD_ID"]))

        print(f"✅ Coordenadas atualizadas com sucesso para {distribuidora} ({ano})")
    except Exception as e:
        print(f"❌ Erro ao processar PONNOT de {distribuidora} ({ano}): {e}")
