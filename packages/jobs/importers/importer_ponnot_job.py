import geopandas as gpd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path
import json

load_dotenv()

# Conexão com o banco
conn_str = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
)

engine = create_engine(conn_str)

def main(gdb_path: Path, distribuidora: str, ano: int, camada: str = "PONNOT"):
    print(f"📍 Lendo camada {camada} da distribuidora {distribuidora} ({ano})")
    try:
        gdf = gpd.read_file(gdb_path, layer=camada)
        gdf = gdf[["COD_ID", "geometry"]].dropna()

        # Extrai coordenadas
        gdf["lat"] = gdf.geometry.y
        gdf["lng"] = gdf.geometry.x
        gdf["coordenadas"] = gdf.apply(lambda row: json.dumps({"lat": row["lat"], "lng": row["lng"]}), axis=1)

        with engine.begin() as conn:
            for _, row in gdf.iterrows():
                conn.execute("""
                    UPDATE plead.unidade_consumidora
                    SET coordenadas = %s
                    WHERE cod_id = %s
                      AND (coordenadas IS NULL OR coordenadas = '{}')
                """, (row["coordenadas"], row["COD_ID"]))

        print(f"✅ Coordenadas atualizadas com sucesso para {distribuidora} ({ano})")
    except Exception as e:
        print(f"❌ Erro ao processar PONNOT de {distribuidora} ({ano}): {e}")
