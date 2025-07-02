import geopandas as gpd
import pandas as pd
import psycopg2
import io
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

TABELA_BRUTA = "lead.lead_bruto"
TABELA_ENERGIA = "lead.lead_energia"
TABELA_DEMANDA = "lead.lead_demanda"
TABELA_STATUS = "lead.import_status"

def _to_pg_array(data):
    return pd.Series([
        "{" + ",".join(map(str, row)) + "}" if len(row) > 0 else r"\N"
        for row in data
    ])

def registrar_status(conn, distribuidora, ano, camada, status):
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABELA_STATUS} (distribuidora, ano, camada, status, data_execucao)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (distribuidora, ano) DO UPDATE
            SET status = EXCLUDED.status, data_execucao = EXCLUDED.data_execucao
        """, (distribuidora, ano, camada, status, datetime.now()))
        conn.commit()

async def main(gdb_path: str, distribuidora: str, ano: int, modo_debug: bool = False):
    print(f"🚨 DEBUG MODE (UCAT): {modo_debug}")

    camada = "UCAT_tab"
    registrar_status(psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, sslmode="require"
    ), distribuidora, ano, camada, "iniciando")

    gdf = gpd.read_file(gdb_path, layer=camada, encoding="utf-8")
    print(f"📥 Lido {len(gdf)} linhas de {camada}")

    # Normaliza os campos
    df = pd.DataFrame(gdf)

    df["cod_id"] = df["COD_ID"].astype("Int64")
    df["cod_distribuidora"] = int(distribuidora.split()[0])  # Assuma código no início
    df["origem"] = "UCAT"
    df["ano"] = ano
    df["status"] = "raw"
    df["data_conexao"] = pd.to_datetime(df["DAT_CON"], errors="coerce").dt.date
    df["cnae"] = df["CNAE"].astype(str)
    df["grupo_tensao"] = df["GRU_TEN"]
    df["modalidade"] = df["GRU_TAR"]
    df["tipo_sistema"] = df["TIP_CC"]
    df["situacao"] = df["SIT_ATIV"]
    df["classe"] = df["CLAS_SUB"]
    df["segmento"] = df["CONJ"]
    df["subestacao"] = df["SUB"]
    df["municipio_ibge"] = df["MUN"].astype(str)
    df["bairro"] = df["BRR"]
    df["cep"] = df["CEP"]
    df["pac"] = pd.to_numeric(df["PAC"], errors="coerce")
    df["pn_con"] = df["PN_CON"].astype("Int64")
    df["descricao"] = None  # pode ser preenchido depois

    df_bruto = df[[
        "cod_id", "cod_distribuidora", "origem", "ano", "status", "data_conexao",
        "cnae", "grupo_tensao", "modalidade", "tipo_sistema", "situacao", "classe",
        "segmento", "subestacao", "municipio_ibge", "bairro", "cep", "pac", "pn_con", "descricao"
    ]]

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER,
        password=DB_PASS, port=DB_PORT, sslmode="require"
    )
    cur = conn.cursor()

    if modo_debug:
        print(df_bruto.head())
        return

    buf = io.StringIO()
    df_bruto.to_csv(buf, index=False, header=False, sep=",", na_rep="\\N")
    buf.seek(0)

    cur.copy_expert(f"""
        COPY {TABELA_BRUTA} (
            cod_id, cod_distribuidora, origem, ano, status, data_conexao,
            cnae, grupo_tensao, modalidade, tipo_sistema, situacao, classe,
            segmento, subestacao, municipio_ibge, bairro, cep, pac, pn_con, descricao
        ) FROM STDIN WITH CSV NULL '\\N'
    """, buf)
    conn.commit()
    print(f"📤 Carga em {camada} completa")

    # Recupera os uc_id inseridos
    query_ids = f"""
        SELECT uc_id, cod_id FROM {TABELA_BRUTA}
        WHERE ano = %s AND cod_distribuidora = %s AND origem = %s
    """
    cur.execute(query_ids, (ano, df["cod_distribuidora"].iloc[0], "UCAT"))
    id_map = dict(cur.fetchall())

    # Prepara energia e demanda
    df["uc_id"] = df["cod_id"].map(id_map)
    df_energy = df[["uc_id", "ENE_P", "ENE_F"]].copy()
    df_energy["consumo"] = df_energy[["ENE_P", "ENE_F"]].sum(axis=1)
    df_energy["potencia"] = pd.to_numeric(df["DEM_CONT"], errors="coerce")
    df_energy = df_energy[["uc_id", "consumo", "potencia"]].dropna()

    buf_energy = io.StringIO()
    df_energy.to_csv(buf_energy, index=False, header=False, sep=",", na_rep="\\N")
    buf_energy.seek(0)

    cur.copy_expert(f"""
        COPY {TABELA_ENERGIA} (uc_id, consumo, potencia)
        FROM STDIN WITH CSV NULL '\\N'
    """, buf_energy)
    conn.commit()

    # Demanda ponta e fora de ponta
    df_demand = df[[
        "uc_id",
        *[f"DEM_P_{str(i).zfill(2)}" for i in range(1, 13)],
        *[f"DEM_F_{str(i).zfill(2)}" for i in range(1, 13)],
    ]].copy()

    df_demand["dem_ponta"] = _to_pg_array(df_demand[[f"DEM_P_{str(i).zfill(2)}" for i in range(1, 13)]].values)
    df_demand["dem_fora_ponta"] = _to_pg_array(df_demand[[f"DEM_F_{str(i).zfill(2)}" for i in range(1, 13)]].values)
    df_demand = df_demand[["uc_id", "dem_ponta", "dem_fora_ponta"]].dropna()

    buf_demand = io.StringIO()
    df_demand.to_csv(buf_demand, index=False, header=False, sep=",", na_rep="\\N")
    buf_demand.seek(0)

    cur.copy_expert(f"""
        COPY {TABELA_DEMANDA} (uc_id, dem_ponta, dem_fora_ponta)
        FROM STDIN WITH CSV NULL '\\N'
    """, buf_demand)
    conn.commit()

    registrar_status(conn, distribuidora, ano, camada, "sucesso")
    cur.close()
    conn.close()
