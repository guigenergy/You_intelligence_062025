import geopandas as gpd
import pandas as pd
import io
from datetime import datetime
from pathlib import Path

from packages.database.connection import get_db_cursor
from packages.jobs.utils.rastreio import registrar_status

# Nome das tabelas
TABELA_BRUTA = "lead_bruto"
TABELA_ENERGIA = "lead_energia"
TABELA_DEMANDA = "lead_demanda"
TABELA_STATUS = "import_status"

# Colunas esperadas
COLS_UCAT = [
    "COD_ID", "DIST", "PAC", "CTAT", "SUB", "CONJ", "MUN", "CEG_GD", "BRR", "CEP",
    "CLAS_SUB", "CNAE", "TIP_CC", "FAS_CON", "GRU_TEN", "TEN_FORN", "GRU_TAR",
    "SIT_ATIV", "DAT_CON", "CAR_INST", "LIV", "ARE_LOC", "DEM_P_01", "DEM_P_02", "DEM_P_03",
    "DEM_P_04", "DEM_P_05", "DEM_P_06", "DEM_P_07", "DEM_P_08", "DEM_P_09", "DEM_P_10",
    "DEM_P_11", "DEM_P_12", "DEM_F_01", "DEM_F_02", "DEM_F_03", "DEM_F_04", "DEM_F_05",
    "DEM_F_06", "DEM_F_07", "DEM_F_08", "DEM_F_09", "DEM_F_10", "DEM_F_11", "DEM_F_12"
]

def _to_pg_array(data):
    """Converte DataFrame com arrays para string no formato Postgres (mantém decimais)"""
    return pd.Series([
        "{" + ",".join(map(str, row)) + "}" if len(row) > 0 else r"\N"
        for row in data
    ])

async def main(gdb_path: str, distribuidora: str, ano: int, camada: str = "UCAT_tab", modo_debug: bool = False):
    registrar_status(distribuidora, ano, camada, "iniciando")
    print(f"🚨 DEBUG MODE ({camada}): {modo_debug}")

    try:
        gdf = gpd.read_file(gdb_path, layer=camada)[COLS_UCAT]
    except Exception as e:
        registrar_status(distribuidora, ano, camada, "erro leitura GDB")
        raise RuntimeError(f"Erro ao ler camada {camada} do arquivo GDB: {e}")

    print(f"📥 Lido {len(gdf)} linhas de {camada} em {gdb_path}")

    # Renomear e normalizar
    df = gdf.rename(columns=str.lower).copy()
    df.columns = df.columns.str.lower()

    df['cod_id'] = df['cod_id'].astype("Int64")
    df['dat_con'] = pd.to_datetime(df['dat_con'], errors="coerce")
    df['car_inst'] = pd.to_numeric(df['car_inst'], errors="coerce")
    df['dist'] = df['dist'].astype("Int64")
    df['pac'] = pd.to_numeric(df['pac'], errors="coerce")
    df['cnae'] = df['cnae'].astype(str)

    # Identidade do batch
    df["origem"] = "UCAT"
    df["ano"] = ano
    df["status"] = "raw"
    df["data_conexao"] = df["dat_con"]
    df["cod_distribuidora"] = df["dist"]

    # Remove duplicatas com base no banco
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT cod_id, ano, cod_distribuidora FROM {TABELA_BRUTA}
            WHERE origem = 'UCAT' AND ano = %s AND cod_distribuidora = %s
        """, (ano, df["cod_distribuidora"].iloc[0]))
        duplicados = set((row["cod_id"], row["ano"], row["cod_distribuidora"]) for row in cur.fetchall())

    df_bruto = df[
        ~df.apply(lambda row: (row["cod_id"], row["ano"], row["cod_distribuidora"]) in duplicados, axis=1)
    ].copy()

    print(f"🛠️ Transformado em {len(df_bruto)} linhas para inserção")

    if modo_debug or len(df_bruto) == 0:
        registrar_status(distribuidora, ano, camada, "sem dados para importar")
        print(f"⚠️ Modo debug ativo ou sem dados novos — finalizado.")
        return

    # Insert lead_bruto
    buf = io.StringIO()
    df_bruto_pg = pd.DataFrame({
        "cod_id": df_bruto["cod_id"],
        "cod_distribuidora": df_bruto["cod_distribuidora"],
        "origem": df_bruto["origem"],
        "ano": df_bruto["ano"],
        "status": df_bruto["status"],
        "data_conexao": df_bruto["data_conexao"],
        "cnae": df_bruto["cnae"],
        "grupo_tensao": df_bruto["gru_ten"],
        "modalidade": df_bruto["gru_tar"],
        "tipo_sistema": df_bruto["tip_cc"],
        "situacao": df_bruto["sit_ativ"],
        "classe": df_bruto["clas_sub"],
        "segmento": df_bruto["conj"],
        "subestacao": df_bruto["sub"],
        "municipio_ibge": df_bruto["mun"],
        "bairro": df_bruto["brr"],
        "cep": df_bruto["cep"],
        "pac": df_bruto["pac"],
        "pn_con": df_bruto["ceg_gd"],
        "descricao": df_bruto["are_loc"]
    })
    df_bruto_pg.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    with get_db_cursor(commit=True) as cur:
        cur.copy_expert(f"""
            COPY {TABELA_BRUTA} (
                cod_id, cod_distribuidora, origem, ano, status, data_conexao,
                cnae, grupo_tensao, modalidade, tipo_sistema, situacao,
                classe, segmento, subestacao, municipio_ibge, bairro, cep,
                pac, pn_con, descricao
            ) FROM STDIN WITH CSV
        """, buf)

    # Recuperar uc_id para associar demandas/energia
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT uc_id, cod_id FROM {TABELA_BRUTA}
            WHERE origem = 'UCAT' AND ano = %s AND cod_distribuidora = %s
        """, (ano, df_bruto["cod_distribuidora"].iloc[0]))
        mapa_ucid = {row["cod_id"]: row["uc_id"] for row in cur.fetchall()}

    # Inserir tabela de energia
    df_energia = pd.DataFrame({
        "uc_id": df_bruto["cod_id"].map(mapa_ucid),
        "consumo": _to_pg_array(df_bruto[[f"dem_p_{str(i).zfill(2)}" for i in range(1,13)]].values),
        "potencia": df_bruto["car_inst"].fillna(0).astype(int)
    })

    buf_energia = io.StringIO()
    df_energia.to_csv(buf_energia, index=False, header=False, na_rep="\\N")
    buf_energia.seek(0)

    with get_db_cursor(commit=True) as cur:
        cur.copy_expert(f"""
            COPY {TABELA_ENERGIA} (uc_id, consumo, potencia)
            FROM STDIN WITH CSV
        """, buf_energia)

    # Inserir tabela de demanda
    df_demanda = pd.DataFrame({
        "uc_id": df_bruto["cod_id"].map(mapa_ucid),
        "dem_ponta": _to_pg_array(df_bruto[[f"dem_p_{str(i).zfill(2)}" for i in range(1,13)]].values),
        "dem_fora_ponta": _to_pg_array(df_bruto[[f"dem_f_{str(i).zfill(2)}" for i in range(1,13)]].values),
    })

    buf_demanda = io.StringIO()
    df_demanda.to_csv(buf_demanda, index=False, header=False, na_rep="\\N")
    buf_demanda.seek(0)

    with get_db_cursor(commit=True) as cur:
        cur.copy_expert(f"""
            COPY {TABELA_DEMANDA} (uc_id, dem_ponta, dem_fora_ponta)
            FROM STDIN WITH CSV
        """, buf_demanda)

    registrar_status(distribuidora, ano, camada, "importado")
    print(f"📤 Carga em UCAT concluída com sucesso!")

