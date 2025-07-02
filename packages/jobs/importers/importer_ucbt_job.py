#!/usr/bin/env python3
import os
import io
import uuid
import pandas as pd
import fiona
from fiona import listlayers
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
from packages.database.connection import get_db_cursor

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")

def _to_pg_array(data):
    return pd.Series([
        "{" + ",".join(map(str, row)) + "}" if len(row) else "{}"
        for row in data
    ])

def normalizar_cep(cep):
    return ''.join(filter(str.isdigit, str(cep)))[:8] if cep else ""

def carregar_com_progresso(gdb_path: Path, layer: str) -> pd.DataFrame:
    """
    Carrega elementos da camada sem geometria com barra de progresso, usando Fiona.
    """
    with fiona.open(str(gdb_path), layer=layer) as src:
        total = len(src)
        print(f"🔍 Camada '{layer}' possui {total} feições")
        props = []
        for feat in tqdm(src, total=total, desc="Lendo feições", ncols=80):
            props.append(feat["properties"])
    return pd.DataFrame(props)


def main(
    gdb_path: Path,
    distribuidora: str,
    ano: int,
    prefixo: str,
    camada: str = "UCBT_tab",
    modo_debug: bool = False
):
    print(f"🔄 Iniciando importação: {camada} | {distribuidora} {ano}")
    print(f"🚨 DEBUG MODE ({camada}): {modo_debug}")

    # 1) Leitura com progresso visual
    if camada not in listlayers(str(gdb_path)):
        print(f"❌ Camada '{camada}' não encontrada em {gdb_path}" )
        return
    start = datetime.now()
    try:
        df = carregar_com_progresso(gdb_path, camada)
    except Exception as e:
        print(f"❌ Erro ao ler camada {camada}: {e}")
        return
    elapsed = (datetime.now() - start).total_seconds()
    print(f"📥 Concluída leitura: {len(df)} linhas em {elapsed:.2f}s")

    # 2) Transformações
    t1 = datetime.now()
    df["CEP"] = df.get("CEP", "").astype(str).apply(normalizar_cep)
    df["id_interno"] = prefixo + "_" + df.get("COD_ID", "").astype(str)
    df["uc_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    print(f"🛠️ Transformações concluídas em {(datetime.now() - t1).total_seconds():.2f}s")

    if modo_debug:
        print(df.head())
        return

    # 3) Preparação de lead
    df_lead = (
        df[["id_interno","CEP","BRR","MUN"]]
        .drop_duplicates("id_interno")
        .rename(columns={"BRR":"bairro","MUN":"municipio_ibge","CEP":"cep"})
        .copy()
    )
    df_lead["id"] = df_lead["id_interno"]
    df_lead["distribuidora"] = distribuidora
    df_lead["status"] = "raw"
    df_lead["ultima_atualizacao"] = datetime.utcnow()
    cols_lead = [
        "id","id_interno","bairro","cep",
        "municipio_ibge","distribuidora","status","ultima_atualizacao"
    ]

    # 4) Preparação de unidade consumidora
    df_uc = pd.DataFrame({
        "id": df["uc_id"],
        "cod_id": df.get("COD_ID"),
        "lead_id": df["id_interno"],
        "origem": camada,
        "ano": ano,
        "data_conexao": pd.to_datetime(df.get("DAT_CON"), errors="coerce"),
        "tipo_sistema": df.get("TIP_SIST"),
        "grupo_tensao": df.get("GRU_TEN"),
        "modalidade": df.get("GRU_TAR"),
        "situacao": df.get("SIT_ATIV"),
        "classe": df.get("CLAS_SUB"),
        "segmento": df.get("CONJ"),
        "subestacao": df.get("SUB"),
        "cnae": df.get("CNAE"),
        "descricao": df.get("DESCR"),
        "potencia": df.get("PN_CON").fillna(0).astype(float)
    })

    # 5) Séries temporais
    ene_cols = [c for c in df.columns if c.startswith("ENE_")]
    dem_p_cols = [c for c in df.columns if c.startswith("DEM_P_")]
    dem_f_cols = [c for c in df.columns if c.startswith("DEM_F_")]
    dic_cols = [c for c in df.columns if c.startswith("DIC_")]
    fic_cols = [c for c in df.columns if c.startswith("FIC_")]

    df_energia = pd.DataFrame({
        "id": df["uc_id"],
        "uc_id": df["uc_id"],
        "ene": _to_pg_array(df[ene_cols].fillna(0).astype(int).values)
    })
    df_demanda = pd.DataFrame({
        "id": df["uc_id"],
        "uc_id": df["uc_id"],
        "dem_ponta": _to_pg_array(df[dem_p_cols].fillna(0).astype(int).values),
        "dem_fora_ponta": _to_pg_array(df[dem_f_cols].fillna(0).astype(int).values)
    })
    df_qualidade = pd.DataFrame({
        "id": df["uc_id"],
        "uc_id": df["uc_id"],
        "dic": _to_pg_array(df[dic_cols].fillna(0).astype(int).values),
        "fic": _to_pg_array(df[fic_cols].fillna(0).astype(int).values)
    })

    # 6) Carga otimizada
    t2 = time.time()
    with get_db_cursor(commit=True) as cur:
        cur.execute(f"SELECT id FROM {DB_SCHEMA}.lead WHERE id = ANY(%s)", (df_lead["id"].tolist(),))
        existentes = {row[0] for row in cur.fetchall()}
        df_novos = df_lead[~df_lead["id"].isin(existentes)]
        if df_novos.empty:
            print("⏩ Nenhum lead novo para importar")
        else:
            buf_lead = io.StringIO(); df_novos[cols_lead].to_csv(buf_lead, index=False, header=False, na_rep='\\N'); buf_lead.seek(0)
            cur.copy_expert(f"COPY {DB_SCHEMA}.lead ({','.join(cols_lead)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf_lead)

        buf_uc = io.StringIO(); df_uc.to_csv(buf_uc, index=False, header=False, na_rep='\\N'); buf_uc.seek(0)
        cur.copy_expert(f"COPY {DB_SCHEMA}.unidade_consumidora ({','.join(df_uc.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf_uc)

        for table, df_tab in [
            (f"{DB_SCHEMA}.lead_energia", df_energia),
            (f"{DB_SCHEMA}.lead_demanda", df_demanda),
            (f"{DB_SCHEMA}.lead_qualidade", df_qualidade)
        ]:
            buf_tab = io.StringIO(); df_tab.to_csv(buf_tab, index=False, header=False, na_rep='\\N'); buf_tab.seek(0)
            cur.copy_expert(f"COPY {table} ({','.join(df_tab.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf_tab)

        cur.execute(
            f"INSERT INTO {DB_SCHEMA}.import_status(distribuidora,ano,camada,status) VALUES(%s,%s,%s,'success') ON CONFLICT(distribuidora,ano,camada) DO UPDATE SET status=EXCLUDED.status,data_execucao=now()",
            (distribuidora, ano, camada)
        )
    print(f"📤 Carga completa em {time.time() - t2:.2f}s — {camada} importado com sucesso!")
