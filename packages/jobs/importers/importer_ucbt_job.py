# packages/jobs/importers/importer_ucbt.job.py

import os
import io
import hashlib
import pandas as pd
import geopandas as gpd
from pathlib import Path
from fiona import listlayers
from dotenv import load_dotenv
from datetime import datetime
from packages.database.connection import get_db_cursor

load_dotenv()

def _to_pg_array(data):
    return pd.Series(["{" + ",".join(map(str, row)) + "}" if len(row) > 0 else r"\N" for row in data])

def hash_endereco(bairro, cep, municipio, dist):
    txt = f"{bairro or ''}-{cep or ''}-{municipio or ''}-{dist or ''}".lower()
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()

def normalizar_cep(cep):
    return ''.join(filter(str.isdigit, str(cep)))[:8] if cep else ""

def main(gdb_path: Path, distribuidora: str, ano: int, camada: str = "UCBT_tab", modo_debug: bool = False):
    if camada not in listlayers(gdb_path):
        print(f"❌ Camada {camada} não encontrada")
        return

    print(f"📥 Lendo camada {camada}")
    df = gpd.read_file(gdb_path, layer=camada)

    df["CEP"] = df["CEP"].astype(str).apply(normalizar_cep)
    df["id_interno"] = df.apply(lambda r: hash_endereco(r.get("BRR"), r.get("CEP"), r.get("MUN"), distribuidora), axis=1)

    with get_db_cursor() as cur:
        cur.execute("SELECT cod_id FROM plead.unidade_consumidora WHERE cod_id = ANY(%s)", (list(df["COD_ID"]),))
        cods_existentes = {r[0] for r in cur.fetchall()}
    df = df[~df["COD_ID"].isin(cods_existentes)]

    if df.empty:
        print("⚠️ Nenhum novo dado para importar")
        return

    df_lead = df[["id_interno", "CEP", "BRR", "MUN"]].drop_duplicates("id_interno").copy()
    df_lead["id"] = df_lead["id_interno"]
    df_lead["bairro"] = df_lead["BRR"]
    df_lead["cep"] = df_lead["CEP"]
    df_lead["municipio_ibge"] = df_lead["MUN"]
    df_lead["distribuidora"] = distribuidora
    df_lead["status"] = "raw"
    df_lead["ultima_atualizacao"] = datetime.utcnow()
    df_lead = df_lead[["id", "id_interno", "bairro", "cep", "municipio_ibge", "distribuidora", "status", "ultima_atualizacao"]]

    df_uc = pd.DataFrame({
        "id": df["COD_ID"],
        "cod_id": df["COD_ID"],
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
        "potencia": df.get("CAR_INST", pd.Series([0]*len(df))).fillna(0).astype(float)
    })

    ene = _to_pg_array(df[[c for c in df.columns if c.startswith("ENE_")]].fillna(0).astype(int).values)
    dic = _to_pg_array(df[[c for c in df.columns if c.startswith("DIC_")]].fillna(0).astype(int).values)
    fic = _to_pg_array(df[[c for c in df.columns if c.startswith("FIC_")]].fillna(0).astype(int).values)

    df_energia = pd.DataFrame({
        "id": df["COD_ID"], "uc_id": df["COD_ID"],
        "ene": ene, "potencia": df_uc["potencia"]
    })

    df_qualidade = pd.DataFrame({
        "id": df["COD_ID"], "uc_id": df["COD_ID"],
        "dic": dic, "fic": fic
    })

    if modo_debug:
        print(f"[DEBUG] Leads novos: {len(df_lead)}")
        print(f"[DEBUG] UCs novas: {len(df_uc)}")
        return

    with get_db_cursor(commit=True) as cur:
        for table, data in [
            ("plead.lead", df_lead),
            ("plead.unidade_consumidora", df_uc),
            ("plead.lead_energia", df_energia),
            ("plead.lead_qualidade", df_qualidade)
        ]:
            if data.empty: continue
            buf = io.StringIO()
            data.to_csv(buf, index=False, header=False, na_rep=r"\N")
            buf.seek(0)
            cur.copy_expert(f"COPY {table} ({','.join(data.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
            print(f"📤 Inseridos {len(data)} registros em {table}")

        cur.execute("""
            INSERT INTO plead.import_status (distribuidora, ano, camada, status)
            VALUES (%s, %s, %s, 'success')
            ON CONFLICT (distribuidora, ano, camada)
            DO UPDATE SET status = EXCLUDED.status, data_execucao = now()
        """, (distribuidora, ano, camada))

    print("✅ UCBT importado com sucesso!")
