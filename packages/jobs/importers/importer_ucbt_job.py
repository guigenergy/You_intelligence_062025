import os
import io
import time
import hashlib
import uuid
import pandas as pd
import pyogrio
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from packages.database.connection import get_db_cursor

load_dotenv()
DB_SCHEMA = os.getenv("DB_SCHEMA", "plead")


def _to_pg_array(data):
    return pd.Series([
        "{" + ",".join(map(str, row)) + "}" if len(row) else r"\N"
        for row in data
    ])


def hash_endereco(bairro, cep, municipio, dist):
    txt = f"{bairro or ''}-{cep or ''}-{municipio or ''}-{dist or ''}".lower()
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()


def normalizar_cep(cep):
    return ''.join(filter(str.isdigit, str(cep)))[:8] if cep else ""


def main(gdb_path: Path, distribuidora: str, ano: int,
         camada: str = "UCBT_tab", modo_debug: bool = False):
    print(f"🚨 DEBUG MODE ({camada}): {modo_debug}")

    # 1) Leitura
    t0 = time.time()
    df = pyogrio.read_dataframe(str(gdb_path), layer=camada, read_geometry=False)
    print(f"📥 Lido {len(df)} linhas de {camada} em {time.time()-t0:.2f}s")

    # 2) Transformações
    t1 = time.time()
    df["CEP"] = df["CEP"].astype(str).apply(normalizar_cep)
    df["id_interno"] = df.apply(
        lambda r: hash_endereco(r.get("BRR"), r.get("CEP"), r.get("MUN"), distribuidora),
        axis=1
    )
    df["uc_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Prepara lead
    df_lead = df[["id_interno", "CEP", "BRR", "MUN"]].drop_duplicates("id_interno").copy()
    df_lead["id"] = df_lead["id_interno"]
    df_lead["bairro"] = df_lead["BRR"]
    df_lead["cep"] = df_lead["CEP"]
    df_lead["municipio_ibge"] = df_lead["MUN"]
    df_lead["distribuidora"] = distribuidora
    df_lead["status"] = "raw"
    ts = datetime.utcnow()
    df_lead["ultima_atualizacao"] = [ts] * len(df_lead)

    cols_lead = [
        "id", "id_interno", "bairro", "cep",
        "municipio_ibge", "distribuidora", "status", "ultima_atualizacao"
    ]

    # Prepara unidade_consumidora
    df_uc = pd.DataFrame({
        "id": df["uc_id"],
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

    # Energia / Qualidade
    ene = _to_pg_array(df[[c for c in df.columns if c.startswith("ENE_")]].fillna(0).astype(int).values)
    dic = _to_pg_array(df[[c for c in df.columns if c.startswith("DIC_")]].fillna(0).astype(int).values)
    fic = _to_pg_array(df[[c for c in df.columns if c.startswith("FIC_")]].fillna(0).astype(int).values)

    df_energia   = pd.DataFrame({"id": df["uc_id"], "uc_id": df["uc_id"], "ene": ene, "potencia": df_uc["potencia"]})
    df_qualidade = pd.DataFrame({"id": df["uc_id"], "uc_id": df["uc_id"], "dic": dic, "fic": fic})
    print(f"🛠️ Transformado em {time.time()-t1:.2f}s")

    if modo_debug:
        return

    # 3) Carga otimizada com filtragem de duplicados
    t2 = time.time()
    with get_db_cursor(commit=True) as cur:
        # ▶▶▶ Filtra leads já existentes (tupla ou dict)
        cur.execute(
            f"SELECT id FROM {DB_SCHEMA}.lead WHERE id = ANY(%s)",
            (df_lead["id"].tolist(),)
        )
        rows = cur.fetchall()
        existentes = {
            (r["id"] if isinstance(r, dict) else r[0])
            for r in rows
        }
        df_novos = df_lead[~df_lead["id"].isin(existentes)]
        if not df_novos.empty:
            buf_lead = io.StringIO()
            df_novos[cols_lead].to_csv(buf_lead, index=False, header=False, na_rep='\\N')
            buf_lead.seek(0)
            cur.copy_expert(
                f"COPY {DB_SCHEMA}.lead ({','.join(cols_lead)}) "
                "FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                buf_lead
            )
        else:
            print("⏩ Nenhum lead novo para importar")

        # COPY unidade_consumidora
        buf_uc = io.StringIO()
        df_uc.to_csv(buf_uc, index=False, header=False, na_rep='\\N')
        buf_uc.seek(0)
        cur.copy_expert(
            f"COPY {DB_SCHEMA}.unidade_consumidora ({','.join(df_uc.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
            buf_uc
        )

        # COPY energia / qualidade
        for table, df_tab in [
            (f"{DB_SCHEMA}.lead_energia", df_energia),
            (f"{DB_SCHEMA}.lead_qualidade", df_qualidade)
        ]:
            buf_tab = io.StringIO()
            df_tab.to_csv(buf_tab, index=False, header=False, na_rep='\\N')
            buf_tab.seek(0)
            cur.copy_expert(
                f"COPY {table} ({','.join(df_tab.columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                buf_tab
            )

        # Atualiza status
        cur.execute(f"""
            INSERT INTO {DB_SCHEMA}.import_status(distribuidora,ano,camada,status)
            VALUES(%s,%s,%s,'success')
            ON CONFLICT(distribuidora,ano,camada)
            DO UPDATE SET status=EXCLUDED.status,data_execucao=now()
        """, (distribuidora, ano, camada))

    print(f"📤 Carga completa em {time.time()-t2:.2f}s — {camada} importado com sucesso!")
