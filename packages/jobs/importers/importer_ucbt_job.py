# packages/jobs/importers/importer_ucbt_job.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Importer UCBT — alinhado ao UCMT, robusto para alto volume.

- CLI padronizada: --gdb --ano --distribuidora --prefixo [--modo_debug]
- Detecção automática da layer UCBT
- Leitura em streaming (Fiona) com chunking (baixa RAM)
- COPY em micro-batches (configurável por env/CLI)
- uc_id = sha256(cod_id_ano_camada_dist) (mesmo padrão do UCMT)
- Séries:
    * energia_total preenchida (ponta/fora_ponta = NULL)
    * demanda_total e demanda_contratada
    * DIC/FIC/sem_rede
- Idempotência sem depender de UNIQUE(uc_id): dedup local antes do COPY
"""

import os
import io
import hashlib
import argparse
from pathlib import Path
from typing import Iterable, Tuple, List

import pandas as pd
import fiona
from tqdm import tqdm

from packages.database.connection import get_db_connection
from packages.jobs.utils.rastreio import registrar_status, gerar_import_id
from packages.jobs.utils.sanitize import (
    sanitize_cnae,
    sanitize_grupo_tensao,
    sanitize_modalidade,
    sanitize_tipo_sistema,
    sanitize_situacao,
    sanitize_classe,
    sanitize_pac,
    sanitize_str,
    sanitize_int,
    sanitize_numeric,
)

# ---------------------------------------------------------------------------
# Knobs (env) — pode sobrescrever via CLI
# ---------------------------------------------------------------------------
UCBT_CHUNK_SIZE = int(os.getenv("UCBT_CHUNK_SIZE", "500"))
UCBT_ROWS_PER_COPY = int(os.getenv("UCBT_ROWS_PER_COPY", "20000"))
UCBT_SLEEP_MS_BETWEEN = int(os.getenv("UCBT_SLEEP_MS_BETWEEN", "120"))

RELEVANT_COLUMNS = [
    "COD_ID", "DIST", "CNAE", "DAT_CON", "PAC", "GRU_TEN", "GRU_TAR", "TIP_SIST",
    "SIT_ATIV", "CLAS_SUB", "CONJ", "MUN", "BRR", "CEP", "PN_CON", "DESCR",
    "SEMRED", "DEM_CONT",
] + [f"ENE_{i:02d}" for i in range(1, 13)] \
  + [f"DEM_{i:02d}" for i in range(1, 13)] \
  + [f"DIC_{i:02d}" for i in range(1, 13)] \
  + [f"FIC_{i:02d}" for i in range(1, 13)]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def detectar_layer(gdb_path: Path) -> str | None:
    try:
        layers = set(fiona.listlayers(str(gdb_path)))
    except Exception:
        return None
    for cand in ("UCBT_tab", "UCBT_TAB", "UCBT", "ucbt_tab"):
        if cand in layers:
            return cand
    for ly in layers:
        if str(ly).upper().startswith("UCBT"):
            return ly
    return None

def gerar_uc_id(cod_id: str, ano: int, camada: str, distribuidora_id: int | str) -> str:
    base = f"{cod_id}_{ano}_{camada}_{distribuidora_id}"
    return hashlib.sha256(base.encode()).hexdigest()

def _ensure_columns(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df

def _as_records(props) -> dict:
    return dict(props or {})

def insert_copy(cur, df: pd.DataFrame, table: str, columns: list[str], rows_per_copy: int) -> int:
    if df.empty:
        return 0
    total = 0
    for i in range(0, len(df), rows_per_copy):
        chunk = df.iloc[i:i+rows_per_copy]
        buf = io.StringIO()
        chunk.to_csv(buf, index=False, header=False, columns=columns, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
        total += len(chunk)
    tqdm.write(f"Inserido em {table}: {total} registros")
    return total

def _sanitize_base_cols(gdf: pd.DataFrame) -> pd.DataFrame:
    """
    Padrão UCMT: sanitizers escalares com .apply, CEP como Int, etc.
    """
    return pd.DataFrame({
        "cod_id": gdf["COD_ID"].astype(str),
        "data_conexao": pd.to_datetime(gdf["DAT_CON"], errors="coerce"),
        "cnae": sanitize_cnae(gdf["CNAE"]),
        "grupo_tensao": gdf["GRU_TEN"].apply(sanitize_grupo_tensao),
        "modalidade": gdf["GRU_TAR"].apply(sanitize_modalidade),
        "tipo_sistema": gdf["TIP_SIST"].apply(sanitize_tipo_sistema),
        "situacao": gdf["SIT_ATIV"].apply(sanitize_situacao),
        "classe": gdf["CLAS_SUB"].apply(sanitize_classe),
        "segmento": None,
        "subestacao": None,
        "municipio_id": sanitize_int(gdf["MUN"]),
        "bairro": sanitize_str(gdf["BRR"]),
        "cep": sanitize_int(gdf["CEP"]),  # igual UCMT
        "pac": gdf["PAC"].apply(sanitize_pac),
        "pn_con": sanitize_str(gdf["PN_CON"]),
        "descricao": sanitize_str(gdf["DESCR"]),
    })

def _build_series_frames(gdf: pd.DataFrame, uc_ids: pd.Series, camada: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # energia: total preenchida; ponta/fora_ponta = NULL
    energia_frames = []
    for mes in range(1, 13):
        col = f"ENE_{mes:02d}"
        energia_frames.append(pd.DataFrame({
            "uc_id": uc_ids,
            "mes": mes,
            "energia_ponta": None,
            "energia_fora_ponta": None,
            "energia_total": sanitize_numeric(gdf.get(col)),
            "origem": camada
        }))
    energia_df = pd.concat(energia_frames, ignore_index=True)

    # demanda: total + contratada
    demanda_frames = []
    dem_contratada = sanitize_numeric(gdf.get("DEM_CONT"))
    for mes in range(1, 13):
        col = f"DEM_{mes:02d}"
        demanda_frames.append(pd.DataFrame({
            "uc_id": uc_ids,
            "mes": mes,
            "demanda_ponta": None,
            "demanda_fora_ponta": None,
            "demanda_total": sanitize_numeric(gdf.get(col)),
            "demanda_contratada": dem_contratada,
            "origem": camada
        }))
    demanda_df = pd.concat(demanda_frames, ignore_index=True)

    # qualidade: dic/fic + sem_rede
    qualidade_frames = []
    sem_rede = sanitize_numeric(gdf.get("SEMRED"))
    for mes in range(1, 13):
        qualidade_frames.append(pd.DataFrame({
            "uc_id": uc_ids,
            "mes": mes,
            "dic": sanitize_numeric(gdf.get(f"DIC_{mes:02d}")),
            "fic": sanitize_numeric(gdf.get(f"FIC_{mes:02d}")),
            "sem_rede": sem_rede,
            "origem": camada
        }))
    qualidade_df = pd.concat(qualidade_frames, ignore_index=True)

    return energia_df, demanda_df, qualidade_df

# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------
def importar_ucbt(
    gdb_path: Path,
    distribuidora: str,
    ano: int,
    prefixo: str,
    chunk_size: int = UCBT_CHUNK_SIZE,
    rows_per_copy: int = UCBT_ROWS_PER_COPY,
    sleep_ms_between: int = UCBT_SLEEP_MS_BETWEEN,
    modo_debug: bool = False,
):
    camada = "UCBT"
    import_id = gerar_import_id(prefixo, ano, camada)
    registrar_status(prefixo, ano, camada, "running", distribuidora_nome=distribuidora, import_id=import_id)

    try:
        layer = detectar_layer(gdb_path)
        if not layer:
            raise Exception("Camada UCBT não encontrada no GDB.")

        tqdm.write(f"Stream '{layer}' (chunk={chunk_size})")

        # 1) Varredura rápida para validar DIST único (padrão UCMT)
        dist_vals = []
        with fiona.open(str(gdb_path), layer=layer) as src:
            pbar = tqdm(total=len(src), desc=f"UCBT scan {distribuidora} {ano}", unit="reg")
            bucket = []
            for feat in src:
                bucket.append(_as_records(feat.get("properties")))
                if len(bucket) >= chunk_size:
                    df_raw = pd.DataFrame(bucket); bucket.clear()
                    df_raw = _ensure_columns(df_raw, RELEVANT_COLUMNS)
                    dist_vals.extend(sanitize_int(df_raw["DIST"]).dropna().tolist())
                pbar.update(1)
            if bucket:
                df_raw = pd.DataFrame(bucket); bucket.clear()
                df_raw = _ensure_columns(df_raw, RELEVANT_COLUMNS)
                dist_vals.extend(sanitize_int(df_raw["DIST"]).dropna().tolist())
            pbar.close()

        dist_unique = pd.Series(dist_vals).dropna().unique()
        if len(dist_unique) != 1:
            raise ValueError(f"Esperado um único código de distribuidora, mas encontrei: {dist_unique}")
        dist_id = int(dist_unique[0])
        tqdm.write(f"DIST confirmado: {dist_id}")

        # 2) Inserção efetiva em streaming (buffers + COPY)
        total_bruto = energia_total = demanda_total = qualidade_total = 0

        lb_cols = [
            "uc_id", "import_id", "cod_id", "distribuidora_id", "origem", "ano", "status",
            "data_conexao", "cnae", "grupo_tensao", "modalidade", "tipo_sistema",
            "situacao", "classe", "segmento", "subestacao", "municipio_id", "bairro",
            "cep", "pac", "pn_con", "descricao"
        ]
        e_cols = ["uc_id", "mes", "energia_ponta", "energia_fora_ponta", "energia_total", "origem"]
        d_cols = ["uc_id", "mes", "demanda_ponta", "demanda_fora_ponta", "demanda_total", "demanda_contratada", "origem"]
        q_cols = ["uc_id", "mes", "dic", "fic", "sem_rede", "origem"]

        with fiona.open(str(gdb_path), layer=layer) as src, get_db_connection() as conn:
            cur = conn.cursor()
            pbar = tqdm(total=len(src), desc=f"UCBT import {distribuidora} {ano}", unit="reg")

            buf_lb: List[dict] = []
            buf_e: List[dict]  = []
            buf_d: List[dict]  = []
            buf_q: List[dict]  = []

            def _flush():
                nonlocal total_bruto, energia_total, demanda_total, qualidade_total
                if not buf_lb:
                    return

                df_lb = pd.DataFrame(buf_lb, columns=lb_cols)

                # idempotência: dedup local por uc_id
                if df_lb.duplicated(subset=["uc_id"]).any():
                    qtd = df_lb.duplicated(subset=["uc_id"], keep=False).sum()
                    tqdm.write(f"{qtd} uc_id duplicados no buffer — removidos.")
                    df_lb = df_lb.drop_duplicates(subset=["uc_id"], keep="first").reset_index(drop=True)

                insert_copy(cur, df_lb, "lead_bruto", lb_cols, rows_per_copy)
                total_bruto += len(df_lb)
                conn.commit()

                # mapear IDs via import_id (padrão UCMT)
                df_ids = pd.read_sql(
                    "SELECT id AS lead_bruto_id, uc_id FROM lead_bruto WHERE import_id = %s",
                    conn, params=(import_id,)
                )
                if not df_ids.empty:
                    id_map = df_ids.set_index("uc_id")["lead_bruto_id"]

                    if buf_e:
                        df_e = pd.DataFrame(buf_e, columns=e_cols)
                        df_e = df_e.merge(id_map.rename("lead_bruto_id"), left_on="uc_id", right_index=True, how="inner")
                        df_e.drop(columns=["uc_id"], inplace=True)
                        insert_copy(cur, df_e, "lead_energia_mensal", df_e.columns.tolist(), rows_per_copy)
                        energia_total += len(df_e)

                    if buf_d:
                        df_d = pd.DataFrame(buf_d, columns=d_cols)
                        df_d = df_d.merge(id_map.rename("lead_bruto_id"), left_on="uc_id", right_index=True, how="inner")
                        df_d.drop(columns=["uc_id"], inplace=True)
                        insert_copy(cur, df_d, "lead_demanda_mensal", df_d.columns.tolist(), rows_per_copy)
                        demanda_total += len(df_d)

                    if buf_q:
                        df_q = pd.DataFrame(buf_q, columns=q_cols)
                        df_q = df_q.merge(id_map.rename("lead_bruto_id"), left_on="uc_id", right_index=True, how="inner")
                        df_q.drop(columns=["uc_id"], inplace=True)
                        insert_copy(cur, df_q, "lead_qualidade_mensal", df_q.columns.tolist(), rows_per_copy)
                        qualidade_total += len(df_q)

                    conn.commit()

                buf_lb.clear(); buf_e.clear(); buf_d.clear(); buf_q.clear()

                if sleep_ms_between > 0:
                    # respiro para não saturar I/O em Windows/OneDrive
                    import time as _t
                    _t.sleep(sleep_ms_between / 1000.0)

            bucket = []
            for feat in src:
                bucket.append(_as_records(feat.get("properties")))
                if len(bucket) >= chunk_size:
                    df_raw = pd.DataFrame(bucket); bucket.clear()
                    df_raw = _ensure_columns(df_raw, RELEVANT_COLUMNS)
                    df_raw = df_raw[df_raw["COD_ID"].notna()].reset_index(drop=True)
                    if df_raw.empty:
                        pbar.update(chunk_size); continue

                    base = _sanitize_base_cols(df_raw)
                    uc_ids = pd.Series([gerar_uc_id(c, ano, "UCBT", dist_id) for c in base["cod_id"]], index=base.index)

                    df_bruto = pd.DataFrame({
                        "uc_id": uc_ids,
                        "import_id": import_id,
                        "cod_id": base["cod_id"],
                        "distribuidora_id": dist_id,
                        "origem": "UCBT",
                        "ano": ano,
                        "status": "raw",
                        "data_conexao": base["data_conexao"],
                        "cnae": base["cnae"],
                        "grupo_tensao": base["grupo_tensao"],
                        "modalidade": base["modalidade"],
                        "tipo_sistema": base["tipo_sistema"],
                        "situacao": base["situacao"],
                        "classe": base["classe"],
                        "segmento": base["segmento"],
                        "subestacao": base["subestacao"],
                        "municipio_id": base["municipio_id"],
                        "bairro": base["bairro"],
                        "cep": base["cep"],
                        "pac": base["pac"],
                        "pn_con": base["pn_con"],
                        "descricao": base["descricao"],
                    })
                    e_df, d_df, q_df = _build_series_frames(df_raw, uc_ids, "UCBT")

                    buf_lb.extend(df_bruto.to_dict(orient="records"))
                    buf_e.extend(e_df.to_dict(orient="records"))
                    buf_d.extend(d_df.to_dict(orient="records"))
                    buf_q.extend(q_df.to_dict(orient="records"))

                    if len(buf_lb) >= rows_per_copy:
                        _flush()

                    pbar.update(len(df_raw))

            if bucket:
                df_raw = pd.DataFrame(bucket); bucket.clear()
                df_raw = _ensure_columns(df_raw, RELEVANT_COLUMNS)
                df_raw = df_raw[df_raw["COD_ID"].notna()].reset_index(drop=True)
                if not df_raw.empty:
                    base = _sanitize_base_cols(df_raw)
                    uc_ids = pd.Series([gerar_uc_id(c, ano, "UCBT", dist_id) for c in base["cod_id"]], index=base.index)

                    df_bruto = pd.DataFrame({
                        "uc_id": uc_ids,
                        "import_id": import_id,
                        "cod_id": base["cod_id"],
                        "distribuidora_id": dist_id,
                        "origem": "UCBT",
                        "ano": ano,
                        "status": "raw",
                        "data_conexao": base["data_conexao"],
                        "cnae": base["cnae"],
                        "grupo_tensao": base["grupo_tensao"],
                        "modalidade": base["modalidade"],
                        "tipo_sistema": base["tipo_sistema"],
                        "situacao": base["situacao"],
                        "classe": base["classe"],
                        "segmento": base["segmento"],
                        "subestacao": base["subestacao"],
                        "municipio_id": base["municipio_id"],
                        "bairro": base["bairro"],
                        "cep": base["cep"],
                        "pac": base["pac"],
                        "pn_con": base["pn_con"],
                        "descricao": base["descricao"],
                    })
                    e_df, d_df, q_df = _build_series_frames(df_raw, uc_ids, "UCBT")

                    buf_lb.extend(df_bruto.to_dict(orient="records"))
                    buf_e.extend(e_df.to_dict(orient="records"))
                    buf_d.extend(d_df.to_dict(orient="records"))
                    buf_q.extend(q_df.to_dict(orient="records"))

                _flush()
            pbar.close()

        registrar_status(
            prefixo, ano, camada, "completed",
            linhas_processadas=total_bruto,
            observacoes=f"{energia_total} energia | {demanda_total} demanda | {qualidade_total} qualidade",
            import_id=import_id
        )
        tqdm.write(f"Importação UCBT finalizada com {total_bruto} registros.")

    except Exception as e:
        tqdm.write(f"Erro ao importar UCBT: {e}")
        registrar_status(prefixo, ano, camada, "failed", erro=str(e), import_id=import_id)
        if modo_debug:
            raise

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gdb", required=True, type=Path)
    parser.add_argument("--ano", required=True, type=int)
    parser.add_argument("--distribuidora", required=True)
    parser.add_argument("--prefixo", required=True)
    parser.add_argument("--chunk-size", type=int, default=UCBT_CHUNK_SIZE)
    parser.add_argument("--rows-per-copy", type=int, default=UCBT_ROWS_PER_COPY)
    parser.add_argument("--sleep-ms-between", type=int, default=UCBT_SLEEP_MS_BETWEEN)
    parser.add_argument("--modo_debug", action="store_true")
    args = parser.parse_args()

    importar_ucbt(
        gdb_path=args.gdb,
        distribuidora=args.distribuidora,
        ano=args.ano,
        prefixo=args.prefixo,
        chunk_size=args.chunk_size,
        rows_per_copy=args.rows_per_copy,
        sleep_ms_between=args.sleep_ms_between,
        modo_debug=args.modo_debug,
    )
