from __future__ import annotations

from uuid import uuid4
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from packages.database.session import get_session
from packages.jobs.queue import enqueue
from apps.api.services import admin_service

# 👉 prefix agora é /v1 (bate com o front)
router = APIRouter(prefix="/v1", tags=["Admin"])

# ===== Schemas =====

# usado pelos seus endpoints já existentes
class ImportacaoPayload(BaseModel):
    distribuidora: str
    ano: int
    camadas: list[str] = ["UCBT", "PONNOT"]
    url: str | None = None

class DownloadPayload(BaseModel):
    distribuidora: str
    ano: int

class EnrichPayload(BaseModel):
    lead_ids: list[str] | None = None

# 👉 NOVO: payload que o seu frontend envia (múltiplos)
class ImportacaoSelecionados(BaseModel):
    distribuidoras: list[str] = Field(default_factory=list)
    anos: list[int] = Field(default_factory=list)

# mapeia nomes do UI -> nomes da VIEW dataset_url_normalized.distribuidora
UI_TO_DB = {
    "ENEL DISTRIBUIÇÃO SP": "ENEL_SP",
    "ENEL DISTRIBUIÇÃO RIO": "ENEL_RJ",
    "ELETROPAULO": "ENEL_SP",  # histórico
    "LIGHT": "LIGHT",
    "ENERGISA": "ENERGISA",    # ajuste se na sua view for outra sigla
    "NEOENERGIA": "NEOENERGIA",
    "EDP SP": "EDP_SP",
    "CPFL PAULISTA": "CPFL_PAULISTA",
    "CPFL PIRATININGA": "CPFL_PIRATININGA",
    "CPFL SANTA CRUZ": "CPFL_SANTA_CRUZ",
}

def map_status_ui(db_status: str, observacoes: str | None) -> Literal["concluido","erro","importando","baixando","extraindo","pendente"]:
    s = (db_status or "").lower()
    obs = (observacoes or "").lower()
    if s == "done":
        return "concluido"
    if s == "error":
        return "erro"
    if s == "queued":
        return "pendente"
    # running -> detalhar por observações se você marcar no worker
    if "baixando" in obs:
        return "baixando"
    if "extraindo" in obs:
        return "extraindo"
    return "importando"

# ===== Health =====

@router.get("/ping")
async def ping():
    return {"ok": True}

# ===== Importações / Downloads =====

# 👉 NOVO: endpoint que o seu ButtonImportar chama
@router.post("/importar")
async def importar_selecionados(payload: ImportacaoSelecionados, db: AsyncSession = Depends(get_session)):
    """
    Recebe { distribuidoras: string[], anos: number[] }
    Busca URLs na VIEW dataset_url_normalized e insere import_status = 'queued'.
    """
    distribs_ui = payload.distribuidoras or []
    anos = payload.anos or []
    if not distribs_ui or not anos:
        raise HTTPException(status_code=400, detail="Selecione pelo menos 1 distribuidora e 1 ano.")

    # converte UI -> nomes da view
    distribs_db = [UI_TO_DB.get(d, d) for d in distribs_ui]

    # busca linhas na view
    q = text("""
        SELECT id, distribuidora, ano, url, title
        FROM dataset_url_normalized
        WHERE distribuidora = ANY(:dists) AND ano = ANY(:anos)
        ORDER BY distribuidora, ano
    """)
    rs = await db.execute(q, {"dists": distribs_db, "anos": anos})
    rows = rs.fetchall()

    if not rows:
        return {"enfileirados": 0, "aviso": "Nenhuma URL encontrada para os filtros."}

    # insere import_status (1 linha por URL)
    insert_q = text("""
        INSERT INTO import_status
            (import_id, distribuidora_id, ano, camada, status, linhas_processadas,
             data_inicio, data_fim, observacoes, erro, distribuidora_nome)
        VALUES
            (:import_id, NULL, :ano, NULL, 'queued', 0,
             NULL, NULL, :observacoes, NULL, :distribuidora_nome)
    """)

    params = []
    for r in rows:
        params.append({
            "import_id": str(uuid4()),
            "ano": r._mapping["ano"],
            "observacoes": f"catalog:{r._mapping['id']}",
            "distribuidora_nome": r._mapping["distribuidora"],
        })

    # executa em lote
    await db.execute(insert_q, params)
    await db.commit()

    return {"enfileirados": len(params)}

# seus endpoints existentes continuam aqui (ajuste de prefix já aplica)
@router.post("/admin/importar")
async def importar(payload: ImportacaoPayload):
    return await admin_service.executar_importacao(payload)

@router.post("/download")
async def download_dataset(payload: DownloadPayload):
    job_id = enqueue({
        "download": {
            "distribuidora": payload.distribuidora,
            "ano": payload.ano,
            "max_kbps": 256
        }
    }, priority=5)
    return {"status": "queued", "job_id": job_id}

@router.get("/download/status")
async def status_download(distribuidora: str, ano: int, db: AsyncSession = Depends(get_session)):
    q = text("""
        SELECT status, tempo_download, erro, updated_at
        FROM intel_lead.download_log
        WHERE distribuidora = :dist AND ano = :ano
        ORDER BY updated_at DESC
        LIMIT 1
    """)
    rs = await db.execute(q, {"dist": distribuidora, "ano": ano})
    row = rs.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Download não encontrado.")
    return dict(row._mapping)

# ===== Métricas / Listagens rápidas =====

# 👉 AJUSTADO: retorna no formato que sua TabelaStatusImportacoes espera
@router.get("/import-status")
async def listar_importacoes(db: AsyncSession = Depends(get_session)):
    q = text("""
        SELECT distribuidora_nome, ano, camada, status, observacoes,
               COALESCE(data_fim, data_inicio, now()) AS data_ref
        FROM import_status
        ORDER BY data_ref DESC
        LIMIT 200
    """)
    rs = await db.execute(q)
    out = []
    for r in rs.fetchall():
        m = r._mapping
        out.append({
            "distribuidora": m["distribuidora_nome"],
            "ano": m["ano"],
            "camada": (m["camada"] or "UCMT"),  # default se ainda não definido
            "status": map_status_ui(m["status"], m["observacoes"]),
            "data_execucao": m["data_ref"].isoformat(),
        })
    return out

@router.get("/leads/status-count")
async def status_count(db: AsyncSession = Depends(get_session)):
    return await admin_service.contagem_por_status(db)

@router.get("/leads/distribuidoras-count")
async def count_por_distribuidora(db: AsyncSession = Depends(get_session)):
    return await admin_service.contagem_por_distribuidora(db)

@router.get("/leads/raw")
async def listar_leads_raw(db: AsyncSession = Depends(get_session)):
    return await admin_service.listar_leads_raw(db)

# ===== Enriquecimento (stubs seguros) =====

@router.post("/enriquecer")
async def enriquecer_tudo():
    return await admin_service.enriquecer_global()

@router.post("/enrich/geo")
async def enrich_google(payload: EnrichPayload):
    return await admin_service.enriquecer_google(payload)

@router.post("/enrich/cnpj")
async def enrich_cnpj(payload: EnrichPayload):
    return await admin_service.enriquecer_cnpj(payload)

# ===== Ops de banco (materializadas) =====

@router.post("/dashboard/refresh")
async def refresh_materializadas(db: AsyncSession = Depends(get_session)):
    return await admin_service.refresh_materializadas(db)
