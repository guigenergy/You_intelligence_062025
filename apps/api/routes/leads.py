from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from packages.database.session import get_session
from apps.api.schemas.lead import (
    LeadList,
    LeadOut,
    LeadDetail,        # só se existir mesmo no schema
    LeadMapOut,
    LeadResumo,
    LeadQualidade,     # precisa estar declarado no schemas/lead.py
)
from apps.api.services import lead_service
from sqlalchemy import select
router = APIRouter(prefix="/leads", tags=["leads"])

@router.get("", response_model=LeadList)
async def list_leads(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_session),
):
    total, items = await lead_service.list_leads(db, skip, limit)
    return {"total": total, "items": items}


@router.get("/{lead_id}", response_model=LeadDetail)
async def lead_detail(
    lead_id: str,
    db: AsyncSession = Depends(get_session),
):
    lead = await lead_service.get_lead(db, lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return lead


@router.get("/heatmap", response_model=list[tuple[float, float, int]])
async def heatmap(
    segment: str | None = Query(None),
    db: AsyncSession = Depends(get_session),
):
    return await lead_service.heatmap_points(db, segment)


@router.get("/map", response_model=list[LeadMapOut])
async def get_lead_points(
    status: str | None = Query(None),
    distribuidora: str | None = Query(None),
    limit: int = Query(default=1000),
    db: AsyncSession = Depends(get_session),
):
    return await lead_service.get_map_points(db, status, distribuidora, limit)


@router.get("/resumo", response_model=LeadResumo)
async def resumo(
    estado: str | None = Query(None),
    municipio: str | None = Query(None),
    segmento: str | None = Query(None),
    db: AsyncSession = Depends(get_session),
):
    return await lead_service.get_resumo(db, estado, municipio, segmento)


@router.get("/qualidade/{lead_id}", response_model=LeadQualidade)
async def lead_qualidade(
    lead_id: str,
    db: AsyncSession = Depends(get_session),
):
    qualidade = await lead_service.get_qualidade(db, lead_id)
    if not qualidade:
        raise HTTPException(status_code=404, detail="Qualidade não encontrada")
    return qualidade

@router.get("/distribuidoras", response_model=list[str])
async def listar_distribuidoras(db: AsyncSession = Depends(get_session)):
    result = await db.execute(
        select(Lead.distribuidora).distinct().order_by(Lead.distribuidora)
    )
    return [row[0] for row in result.all()]