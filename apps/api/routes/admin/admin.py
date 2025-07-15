from fastapi import APIRouter
from pathlib import Path
from packages.orquestrator.orquestrador_job import rodar_importer, IMPORTERS, CAMADAS
from packages.jobs.utils.rastreio import get_status

router = APIRouter()

GDB_DIR = Path("data/downloads")

@router.get("/admin/gdbs")
def listar_gdbs():
    gdbs = []
    for gdb_path in GDB_DIR.glob("*.gdb"):
        prefixo = gdb_path.stem
        try:
            distribuidora = prefixo.rsplit("_", 1)[0]
            ano = int(prefixo.rsplit("_", 1)[-1])
        except Exception:
            continue

        for camada in CAMADAS:
            status = get_status(prefixo, ano, camada)
            gdbs.append({
                "prefixo": prefixo,
                "ano": ano,
                "distribuidora": distribuidora,
                "camada": camada,
                "status": status
            })
    return gdbs

@router.post("/admin/importar")
def importar_dado(camada: str, prefixo: str, ano: int, distribuidora: str):
    script = IMPORTERS[camada]
    gdb_path = GDB_DIR / f"{prefixo}.gdb"
    if not gdb_path.exists():
        return {"erro": "Arquivo .gdb não encontrado"}

    rodar_importer(
    script_path=IMPORTERS["UCAT"],    # ou "UCMT", "UCBT", etc.
    gdb_path=Path("data/downloads/NOME.gdb"),
    camada="UCAT",
    distribuidora="ENEL",
    ano=2023,
    prefixo="ENEL_2023"
)
    return {"status": "importação iniciada"}
