#!/usr/bin/env python3
import sys
import asyncio
import traceback
from pathlib import Path

# para permitir imports a partir da raiz do projeto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from packages.jobs.importers.importer_ucbt_job import main as importar_ucbt
from packages.jobs.importers.importer_ponnot_job import main as importar_ponnot
from packages.jobs.utils.rastreio import registrar_status

# onde estão os GDBs extraídos
GDB_DIR = Path("data/downloads")

# camada -> função de import
BASES = {
    # "UCAT_tab": importar_ucat,
    # "UCMT_tab": importar_ucmt,
    "UCBT_tab": importar_ucbt,
    "PONNOT":   importar_ponnot,
}

def encontrar_gdb(prefixo: str, ano: int) -> Path | None:
    """
    Busca um GDB no padrão {prefixo}_{ano}*.gdb dentro de GDB_DIR
    """
    candidatos = list(GDB_DIR.glob(f"{prefixo}_{ano}*.gdb"))
    return candidatos[0] if candidatos else None

async def importar_distribuidora(distribuidora: str, prefixo: str, ano: int):
    gdb = encontrar_gdb(prefixo, ano)
    if not gdb:
        print(f"⚠️  GDB não encontrado para {distribuidora} {ano}")
        await registrar_status(prefixo, ano, camada="ALL", status="gdb_not_found")
        return

    for camada, importer in BASES.items():
        print(f"\n🔄 Iniciando importação: {camada} | {distribuidora} {ano}")
        await registrar_status(prefixo, ano, camada=camada, status="started")
        try:
            importer(
                gdb_path=gdb,
                distribuidora=distribuidora,
                ano=ano,
                prefixo=prefixo,
                camada=camada,
                modo_debug=False
            )
        except Exception as e:
            print(f"❌ Erro ao importar {camada} para {distribuidora} {ano}:")
            traceback.print_exc()
            await registrar_status(prefixo, ano, camada=camada, status=f"failed: {e}")
            # opcional: continue para tentar próxima camada,
            # ou pare tudo:
            # return
        else:
            print(f"✅ Importação concluída: {camada} | {distribuidora} {ano}")
            await registrar_status(prefixo, ano, camada=camada, status="success")

async def rodar_orquestrador(selecionados: list[dict]):
    """
    Recebe lista de dicts:
    [
      {"nome": "CPFL PAULISTA", "prefixo": "CPFL_Paulista_63", "ano": 2023},
      {"nome": "ENEL DISTRIBUIÇÃO RIO", "prefixo": "Enel_RJ_383", "ano": 2023},
      ...
    ]
    """
    for item in selecionados:
        await importar_distribuidora(item["nome"], item["prefixo"], item["ano"])

if __name__ == "__main__":
    # defina aqui as distribuidoras a processar
    DISTRIBUIDORAS = [
        {"nome": "CPFL PAULISTA",        "prefixo": "CPFL_Paulista_63",   "ano": 2023},
        {"nome": "ENEL DISTRIBUIÇÃO RIO", "prefixo": "Enel_RJ_383",         "ano": 2023},
        # adicione mais conforme necessário
    ]
    asyncio.run(rodar_orquestrador(DISTRIBUIDORAS))
