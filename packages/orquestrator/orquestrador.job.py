#!/usr/bin/env python3
import sys
import asyncio
import traceback
from pathlib import Path

# Adiciona o root do projeto para os imports funcionarem
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# from packages.jobs.importers.importer_ucat_job import main as importar_ucat
# from packages.jobs.importers.importer_ucmt_job import main as importar_ucmt
from packages.jobs.importers.importer_ucbt_job import main as importar_ucbt
from packages.jobs.importers.importer_ponnot_job import main as importar_ponnot

# Diretório onde os arquivos GDB descompactados são salvos
GDB_DIR = Path("data/downloads")

# Mapeia cada camada ao seu importer
BASES = {
    # "UCAT_tab": importar_ucat,
    # "UCMT_tab": importar_ucmt,
    "UCBT_tab": importar_ucbt,
    "PONNOT":   importar_ponnot,
}


def encontrar_gdb(prefixo: str, ano: int) -> Path | None:
    """
    Busca um GDB extraído local com o padrão {prefixo}_{ano}*.gdb
    """
    candidatos = list(GDB_DIR.glob(f"{prefixo}_{ano}*.gdb"))
    return candidatos[0] if candidatos else None


async def importar_distribuidora(distribuidora: str, prefixo: str, ano: int):
    gdb = encontrar_gdb(prefixo, ano)
    if not gdb:
        print(f"⚠️  GDB não encontrado para {distribuidora} {ano}")
        return

    for camada, importer in BASES.items():
        print(f"\n🔄 Iniciando importação: {camada} | {distribuidora} {ano}")
        try:
            importer(
    gdb_path=gdb,
    distribuidora=distribuidora,
    ano=ano,
    prefixo=prefixo,
    camada=camada,
    modo_debug=False
)


        except Exception:
            print(f"❌ Erro real ao importar {camada} para {distribuidora} {ano}:")
            traceback.print_exc()


async def rodar_orquestrador(selecionados: list[dict]):
    """
    Recebe lista de dicts:
    [
      {"nome": "ENEL DISTRIBUIÇÃO RIO", "prefixo": "Enel_RJ_383", "ano": 2023},
      ...
    ]
    """
    for item in selecionados:
        await importar_distribuidora(item["nome"], item["prefixo"], item["ano"])


if __name__ == "__main__":
    # exemplo local
    DISTRIBUIDORAS = [
        {"nome": "ENEL DISTRIBUIÇÃO RIO", "prefixo": "Enel_RJ_383", "ano": 2023},
        {"nome": "CPFL PAULISTA",         "prefixo": "CPFL_Paulista_63", "ano": 2023},
    ]
    asyncio.run(rodar_orquestrador(DISTRIBUIDORAS))
