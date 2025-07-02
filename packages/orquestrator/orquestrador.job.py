#!/usr/bin/env python3
import sys
import asyncio
from pathlib import Path

# Adiciona o root do projeto para os imports funcionarem
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from packages.jobs.importers.importer_ucat_job import main as importar_ucat
from packages.jobs.importers.importer_ucmt_job import main as importar_ucmt
from packages.jobs.importers.importer_ucbt_job import main as importar_ucbt
from packages.jobs.importers.importer_ponnot_job import main as importar_ponnot

# Diretório onde os arquivos GDB descompactados são salvos
GDB_DIR = Path("data/downloads")

# Dicionário com os importadores por camada
BASES = {
    "UCAT_tab": importar_ucat,
    "UCMT_tab": importar_ucmt,
    "UCBT_tab": importar_ucbt,
    "PONNOT": importar_ponnot,
}

def encontrar_gdb(prefixo: str, ano: int) -> Path | None:
    """
    Busca um GDB extraído local com o padrão {prefixo}_{ano}*.gdb
    """
    candidatos = list(GDB_DIR.glob(f"{prefixo}_{ano}*.gdb"))
    return candidatos[0] if candidatos else None

async def main():
    distribuidoras = [
        "ENEL DISTRIBUIÇÃO RIO",
        "CPFL PAULISTA",
        "CEMIG DISTRIBUIÇÃO",
        # adicionar mais distribuidoras conforme necessário
    ]
    anos = [2020, 2021, 2022, 2023]

    for dist in distribuidoras:
        for ano in anos:
            prefixo = dist.replace(" ", "_")
            gdb_path = encontrar_gdb(prefixo, ano)

            if not gdb_path:
                print(f"\n⚠️  GDB não encontrado para {dist} {ano}\n")
                continue

            for camada, job in BASES.items():
                print(f"\n🔄 Iniciando importação: {camada} | {dist} {ano}")
                try:
                    await job(gdb_path=str(gdb_path), distribuidora=dist, ano=ano)
                except Exception as e:
                    print(f"❌ Erro real ao importar {camada} para {dist} {ano}:\n{e}")

if __name__ == "__main__":
    asyncio.run(main())
