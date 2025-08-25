# packages/orquestrator/orquestrador_job.py
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path
from subprocess import Popen, PIPE
from tqdm import tqdm

# ──────────────────────────────────────────────────────────────────────────────
# Paths e ambiente
# ──────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]  # raiz do repo (…/You_intelligence_062025)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# evitar erros de encoding (Windows) e habilitar unbuffered
ENV = os.environ.copy()
ENV.setdefault("PYTHONIOENCODING", "utf-8")
ENV.setdefault("PYTHONUTF8", "1")

# knobs default para UCBT (não precisa passar por CLI)
# ajuste aqui se quiser outros defaults globais
ENV.setdefault("UCBT_CHUNK_SIZE", "5000")
ENV.setdefault("UCBT_ROWS_PER_COPY", "20000")
ENV.setdefault("UCBT_SLEEP_MS_BETWEEN", "120")

# Caminho do Python da venv e PYTHONPATH
PYTHON_EXEC = sys.executable
ENV["PYTHONPATH"] = str(ROOT)

# ──────────────────────────────────────────────────────────────────────────────
# Importers mapeados (todos exigem --prefixo somente quando aplicável)
# ──────────────────────────────────────────────────────────────────────────────
IMPORTERS = {
    "UCAT": "packages/jobs/importers/importer_ucat_job.py",
    "UCMT": "packages/jobs/importers/importer_ucmt_job.py",
    "UCBT": "packages/jobs/importers/importer_ucbt_job.py",
    "PONNOT": "packages/jobs/importers/importer_ponnot_job.py",
}
CAMADAS = ["UCAT", "UCMT", "UCBT", "PONNOT"]

# Pasta onde estão os .gdb (cada .gdb é um diretório)
DOWNLOADS_DIR = ROOT / "data" / "downloads"


def _build_args(camada: str, gdb_path: Path, distribuidora: str, ano: int, prefixo: str) -> list[str]:
    """
    UCAT/UCMT usam --prefixo (para gerar import_id e mapear IDs).
    UCBT/PONNOT não precisam de --prefixo; eles já geram import_id internamente.
    """
    base = ["--gdb", str(gdb_path), "--distribuidora", distribuidora, "--ano", str(ano)]
    if camada in ("UCAT", "UCMT"):
        return base + ["--prefixo", prefixo, "--modo_debug"]
    else:
        return base + ["--modo_debug"]


def _stream_run(cmd: list[str], cwd: Path):
    """
    Executa subprocesso com streaming de stdout/stderr (UTF-8) sem bloquear.
    Retorna (returncode).
    """
    # text=True/universal_newlines garante str; encoding exige Python 3.10+
    with Popen(
        cmd,
        cwd=str(cwd),
        env=ENV,
        stdout=PIPE,
        stderr=PIPE,
        text=True,
        universal_newlines=True,
        bufsize=1,
    ) as proc:
        # stream organizado
        while True:
            line = proc.stdout.readline()
            err = proc.stderr.readline()
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()
            if err:
                # prefixo ajuda a separar dos prints do importer
                sys.stdout.write(f"[stderr] {err}")
                sys.stdout.flush()
            if not line and not err and proc.poll() is not None:
                break
        return proc.returncode


def rodar_importer(script_path: str, gdb_path: Path, camada: str, distribuidora: str, ano: int, prefixo: str):
    # import leve só para checar status (não traz dependências pesadas)
    try:
        from packages.jobs.utils.rastreio import get_status
    except Exception:
        def get_status(*_a, **_k):  # fallback mudo
            return None

    status = get_status(prefixo, ano, camada)
    if status == "completed":
        tqdm.write(f"[OK] Ja importado: {camada} {prefixo}")
        return

    script_abs = ROOT / script_path
    if not script_abs.exists():
        tqdm.write(f"[ERR] Importer não encontrado: {script_abs}")
        return

    args = _build_args(camada, gdb_path, distribuidora, ano, prefixo)
    tqdm.write(f"[RUN] Importando {camada} para {prefixo}")

    rc = _stream_run([PYTHON_EXEC, str(script_abs), *args], cwd=ROOT)

    if rc != 0:
        tqdm.write(f"[ERR] Importacao falhou {camada} ({prefixo}) (rc={rc})")
    else:
        tqdm.write(f"[DONE] {camada} {prefixo} importado com sucesso.")


def _descobrir_prefixos() -> list[str]:
    # Detecta todos os diretórios *.gdb em data/downloads
    if not DOWNLOADS_DIR.exists():
        return []
    return [p.stem for p in DOWNLOADS_DIR.glob("*.gdb")]


def orquestrar_importacao():
    tqdm.write("[INFO] Iniciando orquestrador (streaming)")

    prefixos = _descobrir_prefixos()
    if not prefixos:
        tqdm.write(f"[WARN] Nenhum .gdb encontrado em {DOWNLOADS_DIR}")
        return

    for prefixo in prefixos:
        gdb_dir = DOWNLOADS_DIR / f"{prefixo}.gdb"
        if not gdb_dir.exists():
            tqdm.write(f"[WARN] .gdb nao encontrado: {gdb_dir}")
            continue

        # Prefixo esperado: NOME_UF_YYYY (ex.: CPFL_Paulista_2023)
        try:
            distribuidora = prefixo.rsplit("_", 1)[0]
            ano = int(prefixo.rsplit("_", 1)[-1])
        except Exception:
            tqdm.write(f"[WARN] Prefixo invalido: {prefixo} — use formato NOME_UF_2023")
            continue

        for camada in CAMADAS:
            try:
                rodar_importer(IMPORTERS[camada], gdb_dir, camada, distribuidora, ano, prefixo)
            except Exception as e:
                tqdm.write(f"[ERR] Erro ao rodar {camada} ({prefixo}): {e}")
                continue

    tqdm.write("[INFO] Orquestracao finalizada.")


if __name__ == "__main__":
    orquestrar_importacao()
