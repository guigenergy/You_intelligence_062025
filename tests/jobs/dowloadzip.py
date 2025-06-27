import requests, zipfile, os
from pathlib import Path
import geopandas as gpd
import fiona
import urllib3

# ─── DESATIVA AVISOS SSL ───────────────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── CONFIGURAÇÕES GERAIS ──────────────────────────────────────
ABOUT_URL = "https://dadosabertos-aneel.opendata.arcgis.com/datasets/4fd3c2e1dae145e5b9974ef81d9f9641/about"
DISTRIBUIDORA = "Enel_RJ"
ANO = 2023
DOWNLOAD_FOLDER = Path("data/downloads")
DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

# ─── FUNÇÕES ────────────────────────────────────────────────────

def gerar_link_zip(about_url: str) -> str:
    parts = about_url.strip("/").split("/")
    arcgis_id = parts[-2] if parts[-1] == "about" else parts[-1]
    return f"https://www.arcgis.com/sharing/rest/content/items/{arcgis_id}/data"


def verificar_arquivo_existente(distribuidora: str, ano: int, pasta: Path) -> Path | None:
    for arquivo in pasta.glob("*.zip"):
        nome = arquivo.name.lower()
        if distribuidora.lower() in nome and str(ano) in nome and arquivo.stat().st_size > 10000:
            print(f"🟡 ZIP já existe: {arquivo.name}")
            return arquivo
    return None

def baixar_arquivo_zip(zip_url: str, pasta_destino: Path) -> Path:
    print("📥 Baixando ZIP...")
    r = requests.get(zip_url, stream=True, verify=False)
    content_disposition = r.headers.get("Content-Disposition", "")
    nome_arquivo = "arquivo_desconhecido.zip"

    if "filename=" in content_disposition:
        nome_arquivo = content_disposition.split("filename=")[-1].replace('"', '').strip()

    zip_path = pasta_destino / nome_arquivo
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    print("✅ ZIP baixado:", zip_path.name)
    return zip_path

def extrair_zip(zip_path: Path, destino: Path):
    print("📂 Extraindo ZIP...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(destino)
    print("✅ Arquivos extraídos para:", destino)

def listar_camadas_gdb(pasta: Path):
    gdbs = list(pasta.glob("*.gdb"))
    if not gdbs:
        raise FileNotFoundError("❌ Nenhuma GDB encontrada na pasta extraída.")

    gdb_path = gdbs[0]
    print(f"📁 GDB encontrada: {gdb_path.name}")
    
    layers = fiona.listlayers(gdb_path)
    print("📋 Camadas encontradas:")
    for layer in layers:
        print(" └──", layer)
    
    return gdb_path, layers

def mostrar_campos_e_amostra(gdb_path, layer):
    print(f"🔍 Amostra da camada: {layer}")
    df = gpd.read_file(gdb_path, layer=layer)
    print(f"🔸 {len(df)} linhas | {len(df.columns)} colunas")
    print("🔹 Colunas:", list(df.columns))
    print(df.head())

# ─── EXECUÇÃO ───────────────────────────────────────────────────

zip_url = gerar_link_zip(ABOUT_URL)

# Etapa 1: verifica se já existe
ZIP_PATH = verificar_arquivo_existente(DISTRIBUIDORA, ANO, DOWNLOAD_FOLDER)
if not ZIP_PATH:
    ZIP_PATH = baixar_arquivo_zip(zip_url, DOWNLOAD_FOLDER)

# Etapa 2: extrai para pasta com nome baseado no ZIP
nome_extraido = ZIP_PATH.stem.replace(".gdb", "")  # tira .gdb do final do nome
EXTRACT_FOLDER = DOWNLOAD_FOLDER / nome_extraido
EXTRACT_FOLDER.mkdir(parents=True, exist_ok=True)
extrair_zip(ZIP_PATH, EXTRACT_FOLDER)

# Etapa 3: analisa conteúdo do .gdb
gdb_path, layers = listar_camadas_gdb(EXTRACT_FOLDER)
camada_alvo = layers[0]  # ou "EQME", etc
mostrar_campos_e_amostra(gdb_path, camada_alvo)
