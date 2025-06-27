import json
import logging
from playwright.sync_api import sync_playwright

# Setup de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def iniciar_json():
    with open("aneel_datasets_completos.json", "w", encoding="utf-8") as f:
        f.write("[\n")

def salvar_json_incremental(dado, primeira_vez=False):
    with open("aneel_datasets_completos.json", "a", encoding="utf-8") as f:
        if not primeira_vez:
            f.write(",\n")
        json.dump(dado, f, indent=2, ensure_ascii=False)

def finalizar_json():
    with open("aneel_datasets_completos.json", "a", encoding="utf-8") as f:
        f.write("\n]")

# 🔄 Rola e clica no botão "Mais resultados"
def carregar_todos_os_resultados(page):
    try:
        logging.info("🔽 Rolando até o final da página para revelar o botão...")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        logging.info("🔘 Buscando e clicando no botão 'Mais resultados'...")
        encontrou = page.evaluate("""
            () => {
                const allButtons = Array.from(document.querySelectorAll('calcite-button'));
                for (const btn of allButtons) {
                    const shadowBtn = btn.shadowRoot?.querySelector('button');
                    if (shadowBtn && shadowBtn.textContent.includes("Mais resultados") && !shadowBtn.disabled) {
                        shadowBtn.click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if encontrou:
            logging.info("✅ Clique no botão 'Mais resultados' realizado com sucesso.")
            page.wait_for_timeout(4000)
        else:
            logging.warning("⚠️ Botão 'Mais resultados' não encontrado ou desabilitado.")
    except Exception as e:
        logging.warning(f"⚠️ Erro ao tentar clicar no botão: {e}")

    try:
        logging.info("🔽 Rolando até o final da página para revelar o botão...")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        logging.info("🔘 Aguardando botão 'Mais resultados' aparecer...")
        page.wait_for_selector("calcite-button", timeout=5000)

        logging.info("🖱️ Clicando no botão 'Mais resultados'...")
        page.evaluate("""
            () => {
                const calciteButton = document.querySelector('calcite-button');
                const shadowRoot = calciteButton?.shadowRoot;
                const button = shadowRoot?.querySelector('button');

                if (button && !button.disabled && button.getAttribute('aria-disabled') !== 'true') {
                    button.click();
                }
            }
        """)
        logging.info("✅ Clique realizado com sucesso.")
        page.wait_for_timeout(4000)  # tempo para carregar nova página
    except Exception as e:
        logging.warning(f"⚠️ Erro ao clicar no botão: {e}")

# 🔍 Extrai links dos cards e salva item por item
def extract_dataset_links(page):
    links_extraidos = []
    primeira_vez_flag = True

    try:
        page.wait_for_selector("li arcgis-hub-entity-card", timeout=10000)
        list_items = page.query_selector_all("li")

        for index, li in enumerate(list_items):
            if index >= 24:
                logging.info("⛔ Limite de 24 registros atingido.")
                break
            try:
                link_element = li.query_selector("h3.title a")
                if link_element:
                    titulo = link_element.inner_text().strip()
                    href = link_element.get_attribute("href")

                    if href and href.startswith("/datasets/"):
                        dataset_id = href.split("/")[2]
                        url_about = f"https://dadosabertos-aneel.opendata.arcgis.com{href}/about"
                        url_download = f"https://www.arcgis.com/sharing/rest/content/items/{dataset_id}/data"

                        dado = {
                            "titulo": titulo,
                            "url": url_about,
                            "id": dataset_id,
                            "download": url_download
                        }

                        salvar_json_incremental(dado, primeira_vez_flag)
                        primeira_vez_flag = False
                        logging.info(f"📝 Salvando no JSON: {dado['titulo']}")
                        links_extraidos.append(dado)

            except Exception as e:
                logging.warning(f"⚠️ Erro ao processar <li>: {e}")

    except Exception as e:
        logging.error(f"❌ Falha ao extrair os links: {e}")

    return links_extraidos

# 🧩 Função principal
def scrape_aneel_all_datasets():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=False)
        page = context.new_page()

        url = "https://dadosabertos-aneel.opendata.arcgis.com/search?tags=distribuicao"
        page.goto(url)
        page.wait_for_timeout(4000)

        iniciar_json()
        carregar_todos_os_resultados(page)
        links = extract_dataset_links(page)
        finalizar_json()

        print(f"Total de registros extraídos: {len(links)}")
        browser.close()

        logging.info(f"✅ Total de datasets salvos: {len(links)}")
        logging.info("📁 Arquivo salvo: aneel_datasets_completos.json")

# 🚀 Executa o scraper
scrape_aneel_all_datasets()
