from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

driver = webdriver.Chrome()
driver.get("https://dadosabertos-aneel.opendata.arcgis.com/search?tags=distribuicao")

# Aguarda o primeiro carregamento dos cards
WebDriverWait(driver, 15).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "calcite-card"))
)

# Clica no botão "Mais resultados"
driver.execute_script('''
    const calciteButtons = document.querySelectorAll("calcite-button");
    for (let btn of calciteButtons) {
        const shadow = btn.shadowRoot;
        if (!shadow) continue;
        const realBtn = shadow.querySelector("button");
        if (!realBtn) continue;
        const span = realBtn.querySelector("span.content");
        if (!span) continue;
        if (span.textContent.trim().toLowerCase() === "mais resultados") {
            realBtn.click();
            break;
        }
    }
''')

# Aguarda a quantidade de cards aumentar
WebDriverWait(driver, 10).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "calcite-card")) > 12)

# Conta os cards após clique
cards = driver.find_elements(By.CSS_SELECTOR, "calcite-card")
print("Total de cards carregados:", len(cards))
