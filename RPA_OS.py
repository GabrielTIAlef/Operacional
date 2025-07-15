#!/usr/bin/env python
# coding: utf-8

# In[6]:


# TESTE DE USUARIO
#from selenium import webdriver
#from selenium.webdriver.chrome.service import Service
#import time
#import os

# Caminho para o ChromeDriver correto
#chrome_driver_path = r"C:\WebDriver\chromedriver.exe"


#user_data_dir = r"C:\Selenium\ChromeProfile"  # <-- criado por você

# Configurações do navegador
#options = webdriver.ChromeOptions()
#options.add_argument(f"--user-data-dir={user_data_dir}")  # usa esse perfil
#options.add_argument("--disable-blink-features=AutomationControlled")
#options.add_argument("--start-maximized")

# Desativa aviso de automação
#options.add_experimental_option("excludeSwitches", ["enable-automation"])
#options.add_experimental_option("useAutomationExtension", False)

# Inicia o ChromeDriver corretamente
#service = Service(chrome_driver_path)
#driver = webdriver.Chrome(service=service, options=options)


#driver.get("https://app.powerbi.com/groups/me/list?experience=power-bi")


#time.sleep(60)




# In[1]:


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import requests

#CONFIGURAÇÃO DO SLACK
SLACK_WEBHOOK_URL = "https://hooks.slack.com/..."

def send_slack_alert(message):
    payload = {"text": message}
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload)
    except Exception as err:
        print(f"Erro ao enviar alerta ao Slack: {err}")

#CONFIGURAÇÃO DO SELENIUM
chrome_driver_path = r"C:\WebDriver\chromedriver.exe"
user_data_dir = r"C:\Selenium\ChromeProfile"

options = webdriver.ChromeOptions()
options.add_argument(f"--user-data-dir={user_data_dir}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")
options.add_argument("--headless=new")  # comentar para visualmente
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)

service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=options)

try:
    driver.get("https://app.powerbi.com/groups/me/list?experience=power-bi")
    wait = WebDriverWait(driver, 30)

    dataset_name = "PBI_OS"
    wait.until(EC.presence_of_element_located((By.XPATH, "//a[@data-testid='item-name']")))

    all_dataset_blocks = driver.find_elements(By.XPATH, "//span[contains(@class, 'col-name')]")

    found = False
    for block in all_dataset_blocks:
        try:
            a = block.find_element(By.XPATH, ".//a[@data-testid='item-name']")
            name = a.get_attribute("aria-label").strip()
            if name == dataset_name:
                actions = ActionChains(driver)
                actions.move_to_element(block).perform()

                update_btn = WebDriverWait(block, 5).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, ".//button[@data-testid='quick-action-button-Atualizar agora']")
                    )
                )
                update_btn.click()
                print("✅ Botão 'Atualizar agora' clicado com sucesso!")
                found = True
                break
        except Exception:
            continue

    if not found:
        msg = "❌ Dataset não encontrado ou botão 'Atualizar agora' não disponível."
        print(msg)
        send_slack_alert(msg)

    time.sleep(5)

except Exception as e:
    error_message = f"❌ Erro geral ao rodar RPA no Power BI Service: {e}"
    print(error_message)
    send_slack_alert(error_message)

finally:
    driver.quit()


# In[ ]:




