#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install pandas


# In[1]:


import requests
import json
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

#Login
diretorio_log = r"C:\Users\Gabriel Alef\Projeto\dados"
os.makedirs(diretorio_log, exist_ok=True)
log_arquivo = os.path.join(diretorio_log, "processo_gestta.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_arquivo, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


diretorio_adicional = r"C:\Users\Gabriel Alef\Dropbox\APRL Contabilidade\1. Contábil\4. Operacional e Processos\11. Gestão\PBI - Operação"
os.makedirs(diretorio_adicional, exist_ok=True)

#Slack
def enviar_mensagem_slack(mensagem):
    webhook_url = "https://hooks.slack.com/services/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"
    slack_data = {
        "text": mensagem
    }
    try:
        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            logging.error(f"Erro ao enviar mensagem para Slack: {response.status_code}, {response.text}")
    except Exception as e:
        logging.error(f"Erro ao tentar enviar mensagem para Slack: {e}")


url = "https://api.gestta.com.br/core/customer/task/report"
headers = {
    "authorization": "JWT eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "Content-Type": "application/json"
}

hoje = datetime.now()
seis_meses_atras = hoje - relativedelta(months=6)
fuso = "-03:00"

start_date = seis_meses_atras.strftime(f"%Y-%m-%dT00:00:00{fuso}")
end_date = hoje.strftime(f"%Y-%m-%dT23:59:59{fuso}")

payload = {
    "type": "CUSTOMER_TASK",
    "filter": "CURRENT_MONTH",
    "dates": {
        "startDate": start_date,
        "endDate": end_date
    }
}

#Exportação arquivos
json_arquivo = os.path.join(diretorio_log, "gestta_relatorios.json")
csv_arquivo = os.path.join(diretorio_log, "gestta_relatorios.csv")
json_arquivo_adicional = os.path.join(diretorio_adicional, "gestta_relatorios.json")
csv_arquivo_adicional = os.path.join(diretorio_adicional, "gestta_relatorios.csv")

try:
    logging.info("Iniciando requisição para a API Gestta...")
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    data = response.json()


    with open(json_arquivo, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"JSON salvo em: {json_arquivo}")


    with open(json_arquivo_adicional, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"JSON também salvo em: {json_arquivo_adicional}")

    if isinstance(data, list):
        df = pd.json_normalize(data)
    else:
        df = pd.json_normalize(data.get("data", data))


    df.to_csv(csv_arquivo, index=False, encoding="utf-8-sig")
    logging.info(f"CSV salvo em: {csv_arquivo}")


    df.to_csv(csv_arquivo_adicional, index=False, encoding="utf-8-sig")
    logging.info(f"CSV também salvo em: {csv_arquivo_adicional}")

except requests.exceptions.RequestException as err:
    msg = f"Erro na requisição: {err}"
    logging.error(msg)
    enviar_mensagem_slack(f":x: ERRO no script Gestta: {err}")

except Exception as e:
    msg = f"Erro ao processar/salvar: {e}"
    logging.error(msg)
    enviar_mensagem_slack(f":x: ERRO no processamento do script Gestta: {e}")


# In[3]:


import subprocess

subprocess.run(['python', r'C:\Users\Gabriel Alef\Projeto\Script\PBI_OS.py'])


# In[3]:


import subprocess

subprocess.run(['python', r'C:\Users\Gabriel Alef\Projeto\Script\RPA\RPA_OS.py'])


# In[ ]:




