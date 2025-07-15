#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install pandas sqlalchemy psycopg2-binary openpyxl
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os


# In[2]:


DB_CONFIG = {
    "usuario": "postgres",
    "senha": "123456",
    "host": "localhost",
    "porta": "5432",
    "banco": "ProjetoImport"
}


# In[3]:


def conectar_banco():
    url = f"postgresql+psycopg2://{DB_CONFIG['usuario']}:{DB_CONFIG['senha']}@{DB_CONFIG['host']}:{DB_CONFIG['porta']}/{DB_CONFIG['banco']}"
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            print("✅ Conexão bem-sucedida ao banco de dados.")
        return engine
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        raise


# In[4]:


def apagar_views(engine):
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS bi_final CASCADE"))
        conn.execute(text("DROP VIEW IF EXISTS gestta_completo CASCADE"))
    print("✅ Views apagadas.")


# In[5]:


def limpar_e_carregar_gestta_relatorios(engine):
    df = pd.read_csv(r"C:\Users\Gabriel Alef\Projeto\dados\gestta_relatorios.csv", sep=',')
    colunas_excluir = [
        "company_task.score", "owner.role", "due_iso_week", "value", "concluded_by.role",
        "customer.company_groupers", "customer.state_regime", "customer.municipal_regime", "note",
        "score", "customer.federal_regime", "_forever", "customer.state_regime.name",
        "customer.municipal_regime.name", "customer.monthly_payment", "company.name", "company.status","owner"
    ]
    df = df.drop(columns=colunas_excluir, errors='ignore')
    novo_nome = {
        "company.created_at": "data_criação_completa_empresa",
        "name": "Tarefa - Nome",
        "company_department.name": "Empresa - Departamento",
        "type": "Tarefa - Tipo",
        "subtype": "Tarefa - Subtipo",
        "status": "Tarefa - Status",
        "owner.name": "Tarefa - Responsável",
        "notify_customer": "Tarefa - Notifica Cliente?",
        "fine": "Tarefa - Gera Multa?",
        "_due_date": "tarefa__data_de_vencimento_(completa)",
        "downloaded": "Tarefa - Baixada?",
        "done_overdue": "Tarefa - Concluída em Atraso",
        "done_fine": "Tarefa - Concluída com Multa",
        "created_at": "data_criação_completa",
        "concluded_by.name": "Tarefa - Concluído por",
        "conclusion_date": "Tarefa - Data de conclusão (completa)",
        "id": "Tarefa - ID",
        "overdue": "Tarefa - Atrasada?",
        "on_time": "Tarefa - No prazo?",
        "customer.federal_regime.name": "Cliente - Regime federal",
        "customer.name": "Cliente - Nome",
        "customer.cnpj": "Cliente - CNPJ",
        "customer.active": "Cliente - Ativo?",
        "customer.code": "Cliente - Código",
        "legal_date": "data_legal"
    }
    df = df.rename(columns=novo_nome)
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_').str.replace(r'[.\-?]', '', regex=True)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df['cliente__código'] = pd.to_numeric(df['cliente__código'], errors='coerce')
    df['cliente__cnpj'] = pd.to_numeric(df['cliente__cnpj'], errors='coerce')
    replace_dict = {
        "SERVICE_ORDER" : "Solicitações de serviço",
        "RECURRENT" : "Recorrentes",
        "DISCONSIDERED": "Desconsideradas",
        "DONE": "Concluídas",
        "IMPEDIMENT": "Impedimentos",
        "OPEN": "Abertas",
        "AUTOMATIC": "Recorrentes automáticas",
        "FREE": "Ordens de serviço livres",
        "MANUAL": "Recorrentes manuais",
        "TEMPLATE": "Ordens de serviço padronizadas",
        "WORKFLOW": "Workflows",
         True: "Sim",
         False: "Não"
    }
    df = df.replace(replace_dict)
    
    colunas_data = [
        'data_criação_completa_empresa', 
        'tarefa__data_de_vencimento_(completa)', 
        'data_criação_completa', 
        'tarefa__data_de_conclusão_(completa)', 
        'data_legal'
    ]

    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    df.to_sql('gestta_relatorios', engine, if_exists='replace', index=False)
    print("✅ Tabela gestta_relatorios carregada.")


# In[6]:


def limpar_e_carregar_notion_dados(engine):
    df = pd.read_csv(r"C:\Users\Gabriel Alef\Projeto\dados\base_notion.csv", sep=',', na_values=['Sem dado'], dtype_backend = "numpy_nullable")
    df['Código Domínio'] = pd.to_numeric(df['Código Domínio'], errors = 'coerce')
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    df['gestão_de_clientes'] = df['gestão_de_clientes'].str.split('(').str[0].str.strip()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df.to_sql('notion_dados', engine, if_exists='replace', index=False)
    print("✅ Tabela notion_dados carregada.")


# In[7]:


def criar_views(engine):
    with engine.begin() as conn:        
        conn.execute(text("""
            CREATE OR REPLACE VIEW bi_final AS
            SELECT 
                g.*, 
                n.*
            FROM gestta_relatorios g
            LEFT JOIN notion_dados n 
                ON g.cliente__código = n.código_domínio;
        """))
    print("✅ Views gestta_completo e bi_final criadas.")


# In[8]:


def exportar_excel(engine):
    df = pd.read_sql("SELECT * FROM bi_final", engine)
    df.to_excel(r"C:\Users\Gabriel Alef\Projeto\dados\projeto_bi.xlsx", index=False)
    df.to_excel(r"C:\Users\Gabriel Alef\Dropbox\APRL Contabilidade\1. Contábil\4. Operacional e Processos\11. Gestão\PBI - Operação\projeto_bi.xlsx", index=False)
    print("✅ Exportado para Excel com sucesso.")


# In[9]:


def main():
    engine = conectar_banco()
    apagar_views(engine)
    limpar_e_carregar_gestta_relatorios(engine)
    limpar_e_carregar_notion_dados(engine)
    criar_views(engine)
    exportar_excel(engine)


# In[10]:


if __name__ == "__main__":
    main()


# In[ ]:




