import requests
import os
from dotenv import load_dotenv
import pyodbc

#from tqdm import tqdm  # Importar a classe tqdm

# ========= Load variables from .env file into environment ========= #
load_dotenv()

# ========= Get environment variables environment ========= #
base_url = os.environ.get("base_url")
api_token = os.environ.get("api_token")
Server = os.environ.get("Server")
database = os.environ.get("database")
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')

# ========= Conexão com o Banco de Dados ========= # 
conn = pyodbc.connect(
    Driver='{SQL Server Native Client 11.0}',
    Server=Server,
    database=database,
    UID=UID,
    PWD=PWD
)
cursor = conn.cursor()

endpoint = "deals"  # Substitua "deals" pelo endpoint desejado

page = 1
per_page = 500  # Número de itens por página

while True:
    params = {
        "api_token": api_token,
        "start": (page - 1) * per_page,
        "limit": per_page
    }

    response = requests.get(f"{base_url}{endpoint}", params=params)

    if response.status_code == 200:
        data = response.json()
        items = data["data"]

        if not items:
            # Não há mais itens, sair do loop
            break

        for item in items:
            # Extrair os dados relevantes do item
            deal_id = item["id"]
            deal_title = item["title"]
            deal_value = item["value"]

            try:
                # Preparar a consulta SQL
                insert_query = "INSERT INTO [SDG_CRM].[dbo].[deals] (id, title, value) VALUES (?, ?, ?)"

                # Executar a consulta com parâmetros
                cursor.execute(insert_query, (deal_id, deal_title, deal_value))
                
                # Confirmar as alterações
                conn.commit()

                print(f"Deal inserido: ID = {deal_id}, Título = {deal_title}, Valor = {deal_value}")
            except Exception as e:
                print(f"Erro ao inserir o deal {deal_id}: {str(e)}")

        page += 1
    else:
        print("Erro na solicitação:", response.status_code)
        break

# Fechar a conexão
conn.close()