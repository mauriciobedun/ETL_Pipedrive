import requests
import os
from dotenv import load_dotenv
import pyodbc
from datetime import datetime

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Obter variáveis de ambiente
base_url = os.environ.get("base_url")
api_token = os.environ.get("api_token")
Server = os.environ.get("Server")
database = os.environ.get("database")
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')

# Função para estabelecer a conexão com o banco de dados

def connect_to_database():
    return pyodbc.connect(
        Driver='{SQL Server Native Client 11.0}',
        Server=Server,
        database=database,
        UID=UID,
        PWD=PWD
    )

# Função para extrair dados da API

def extract_data(endpoint, params):
    response = requests.get(f"{base_url}{endpoint}", params=params)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        print("Erro na solicitação:", response.status_code)
        return []

# Função para transformar os dados

def transform_data(item):
    # Transformações e formatações aqui
    values = [
        item.get("id"),                                                                                         # ID
        item.get("order_nr"),                                                                                   # Número_de_Pedido
        item.get("name"),                                                                                       # Nome_Etapa
        item.get("active_flag"),                                                                                # Flag_Ativo
        item.get("deal_probability"),                                                                           # Probabilidade_Negócio
        item.get("pipeline_id"),                                                                                # ID_Pipeline
        item.get("rotten_flag"),                                                                                # Flag_Rotten
        item.get("rotten_days"),                                                                                # Dias_Rotten
        datetime.strptime(item["add_time"], "%Y-%m-%d %H:%M:%S") if item.get("add_time") else None,             # Data_de_Adição_Etapa
        datetime.strptime(item["update_time"], "%Y-%m-%d %H:%M:%S") if item.get("update_time") else None,       # Data_de_Atualização_Etapa
        item.get("pipeline_name"),                                                                              # Nome_do_Pipeline
        item.get("pipeline_deal_probability"),                                                                  # Probabilidade_do_Negócio_do_Pipeline
    ]
    return values

# Função para carregar os dados no banco de dados

def load_data(cursor, values):
    insert_query = f"INSERT INTO Stages_raw VALUES ({', '.join(['?' for _ in range(len(values))])})"
    try:
        cursor.execute(insert_query, values)
        cursor.commit()
        print(f"Stage inserido: ID = {values[0]}")
    except Exception as e:
        print(f"Erro ao inserir o stage {values[0]}: {str(e)}")


def main():
    conn = connect_to_database()
    cursor = conn.cursor()
    endpoint = "stages"
    page = 1
    per_page = 500

    while True:
        params = {
            "api_token": api_token,
            "start": (page - 1) * per_page,
            "limit": per_page
        }

        items = extract_data(endpoint, params)

        if not items:
            break

        for item in items:
            values = transform_data(item)
            load_data(cursor, values)

        page += 1

    conn.close()


if __name__ == "__main__":
    main()
