import requests
import os
from dotenv import load_dotenv
import pyodbc

# Load variables from .env file into environment
load_dotenv()

# Get environment variables
base_url = os.environ.get("base_url")
api_token = os.environ.get("api_token")
Server = os.environ.get("Server")
database = os.environ.get("database")
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')

# Conexão com o Banco de Dados
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
        data = response.json()["data"]

        if not data:
            # Não há mais itens, sair do loop
            break

        for item in data:
            deal_id = item["id"]
            
            # Inserir nas tabelas relacionadas
            creator_user = item["creator_user_id"]
            cursor.execute("INSERT INTO Users (UserID, Name, Email, ...) VALUES (?, ?, ?, ...)", creator_user["id"], creator_user["name"], creator_user["email"], ...)

            person = item["person_id"]
            cursor.execute("INSERT INTO People (PersonID, Name, OwnerID, ...) VALUES (?, ?, ?, ...)", person["value"], person["name"], person["owner_id"], ...)

            org = item["org_id"]
            cursor.execute("INSERT INTO Organizations (OrgID, Name, PeopleCount, OwnerID, ...) VALUES (?, ?, ?, ?, ...)", org["value"], org["name"], org["people_count"], org["owner_id"], ...)

            # Inserir o restante das informações na tabela Deals
            cursor.execute("INSERT INTO Deals (DealID, CreatorUserID, PersonID, OrgID, ...) VALUES (?, ?, ?, ?, ...)", deal_id, creator_user["id"], person["value"], org["value"], ...)

            # Inserir números de telefone na tabela PhoneNumbers
            for phone in person.get("phone", []):
                cursor.execute("INSERT INTO PhoneNumbers (PersonID, Label, Value, IsPrimary) VALUES (?, ?, ?, ?)", person["value"], phone["label"], phone["value"], phone["primary"])

        conn.commit()

        page += 1
    else:
        print("Erro na solicitação:", response.status_code)
        break

# Fechar a conexão
conn.close()


## teste com funções

# import requests
# import os
# from dotenv import load_dotenv
# import pyodbc

# # Load environment variables
# load_dotenv()

# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# Server = os.environ.get("Server")
# database = os.environ.get("database")
# UID = os.environ.get('UID')
# PWD = os.environ.get('PWD')

# def main():
#     # Connect to the database
#     with pyodbc.connect(
#         Driver='{SQL Server Native Client 11.0}',
#         Server=Server,
#         database=database,
#         UID=UID,
#         PWD=PWD
#     ) as conn:
#         cursor = conn.cursor()

#         endpoint = "deals"
#         page = 1
#         per_page = 500
#         total_items = None

#         while total_items is None or (page - 1) * per_page < total_items:
#             params = {
#                 "api_token": api_token,
#                 "start": (page - 1) * per_page,
#                 "limit": per_page
#             }

#             response = requests.get(f"{base_url}{endpoint}", params=params)
#             status_code = response.status_code

#             if status_code == 200:
#                 data = response.json()
#                 total_items = data["total"]
#                 items = data["data"]

#                 if not items:
#                     break

#                 batch_values = [(item["id"], item["title"], item["value"]) for item in items]
#                 insert_query = "INSERT INTO [SDG_CRM].[dbo].[deals] (id, title, value) VALUES (?, ?, ?)"
#                 cursor.executemany(insert_query, batch_values)
#                 conn.commit()

#                 for deal_id, deal_title, deal_value in batch_values:
#                     print(f"Deal inserido: ID = {deal_id}, Título = {deal_title}, Valor = {deal_value}")
                
#                 page += 1
#             else:
#                 print("Erro na solicitação:", status_code)
#                 break

# if _name_ == "_main_":
#     main()
