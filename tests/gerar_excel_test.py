import requests
import os
from dotenv import load_dotenv
import pyodbc
import openpyxl
import pandas as pd

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

# Criar um novo arquivo Excel
wb = openpyxl.Workbook()
ws = wb.active

# Adicionar cabeçalhos às colunas
ws.append(["DealID", "CreatorUserID", "PersonID", "PersonEmail", "PersonPhone", "OrgID", "Title", "Value"])

endpoint = "deals"
per_page = 500  # Número de itens por página
page = 1
data_list = []

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
        ## está correta esses itens 
        # deal_id = item["id"]
        # creator_user = item.get("creator_user_id", {}).get("name", "")
    
        # person = item.get("name", {}).get("name","")
        # person_id = person.get("value", "") if person else ""
        # person_email = person.get("email", [{}])[0].get("value", "") if person else ""
        # person_phone = person.get("phone", [{}])[0].get("value", "") if person else ""

        # org = item.get("org_id", {})
        # title = item.get("title", "")
        # value = item.get("value", "")


        deal_id = item["id"]
        creator_user = item.get("creator_user_id", {}).get("id", "")

        person = item.get("person_id", {})
        person_active_flag = person.get("active_flag", False) if person else False
        person_name = person.get("name", "") if person else ""
        person_email = person.get("email", [{}])[0].get("value", "") if person else ""
        person_phone = person.get("phone", [{}])[0].get("value", "") if person else ""
        person_owner_id = person.get("owner_id", "") if person else ""
        person_value = person.get("value", "") if person else ""

        org = item.get("org_id", {})
        org_name = org.get("name", "") if org else ""
        org_people_count = org.get("people_count", 0) if org else 0
        org_owner_id = org.get("owner_id", "") if org else ""
        org_address = org.get("address", "") if org else ""
        org_active_flag = org.get("active_flag", False) if org else False
        org_cc_email = org.get("cc_email", "") if org else ""
        org_owner_name = org.get("owner_name", "") if org else ""
        org_value = org.get("value", "") if org else ""

        stage_id = item.get("stage_id", "")
        title = item.get("title", "")
        value = item.get("value", "")
        currency = item.get("currency", "")
        add_time = item.get("add_time", "")
        update_time = item.get("update_time", "")


        data_list.append([
            deal_id,
            creator_user,
            person_active_flag,
            person_name,
            person_email,
            person_phone,
            person_owner_id,
            person_value,
            org_name,
            org_people_count,
            org_owner_id,
            org_address,
            org_active_flag,
            org_cc_email,
            org_owner_name,
            org_value,
            stage_id,
            title,
            value,
            currency,
            add_time,
            update_time
        ])

        # data_list.append([deal_id, creator_user, person_id, person_email, person_phone, org, title, value])

        page += 1
    else:
        print("Erro na solicitação:", response.status_code)
        break

# Fechar a conexão
conn.close()

# Criar um DataFrame pandas com os dados
columns = [
    "DealID",
    "CreatorUserID",
    "PersonActiveFlag",
    "PersonName",
    "PersonEmail",
    "PersonPhone",
    "PersonOwnerID",
    "PersonValue",
    "OrgName",
    "OrgPeopleCount",
    "OrgOwnerID",
    "OrgAddress",
    "OrgActiveFlag",
    "OrgCCEmail",
    "OrgOwnerName",
    "OrgValue",
    "StageID",
    "Title",
    "Value",
    "Currency",
    "AddTime",
    "UpdateTime"
]
df = pd.DataFrame(data_list, columns=columns)

# Salvar o DataFrame em um arquivo Excel
excel_file_path = "dados_deals.xlsx"
df.to_excel(excel_file_path, index=False)


