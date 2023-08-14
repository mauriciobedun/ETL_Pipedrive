# import requests
# import os
# from dotenv import load_dotenv
# import pyodbc
# import openpyxl
# import pandas as pd

# # Load variables from .env file into environment
# load_dotenv()

# # Get environment variables
# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# # Server = os.environ.get("Server")
# # database = os.environ.get("database")
# # UID = os.environ.get('UID')
# # PWD = os.environ.get('PWD')

# # # Conexão com o Banco de Dados
# # conn = pyodbc.connect(
# #     Driver='{SQL Server Native Client 11.0}',
# #     Server=Server,
# #     database=database,
# #     UID=UID,
# #     PWD=PWD
# # )
# # cursor = conn.cursor()

# # Criar um novo arquivo Excel
# wb = openpyxl.Workbook()
# ws = wb.active

# # Adicionar cabeçalhos às colunas
# columns = [
#     "ID", "Título", "Proprietário", "Valor", "Moeda", "Organização", "Funil",
#     "Pessoa de contato", "Etapa", "Status", "Negócio criado", "Horário da atualização",
#     "Última alteração de etapa", "Data da próxima atividade", "Data da última atividade",
#     "Data de ganho", "Data de perda", "Negócio fechado em", "Motivo da perda",
#     "Visível para", "Total de atividades", "Atividades concluídas", "Atividades para fazer",
#     "Número de mensagens de e-mail", "Data de fechamento esperada", "Criador",
#     "Último e-mail recebido", "Último e-mail enviado", "Probabilidade", "Valor ponderado",
#     "Moeda do valor ponderado", "Quantidade de produtos", "Valor de produtos",
#     "Etiqueta", "Nome do produto", "Operadora Atual", "Aniversário do contrato",
#     "Principais Dores atuais", "Tipo de Produto", "Reunião Agendada por:", "Origem da Oportunidade",
#     "Data de Agendamento (Obrigatório SDR)"
# ]
# ws.append(columns)

# # Endpoint da API
# endpoint = "deals"
# per_page = 500  # Número de itens por página
# page = 1

# while True:
#     params = {
#         "api_token": api_token,
#         "start": (page - 1) * per_page,
#         "limit": per_page
#     }

#     response = requests.get(f"{base_url}{endpoint}", params=params)

#     if response.status_code == 200:
#         data = response.json()["data"]

#         if not data:
#             # Não há mais itens, sair do loop
#             break

#         for item in data:
#             # Extrair os valores das colunas do JSON
#             row = [
#                 item.get("id", ""), item.get("title", ""),
#                 item.get("user_id", {}).get("name", ""),
#                 item.get("value", ""), item.get("currency", ""),
#                 item.get("org_id", {}).get("name", ""),
#                 item.get("stage_id", ""), item.get("person_id", {}).get("name", ""),
#                 item.get("stage_id", ""), item.get("status", ""),
#                 item.get("add_time", ""), item.get("update_time", ""),
#                 item.get("stage_change_time", ""),
#                 item.get("next_activity_date", ""), item.get("last_activity_date", ""),
#                 item.get("won_time", ""), item.get("lost_time", ""),
#                 item.get("close_time", ""), item.get("lost_reason", ""),
#                 item.get("visible_to", ""), item.get("activities_count", ""),
#                 item.get("done_activities_count", ""), item.get("undone_activities_count", ""),
#                 item.get("email_messages_count", ""), item.get("expected_close_date", ""),
#                 item.get("creator_user_id", {}).get("name", ""),
#                 item.get("last_incoming_mail_time", ""), item.get("last_outgoing_mail_time", ""),
#                 item.get("probability", ""), item.get("weighted_value", ""),
#                 item.get("weighted_value_currency", ""),
#                 item.get("products_count", ""), item.get("products_value", ""),
#                 item.get("label", ""),
#                 item.get("product.name", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("product.provider", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("product.contract_starts", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("product.current_contract_amount", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("product.dominant_problem", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("product.product_type", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
#                 item.get("meeting_booked_by", ""),
#                 item.get("lead_source", ""), item.get("sdr_scheduled_on", "")
#             ]
#             ws.append(row)

#         page += 1
#     else:
#         print("Erro na solicitação:", response.status_code)
#         break

# # Salvar o arquivo Excel
# excel_file_path = "dados_deals.xlsx"
# wb.save(excel_file_path)


#################### Teste 2 ############################

api_token = "4387a4cf87b1ef0e2a8eb6b0793f442f8d5c1df7"
base_url = "https://bencorp3.pipedrive.com/v1/"

import requests
import os
from dotenv import load_dotenv
import pyodbc
import openpyxl
import pandas as pd

# Load variables from .env file into environment
load_dotenv()

# Get environment variables
# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# Server = os.environ.get("Server")
# database = os.environ.get("database")
# UID = os.environ.get('UID')
# PWD = os.environ.get('PWD')

# # Conexão com o Banco de Dados
# conn = pyodbc.connect(
#     Driver='{SQL Server Native Client 11.0}',
#     Server=Server,
#     database=database,
#     UID=UID,
#     PWD=PWD
# )
# cursor = conn.cursor()

# Criar um novo arquivo Excel
wb = openpyxl.Workbook()
ws = wb.active

# Adicionar cabeçalhos às colunas
columns = [
    "ID", "Título", "Proprietário", "Valor", "Moeda", "Organização", "Funil",
    "Pessoa de contato", "Etapa", "Status", "Negócio criado", "Horário da atualização",
    "Última alteração de etapa", "Data da próxima atividade", "Data da última atividade",
    "Data de ganho", "Data de perda", "Negócio fechado em", "Motivo da perda",
    "Visível para", "Total de atividades", "Atividades concluídas", "Atividades para fazer",
    "Número de mensagens de e-mail", "Data de fechamento esperada", "Criador",
    "Último e-mail recebido", "Último e-mail enviado", "Probabilidade", "Valor ponderado",
    "Moeda do valor ponderado", "Quantidade de produtos", "Valor de produtos",
    "Etiqueta", "Nome do produto", "Operadora Atual", "Aniversário do contrato",
    "Principais Dores atuais", "Tipo de Produto", "Reunião Agendada por:", "Origem da Oportunidade",
    "Data de Agendamento (Obrigatório SDR)"
]
ws.append(columns)

# Lista de endpoints a serem consultados
endpoints = [
    "deals", "persons", "organizations", "activities", "users", "files", "products", "notes", "stages", "pipelines", "webhooks"
]

# Função para extrair informações de uma endpoint
def extract_data(endpoint):
    response = requests.get(f"{base_url}/{endpoint}", params={"api_token": api_token})
    if response.status_code == 200:
        return response.json()["data"]
    return []

for endpoint in endpoints:
    data = extract_data(endpoint)
    for item in data:
        person_id_data = item.get("person_id", {})
        person_name = person_id_data.get("name", "")
        
        row = [
                item.get("id", ""), item.get("title", ""), item.get("user_id", {}).get("name", ""),
                item.get("value", ""), item.get("currency", ""), item.get("org_id", {}).get("name", ""),
                item.get("stage_id", ""), 
                item.get("person_id", {}).get("name", "") if "person_id" in item else "",
                item.get("stage_id", ""), item.get("status", ""), item.get("add_time", ""),
                item.get("add_time", ""), item.get("update_time", ""),
                item.get("stage_change_time", ""),
                item.get("next_activity_date", ""), item.get("last_activity_date", ""),
                item.get("won_time", ""), item.get("lost_time", ""),
                item.get("close_time", ""), item.get("lost_reason", ""),
                item.get("visible_to", ""), item.get("activities_count", ""),
                item.get("done_activities_count", ""), item.get("undone_activities_count", ""),
                item.get("email_messages_count", ""), item.get("expected_close_date", ""),
                item.get("creator_user_id", {}).get("name", ""),
                item.get("last_incoming_mail_time", ""), item.get("last_outgoing_mail_time", ""),
                item.get("probability", ""), item.get("weighted_value", ""),
                item.get("weighted_value_currency", ""),
                item.get("products_count", ""), item.get("products_value", ""),
                item.get("label", ""),
                item.get("product.name", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("product.provider", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("product.contract_starts", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("product.current_contract_amount", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("product.dominant_problem", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("product.product_type", ""),  # Corrigir essa linha de acordo com os dados reais no JSON
                item.get("meeting_booked_by", ""),
                item.get("lead_source", ""), item.get("sdr_scheduled_on", "")
            ]
ws.append(row)

# Salvar o arquivo Excel
excel_file_path = "dados_combinados.xlsx"
wb.save(excel_file_path)


