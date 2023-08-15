import requests
import os
from dotenv import load_dotenv
import pyodbc
from datetime import datetime

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

            # Extrair e formatar os valores
            values = [
                item.get("id"),
                item.get("creator_user_id", {}).get("id"),
                item.get("user_id", {}).get("id"),
                item.get("person_id", {}).get("value") if item.get("person_id") else None,
                item.get("org_id", {}).get("value") if item.get("org_id") else None,
                item.get("stage_id"),
                item.get("title"),
                item.get("value"),
                item.get("currency"),
                datetime.strptime(item["add_time"], "%Y-%m-%d %H:%M:%S") if item.get("add_time") else None, # ok
                datetime.strptime(item["update_time"], "%Y-%m-%d %H:%M:%S") if item.get("update_time") else None, # ok
                datetime.strptime(item["stage_change_time"], "%Y-%m-%d %H:%M:%S") if item.get("stage_change_time") else None, # ok
                item.get("active"),
                item.get("deleted"),
                item.get("status"),
                item.get("probability"),
                datetime.strptime(item["next_activity_date"], "%Y-%m-%d") if item.get("next_activity_date") else None, # ok
                item.get("next_activity_time"),
                item.get("next_activity_id"),
                item.get("last_activity_id"),
                datetime.strptime(item["last_activity_date"], "%Y-%m-%d") if item.get("last_activity_date") else None, # ok
                item.get("lost_reason"),
                item.get("visible_to"),
                item.get("close_time"),
                item.get("pipeline_id"),
                item.get("won_time"),
                item.get("first_won_time"),
                item.get("lost_time"),
                item.get("products_count"),
                item.get("files_count"),
                item.get("notes_count"),
                item.get("followers_count"),
                item.get("email_messages_count"),
                item.get("activities_count"),
                item.get("done_activities_count"),
                item.get("undone_activities_count"),
                item.get("participants_count"),
                datetime.strptime(item["expected_close_date"], "%Y-%m-%d") if item.get("expected_close_date") else None, # ok
                datetime.strptime(item["last_incoming_mail_time"], "%Y-%m-%d %H:%M:%S") if item.get("last_incoming_mail_time") else None,
                datetime.strptime(item["last_outgoing_mail_time"], "%Y-%m-%d %H:%M:%S") if item.get("last_outgoing_mail_time") else None,
                item.get("label"),
                item.get("stage_order_nr"),
                item.get("person_name"),
                item.get("org_name"),
                item.get("next_activity_subject"),
                item.get("next_activity_type"),
                item.get("next_activity_duration"),
                item.get("next_activity_note"),
                item.get("formatted_value"),
                item.get("weighted_value"),
                item.get("formatted_weighted_value"),
                item.get("weighted_value_currency"),
                datetime.strptime(item["rotten_time"], "%Y-%m-%d %H:%M:%S") if item.get("rotten_time") else None, # Ok
                item.get("owner_name"),
                item.get("cc_email"),
                item.get("0c8a1f5f74996e880ffac2e4224930a68151d7c1"),
                item.get("cd9bbdc71791405f483fb504e748a842623e6cbf"),
                item.get("644408ff0c7bb12a577f7393113279bc7827793a"),
                item.get("ede369fcd43ee9b1d8d81547c7f41d6be961fe7c"),
                item.get("3973aef6f88f6fd2ff105f446e789a5ac2eb227a"),
                item.get("bbb013363135615beb9207a9effadc02cb7ee3b9"),
                item.get("5982f8f86c637388ba4b33a74fbd2d2cb5750fcd"),
                item.get("org_hidden"),
                item.get("person_hidden")
            ]

            # Antes de inserir, imprima os valores para depuração
            #print("Valores a serem inseridos:", values)


            # Preparar a consulta SQL
            insert_query = f"INSERT INTO Deals VALUES ({', '.join(['?' for _ in range(len(values))])})"

            try:
                # Executar a consulta SQL
                cursor.execute(insert_query, values)

                # Confirmar as alterações
                conn.commit()

                print(f"Deal inserido: ID = {item.get('id')}")
            except Exception as e:
                print(f"Erro ao inserir o deal {item.get('id')}: {str(e)}")

        page += 1  # Incrementar a página
    else:
        print("Erro na solicitação:", response.status_code)
        break

# Fechar a conexão
conn.close()


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

# # Lista de endpoints a serem consultados
# endpoints = [
#     "deals", "persons", "organizations", "activities", "users", "files", "products", "notes", "stages", "pipelines", "webhooks"
# ]

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
