

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

# Função para formatar valores monetários
def format_currency_value(value):
    if isinstance(value, str):
        # Remove todos os caracteres que não são dígitos ou ponto decimal
        cleaned_value = ''.join(c for c in value if c.isdigit() or c == '.')
    
        # Se o valor for vazio após a limpeza, retorna None
        if not cleaned_value:
            return None
    
        # Converte o valor para float
        return float(cleaned_value)
    else:
        return value

def transform_data(item):
    # Transformações e formatações aqui
    formatted_value = format_currency_value(item.get("formatted_value"))
    weighted_value = format_currency_value(item.get("weighted_value"))
    formatted_weighted_value = format_currency_value(item.get("formatted_weighted_value"))
    
    values = [
        item.get("id"),
        item.get("creator_user_id", {}).get("id"),
        item.get("user_id", {}).get("id"),
        item.get("person_id", {}).get(
            "value") if item.get("person_id") else None,
        item.get("org_id", {}).get("value") if item.get("org_id") else None,
        item.get("stage_id"),
        item.get("title"),
        item.get("value"),
        item.get("currency"),
        datetime.strptime(
            item["add_time"], "%Y-%m-%d %H:%M:%S") if item.get("add_time") else None,  # ok
        datetime.strptime(
            item["update_time"], "%Y-%m-%d %H:%M:%S") if item.get("update_time") else None,  # ok
        datetime.strptime(item["stage_change_time"], "%Y-%m-%d %H:%M:%S") if item.get(
            "stage_change_time") else None,  # ok
        item.get("active"),
        item.get("deleted"),
        item.get("status"),
        item.get("probability"),
        datetime.strptime(item["next_activity_date"],
                          "%Y-%m-%d") if item.get("next_activity_date") else None,  # ok
        item.get("next_activity_time"),
        item.get("next_activity_id"),
        item.get("last_activity_id"),
        datetime.strptime(item["last_activity_date"],
                          "%Y-%m-%d") if item.get("last_activity_date") else None,  # ok
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
        datetime.strptime(item["expected_close_date"],
                          "%Y-%m-%d") if item.get("expected_close_date") else None,  # ok
        datetime.strptime(item["last_incoming_mail_time"],
                          "%Y-%m-%d %H:%M:%S") if item.get("last_incoming_mail_time") else None,
        datetime.strptime(item["last_outgoing_mail_time"],
                          "%Y-%m-%d %H:%M:%S") if item.get("last_outgoing_mail_time") else None,
        item.get("label"),
        item.get("stage_order_nr"),
        item.get("person_name"),
        item.get("org_name"),
        item.get("next_activity_subject"),
        item.get("next_activity_type"),
        item.get("next_activity_duration"),
        item.get("next_activity_note"),
        formatted_value ,
        weighted_value ,
        formatted_weighted_value ,
        item.get("weighted_value_currency"),
        datetime.strptime(
            item["rotten_time"], "%Y-%m-%d %H:%M:%S") if item.get("rotten_time") else None,  # Ok
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
    return values

def load_data(cursor, values, column_names):
    insert_query = f"INSERT INTO Deals_raw_test ({', '.join(column_names)}) VALUES ({', '.join(['?' for _ in range(len(values))])})"
    try:
        cursor.execute(insert_query, values)
        cursor.commit()
        print(f"Deal inserido: ID = {values[0]}")
    except Exception as e:
        print(f"Erro ao inserir o deal {values[0]}: {str(e)}")
        print("Colunas:", column_names)

def main():
    conn = connect_to_database()
    cursor = conn.cursor()
    endpoint = "deals"
    page = 1
    per_page = 500

    # Obter as colunas da tabela Deals_raw_test do SQL Server
    columns_query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Deals_raw_test'"
    cursor.execute(columns_query)
    column_rows = cursor.fetchall()
    column_names = [row[0] for row in column_rows]

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

            # Obtém os detalhes dos produtos associados ao deal
            deal_id = item.get("id")
            products_endpoint = f"deals/{deal_id}/products"
            products = extract_data(products_endpoint, {"api_token": api_token})

            if products:
                for product in products:
                    # Filtra as colunas mais importantes dos produtos
                    product_values = [
                        product.get("ProductID"),            # ID do produto
                        product.get("ProductName"),          # Nome do produto
                        product.get("ItemPrice"),           # Preço do produto
                        product.get("Quantity"),            # 
                        product.get("DiscountPercentage"),          # 
                        product.get("Sum"),           # 
                        product.get("QuantityFormatted"),            # 
                        product.get("SumFormatted"),          # 
                        product.get("TaxMethod"),           # 
                        product.get("Discount"),          # 

                        # ... Adicione outras colunas importantes dos produtos ...
                    ]
                    # Concatena os valores do produto aos valores do negócio (deal)
                    combined_values = values + product_values
                    load_data(cursor, combined_values, column_names)
            else:
                load_data(cursor, values, column_names)

    conn.close()

if __name__ == "__main__":
    main()








# def main():
#     conn = connect_to_database()
#     cursor = conn.cursor()
#     endpoint = "deals"
#     page = 1
#     per_page = 500

#     while True:
#         params = {
#             "api_token": api_token,
#             "start": (page - 1) * per_page,
#             "limit": per_page
#         }

#         items = extract_data(endpoint, params)

#         if not items:
#             break

#         for item in items:
#             values = transform_data(item)
#             load_data(cursor, values)

#             # Obtém os detalhes dos produtos associados ao deal
#             deal_id = item.get("id")
#             products_endpoint = f"deals/{deal_id}/products"
#             products = extract_data(products_endpoint, {"api_token": api_token})

#             if products:
#                 for product in products:
#                     product_id = product.get("product_id")  # Substitua pelo campo correto que contém o ID do produto
#                     product_values = [
#                         product_id,
#                         deal_id,
#                         product.get("name"),
#                         product.get("item_price"),
#                         product.get("quantity"),
#                         product.get("discount_percentage"),
#                         product.get("sum"),
#                         product.get("currency"),
#                         product.get("active_flag"),
#                         product.get("enabled_flag"),
#                         datetime.strptime(product.get("add_time"), "%Y-%m-%d %H:%M:%S") if product.get("add_time") else None,
#                         datetime.strptime(product.get("last_edit"), "%Y-%m-%d %H:%M:%S") if product.get("last_edit") else None,
#                         product.get("comments"),
#                         product.get("tax"),
#                         product.get("quantity_formatted"),
#                         product.get("sum_formatted"),
#                         product.get("tax_method"),
#                         product.get("discount"),
#                         product.get("discount_type")
#                     ]
#                     load_product_data(cursor, product_values)

#     conn.close()

# if __name__ == "__main__":
#     main()

# ... (código anterior)



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
#     "leads", "deals", "persons", "organizations", "activities", "users", "files", "products", "notes", "stages", "pipelines", "webhooks"
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
