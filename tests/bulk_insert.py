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

def transform_data(item):
    # Transformações e formatações aqui
    values = [
        item.get("id"),                                                                                                                     # id
        item.get("title"),                                                                                                                  # título
        item.get("owner_name"),                                                                                                             # nome_do_proprietário
        item.get("value"),                                                                                                                  # valor
        item.get("formatted_value"),                                                                                                        # valor_formatado
        item.get("currency"),                                                                                                               # moeda
        item.get("org_name"),                                                                                                               # nome_da_organização
        item.get("pipeline_id"),                                                                                                            # ID_do_pipeline
        item.get("person_name"),                                                                                                            # nome_da_pessoa
        item.get("stage_id"),                                                                                                               # ID_da_etapa
        item.get("status"),                                                                                                                 # status
        datetime.strptime(item["add_time"], "%Y-%m-%d %H:%M:%S") if item.get("add_time") else None,                                         # data_de_inserção
        datetime.strptime(item["update_time"], "%Y-%m-%d %H:%M:%S") if item.get("update_time") else None,                                   # data_de_atualização
        datetime.strptime(item["stage_change_time"], "%Y-%m-%d %H:%M:%S") if item.get("stage_change_time") else None,                       # data_da_mudança_de_etapa
        datetime.strptime(item["next_activity_date"],"%Y-%m-%d") if item.get("next_activity_date") else None,                               # próxima_data_de_atividade
        datetime.strptime(item["last_activity_date"],"%Y-%m-%d") if item.get("last_activity_date") else None,                               # última_data_de_atividade
        item.get("won_time"),                                                                                                               # data_de_ganho
        item.get("lost_time"),                                                                                                              # data_de_perda
        item.get("close_time"),                                                                                                             # data_de_fechamento
        item.get("lost_reason"),                                                                                                            # motivo_da_perda
        item.get("visible_to"),                                                                                                             # visível_para
        item.get("activities_count"),                                                                                                       # contagem_de_atividades
        item.get("done_activities_count"),                                                                                                  # contagem_de_atividades_concluídas
        item.get("undone_activities_count"),                                                                                                # contagem_de_atividades_não_concluídas
        item.get("email_messages_count"),                                                                                                   # contagem_de_mensagens_de_e-mail
        datetime.strptime(item["expected_close_date"],"%Y-%m-%d") if item.get("expected_close_date") else None,                             # data_de_fechamento_esperada
        item.get("creator_user_id", {}).get("name"),                                                                                        # ID_do_usuário_criador
        datetime.strptime(item["last_incoming_mail_time"],"%Y-%m-%d %H:%M:%S") if item.get("last_incoming_mail_time") else None,            # última_hora_de_e-mail_recebido
        datetime.strptime(item["last_outgoing_mail_time"],"%Y-%m-%d %H:%M:%S") if item.get("last_outgoing_mail_time") else None,            # última_hora_de_e-mail_enviado
        item.get("probability"),                                                                                                            # probabilidade
        item.get("formatted_weighted_value"),                                                                                               # valor_ponderado_formatado
        item.get("weighted_value_currency"),                                                                                                # moeda_do_valor_ponderado
        item.get("products_count")                                                                                                          # contagem_de_produtos
    ]
    return values

# Função para truncar a tabela
def truncate_table(cursor):
    try:
        cursor.execute("TRUNCATE TABLE Deals_raw")
        cursor.commit()
        print("Tabela truncada com sucesso.")
    except Exception as e:
        print(f"Erro ao truncar a tabela: {str(e)}")

# Função para inserir dados em lote
def bulk_insert(cursor, values):
    try:
        insert_query = f"INSERT INTO Deals_raw VALUES ({', '.join(['?' for _ in range(len(values[0]))])})"
        print(f"Inserindo lote de {len(values)} registros.")
        print(f"Consulta SQL: {insert_query}")  # Adicionamos esta linha para depuração
        cursor.executemany(insert_query, values)
        cursor.commit()
        print(f"{len(values)} registros inseridos em lote com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir em lote: {str(e)}")

def main():
    conn = connect_to_database()
    cursor = conn.cursor()
    endpoint = "deals"
    page = 1
    per_page = 500

    truncate_table(cursor)  # Truncar a tabela antes da inserção

    while True:
        params = {
            "api_token": api_token,
            "start": (page - 1) * per_page,
            "limit": per_page
        }

        items = extract_data(endpoint, params)

        if not items:
            break

        values_list = [transform_data(item) for item in items]
        bulk_insert(cursor, values_list)

        page += 1

    conn.close()

if __name__ == "__main__":
    main()
