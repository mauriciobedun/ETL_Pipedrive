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
        item.get("formatted_value"),
        item.get("weighted_value"),
        item.get("formatted_weighted_value"),
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

# Função para carregar os dados no banco de dados


def load_data(cursor, values):
    insert_query = f"INSERT INTO Deals_raw_homolog VALUES ({', '.join(['?' for _ in range(len(values))])})"
    try:
        cursor.execute(insert_query, values)
        cursor.commit()
        print(f"Deal inserido: ID = {values[0]}")
    except Exception as e:
        print(f"Erro ao inserir o deal {values[0]}: {str(e)}")


def main():
    conn = connect_to_database()
    cursor = conn.cursor()
    endpoint = "deals"
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