import os
import requests
from dotenv import load_dotenv
import pyodbc

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
#             deal_id = item.get("id")
#             products_endpoint = f"deals/{deal_id}/products"
#             products = extract_data(products_endpoint, {"api_token": api_token})

#             if products:
#                 for product in products:
#                     product_id = product.get("id")
#                     product_values = [
#                         deal_id,
#                         #product.get("product_id"),
#                         product_id,
#                         #product.get("ProductCode"),
#                         product.get("name"),
#                         product.get("item_price"),
#                         product.get("TotalPrice"),
#                         product.get("quantity"),
#                         product.get("discount_percentage")
#                     ]

#                     # Imprimir os valores que serão inseridos
#                     print("Valores a serem inseridos:", product_values)

#                     insert_query = "INSERT INTO Products_Deals_raw (DealID, ProductID, ProductName, ItemPrice, TotalPrice, Quantity, DiscountPercentage) VALUES (?, ?, ?, ?, ?, ?, ?)"
#                     try:
#                         cursor.execute(insert_query, product_values)
#                         cursor.commit()
#                         print(f"Produto inserido: ID = {deal_id}, Nome = {product.get('name')}")
#                     except Exception as e:
#                         print(f"Erro ao inserir o produto {product_id} (DealID = {deal_id}): {str(e)}")

#         page += 1

#     conn.close()

# if __name__ == "__main__":
#     main()

# ... (código anterior)

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
            deal_id = item.get("id")
            products_endpoint = f"deals/{deal_id}/products"
            products = extract_data(products_endpoint, {"api_token": api_token})

            if products:
                for product in products:
                    product_id = product.get("id")
                    product_values = [
                        deal_id,
                        product_id,
                        product.get("name"),
                        product.get("item_price"),
                        #product.get("TotalPrice"),
                        product.get("sum_formatted"),
                        product.get("quantity"),
                        product.get("discount_percentage")
                    ]
                    insert_query = "INSERT INTO Products_Deals_raw (DealID, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage) VALUES (?, ?, ?, ?, ?, ?, ?)"
                    try:
                        cursor.execute(insert_query, product_values)
                        cursor.commit()
                        print(f"Produto inserido: DealID = {deal_id}, ProductID = {product_id}")
                    except Exception as e:
                        print(f"Erro ao inserir o produto {product_id} do DealID {deal_id}: {str(e)}")

        page += 1

    conn.close()

if __name__ == "__main__":
    main()

