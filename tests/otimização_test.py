##################################### Funciona mas ele atualiza independente se houve alterações nos campos ########
# import os
# import requests
# from dotenv import load_dotenv
# import pyodbc

# load_dotenv()

# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# Server = os.environ.get("Server")
# database = os.environ.get("database")
# UID = os.environ.get('UID')
# PWD = os.environ.get('PWD')

# def connect_to_database():
#     return pyodbc.connect(
#         Driver='{SQL Server Native Client 11.0}',
#         Server=Server,
#         database=database,
#         UID=UID,
#         PWD=PWD
#     )

# def extract_data(endpoint, params):
#     response = requests.get(f"{base_url}{endpoint}", params=params)
#     if response.status_code == 200:
#         return response.json()["data"]
#     else:
#         print("Erro na solicitação:", response.status_code)
#         return []

# def main():
#     conn = connect_to_database()
#     cursor = conn.cursor()
#     endpoint = "deals"
#     page = 1
#     per_page = 500

#     # Preparar o statement de inserção
#     insert_query = "INSERT INTO Products_Deals_raw (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

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
#                     # Verificar se o registro já existe
#                     existing_record = cursor.execute("SELECT 1 FROM Products_Deals_raw WHERE DealID = ? AND id = ?", deal_id, product_id).fetchone()
#                     if existing_record:
#                         # Atualizar campos relevantes, se necessário
#                         update_query = "UPDATE Products_Deals_raw SET ItemPrice = ?, sum_formatted = ?, Quantity = ?, DiscountPercentage = ? WHERE DealID = ? AND id = ?"
#                         cursor.execute(update_query, product.get("item_price"), product.get("sum_formatted"), product.get("quantity"), product.get("discount_percentage"), deal_id, product_id)
#                         conn.commit()
#                         print(f"Produto atualizado: DealID = {deal_id}, ProductID = {product_id}")
#                     else:
#                         # Inserir novo registro
#                         product_values = [
#                             deal_id,
#                             product.get("id"),
#                             product.get("product_id"),
#                             product.get("name"),
#                             product.get("item_price"),
#                             product.get("sum_formatted"),
#                             product.get("quantity"),
#                             product.get("discount_percentage")
#                         ]
#                         cursor.execute(insert_query, product_values)
#                         conn.commit()
#                         print(f"Produto inserido: DealID = {deal_id}, ProductID = {product_id}")

#         page += 1

#     conn.close()

# if __name__ == "__main__":
#     main()


# import os
# import requests
# from dotenv import load_dotenv
# import pyodbc

# load_dotenv()

# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# Server = os.environ.get("Server")
# database = os.environ.get("database")
# UID = os.environ.get('UID')
# PWD = os.environ.get('PWD')

# def connect_to_database():
#     return pyodbc.connect(
#         Driver='{SQL Server Native Client 11.0}',
#         Server=Server,
#         database=database,
#         UID=UID,
#         PWD=PWD
#     )

# def extract_data(endpoint, params):
#     response = requests.get(f"{base_url}{endpoint}", params=params)
#     if response.status_code == 200:
#         return response.json()["data"]
#     else:
#         print("Erro na solicitação:", response.status_code)
#         return []

# def main():
#     conn = connect_to_database()
#     cursor = conn.cursor()
#     endpoint = "deals"
#     page = 1
#     per_page = 500

#     insert_query = "INSERT INTO Products_Deals_raw (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

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
#                     existing_record = cursor.execute("SELECT * FROM Products_Deals_raw WHERE DealID = ? AND id = ?", deal_id, product_id).fetchone()
#                     if not existing_record:
#                         # Inserir novo registro
#                         product_values = [
#                             deal_id,
#                             product_id,
#                             product.get("product_id"),
#                             product.get("name"),
#                             product.get("item_price"),
#                             product.get("sum_formatted"),
#                             product.get("quantity"),
#                             product.get("discount_percentage")
#                         ]
#                         cursor.execute(insert_query, product_values)
#                         conn.commit()
#                         print(f"Produto inserido: DealID = {deal_id}, ProductID = {product_id}")
#                     else:
#                         # Verificar se há alterações nos campos relevantes
#                         if (existing_record.ItemPrice != product.get("item_price") or
#                             existing_record.sum_formatted != product.get("sum_formatted") or
#                             existing_record.Quantity != product.get("quantity") or
#                             existing_record.DiscountPercentage != product.get("discount_percentage")):
#                             # Atualizar campos relevantes
#                             update_query = "UPDATE Products_Deals_raw SET ItemPrice = ?, sum_formatted = ?, Quantity = ?, DiscountPercentage = ? WHERE DealID = ? AND id = ?"
#                             cursor.execute(update_query, product.get("item_price"), product.get("sum_formatted"), product.get("quantity"), product.get("discount_percentage"), deal_id, product_id)
#                             conn.commit()
#                             print(f"Produto atualizado: DealID = {deal_id}, ProductID = {product_id}")
#                     conn.commit()  # Adicione esta linha fora do loop interno

#         page += 1

#     conn.close()

# if __name__ == "__main__":
#     main()



# import os
# import requests
# from dotenv import load_dotenv
# import pyodbc

# load_dotenv()

# base_url = os.environ.get("base_url")
# api_token = os.environ.get("api_token")
# Server = os.environ.get("Server")
# database = os.environ.get("database")
# UID = os.environ.get('UID')
# PWD = os.environ.get('PWD')

# def connect_to_database():
#     return pyodbc.connect(
#         Driver='{SQL Server Native Client 11.0}',
#         Server=Server,
#         database=database,
#         UID=UID,
#         PWD=PWD
#     )

# def extract_data(endpoint, params):
#     response = requests.get(f"{base_url}{endpoint}", params=params)
#     if response.status_code == 200:
#         return response.json()["data"]
#     else:
#         print("Erro na solicitação:", response.status_code)
#         return []

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
#                         product_id,
#                         product.get("product_id"),
#                         product.get("name"),
#                         product.get("item_price"),
#                         product.get("sum_formatted"),
#                         product.get("quantity"),
#                         product.get("discount_percentage")
#                     ]
#                     merge_query = '''
#                         MERGE INTO Products_Deals_raw AS target
#                         USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage)
#                         ON target.DealID = source.DealID AND target.id = source.id
#                         WHEN MATCHED THEN
#                             UPDATE SET
#                                 ItemPrice = source.ItemPrice,
#                                 sum_formatted = source.sum_formatted,
#                                 Quantity = source.Quantity,
#                                 DiscountPercentage = source.DiscountPercentage
#                         WHEN NOT MATCHED THEN
#                             INSERT (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage)
#                             VALUES (source.DealID, source.id, source.ProductID, source.ProductName, source.ItemPrice, source.sum_formatted, source.Quantity, source.DiscountPercentage);
#                     '''
#                     cursor.execute(merge_query, product_values)
#                     conn.commit()
#                     print(f"Produto inserido/atualizado: DealID = {deal_id}, ProductID = {product_id}")

#         page += 1

#     conn.close()

# if __name__ == "__main__":
#     main()


import os
import requests
from dotenv import load_dotenv
import pyodbc

load_dotenv()

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

def extract_data(endpoint, params):
    response = requests.get(f"{base_url}{endpoint}", params=params)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        print("Erro na solicitação:", response.status_code)
        return []

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
                        product.get("product_id"),
                        product.get("name"),
                        product.get("item_price"),
                        product.get("sum_formatted"),
                        product.get("quantity"),
                        product.get("discount_percentage")
                    ]
                    merge_query = '''
                        MERGE INTO Products_Deals_raw AS target
                        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage)
                        ON target.DealID = source.DealID AND target.id = source.id
                        WHEN MATCHED THEN
                            UPDATE SET
                                ItemPrice = source.ItemPrice,
                                sum_formatted = source.sum_formatted,
                                Quantity = source.Quantity,
                                DiscountPercentage = source.DiscountPercentage
                        WHEN NOT MATCHED THEN
                            INSERT (DealID, id, ProductID, ProductName, ItemPrice, sum_formatted, Quantity, DiscountPercentage)
                            VALUES (source.DealID, source.id, source.ProductID, source.ProductName, source.ItemPrice, source.sum_formatted, source.Quantity, source.DiscountPercentage);
                    '''
                    cursor.execute(merge_query, product_values)
                    conn.commit()
                    if cursor.rowcount > 0:
                        print(f"Produto inserido/atualizado: DealID = {deal_id}, ProductID = {product_id}")
                    else:
                        print(f"Produto não foi inserido/atualizado: DealID = {deal_id}, ProductID = {product_id}")

        page += 1

    conn.close()

if __name__ == "__main__":
    main()
