import requests
import json
import datetime
import pandas as pd
import psycopg2

# seta o cabeçalho
headers = {
    "Content-Type": "application/json"
}

# Obter a data atual
current_date = datetime.datetime.now()

# Definir as datas de início e fim
start_date = current_date - datetime.timedelta(days=7)
end_date = current_date - datetime.timedelta(days=1)

# Converter as datas para objetos do tipo datetime
start_date_str = start_date.strftime("%d/%m/%Y")
end_date_str = end_date.strftime("%d/%m/%Y")

all_data = []

current_date = start_date
while current_date <= end_date:
    current_date_str = current_date.strftime("%d/%m/%Y")

    # define a carga do request como dicionario
    payload = {
        "password": "XXXXX",
        "cliente": "XXX"
    }

    #Envia uma solicitação http post para endpoint de autentificação
    response = requests.post("https://XXXX", headers=headers, data=json.dumps(payload))

    #Verifica se a resposta obteve sucesso
    if response.status_code == 200:
        json_data = response.json()
        token = json_data["token"]
    else:
        print("Requisição do token falhou")
        exit()
    
    payload = {
        "identifier": "32",
        "filters": {
            "page": 1,
            "size": 10000,
            "data": current_date_str
        }
    }

    headers = {
        "X-Auth-Token": token,
        "Content-Type": "application/json"
    }

    response = requests.post("https://XXXX", headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        json_data = response.json()
        total_pages = json_data["total_pages"]
        print(f"Número de páginas: {total_pages}")
    else:
        print("Requisição da carga falhou", response.status_code)
        exit()

    for page in range(1, total_pages + 1):
        payload["filters"]["page"] = page
        response = requests.post("https://XXXX", headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            json_data = response.json()
            data = json_data["data"]
            all_data.extend(data)
            print(f"Dados retirados com sucesso da página {page} do dia {current_date_str}")
        else:
            print(f"Dados não foram retirados da página {page} do dia {current_date_str}")

    current_date += datetime.timedelta(days=1)

json_file_path = "data1.json"
with open(json_file_path, "w") as file:
    json.dump(all_data, file)

print("Dados salvos com sucesso")

# Define the path to the JSON file
json_file_path = "data1.json"

# Open the JSON file for reading
try:
    with open(json_file_path, "r") as file:
        json_data = json.load(file)
except FileNotFoundError:
    print(f"JSON file '{json_file_path}' not found.")
    exit(1)
except Exception as e:
    print(f"Error reading JSON file: {str(e)}")
    exit(1)

# Create a DataFrame from the JSON data
df = pd.DataFrame(json_data)

# Convert date columns to datetime objects
df['DT_LOCAL'] = pd.to_datetime(df['DT_LOCAL'], format='%d/%m/%Y')
df['DT_HR_LOCAL'] = pd.to_datetime(df['DT_HR_LOCAL'], format='%d/%m/%Y %H:%M:%S')

# Convert time-related columns to timedelta objects
time_columns = ['HRS_OPERACIONAIS', 'HRS_IMPLEMENTO_LIGADO', 'HRS_MOTOR_LIGADO']
for col in time_columns:
    df[col] = pd.to_timedelta(df[col])

# Group and aggregate data
grouped = df.groupby(['EQUIPAMENTO', 'DT_LOCAL', 'DESC_GRUPO_OPERACAO']).agg({
    'HRS_OPERACIONAIS': 'sum',
    'VL_VELOCIDADE_MEDIA': 'mean'
}).reset_index()

# Extract only the hours part from the timedelta
grouped['HRS_OPERACIONAIS'] = grouped['HRS_OPERACIONAIS'].apply(lambda x: str(x).split()[2])

# Create a DataFrame with desired columns
result_df = grouped[['EQUIPAMENTO', 'DT_LOCAL', 'HRS_OPERACIONAIS', 'VL_VELOCIDADE_MEDIA', 'DESC_GRUPO_OPERACAO']]

# Establish a connection to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname="XXXX",
        user="XXXX",
        password="XXXX",
        host="XXX.X.X.XXX",
        port="5432"
    )
    print("Connected to PostgreSQL")
except Exception as e:
    print(f"Error connecting to the database: {str(e)}")
    exit(1)

# Create a database cursor
cursor = conn.cursor()

# Insert data into the PostgreSQL table
try:
    for index, row in result_df.iterrows():
        insert_query = '''
        INSERT INTO "Eschema"."Sua_tabela" (EQUIPAMENTO, DT_LOCAL, HRS_OPERACIONAIS, VL_VELOCIDADE_MEDIA, DESC_GRUPO_OPERACAO)
        VALUES (%s, %s, %s, %s, %s);
        '''
        cursor.execute(insert_query, (
            row['EQUIPAMENTO'],
            row['DT_LOCAL'],
            row['HRS_OPERACIONAIS'],
            row['VL_VELOCIDADE_MEDIA'],
            row['DESC_GRUPO_OPERACAO']
        ))
    conn.commit()
    print("Data inserted successfully")
except Exception as e:
    print(f"Error inserting data into the database: {str(e)}")
    conn.rollback()

# Close the database connection
if conn:
    conn.close()
    print("Connection closed")




