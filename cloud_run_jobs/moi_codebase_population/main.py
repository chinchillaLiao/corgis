import requests
import xml.etree.ElementTree as ET
import pandas as pd
from google.cloud import bigquery
# gcloud functions call population --region asia-east1
# data source page: https://data.gov.tw/dataset/18681
urls = [
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=B543B82F-6F42-4026-A11D-479BF8D553C2', #連江縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=05472A79-F07F-4499-A52F-F8421887EE49', #雲林縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=79561B37-CE08-4460-AF25-E9E8E49D0EF9', #嘉義縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=E0AF6D88-2ACE-4F14-A847-05B2616A23D1', #花蓮縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=898C0344-74DF-4A4B-89FD-5D384B1A8FBF', #嘉義市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=C4DBC8FA-C53B-4C85-9799-0B1E29A99C1F', #新竹縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=1CC0A662-CED8-4958-A81B-CF8F93A5522C', #宜蘭縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=14A6C503-E32E-4572-A279-4A3FF8FF7273', #臺南市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=B7EE8228-D28F-488E-B62D-1E5A2F000DFC', #臺中市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=74207BB3-419E-4DE4-A4C4-9E2D903F9944', #金門縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=BEBC8A5A-FAF1-4942-8D85-30E7D15BA76A', #澎湖縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=0247D341-5141-488F-8678-3DC90A0D791B', #臺北市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=B9F69161-B77A-4ADE-8361-06290B8255DB', #新北市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=33675F9C-8712-48CF-B84D-A016159DBC96', #彰化縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=B0534A44-4495-41C6-9B18-810B0741AEEB', #桃園市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=3AA10BE9-BDC6-4F54-A245-08D2BDF883A6', #基隆市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=A069682C-136C-47D8-8C0E-312B5AE03A4E', #臺東縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=A02A7097-76EC-43D4-BE7E-9450E959B349', #新竹市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=810A477C-B90A-45A1-9C33-3C7B632D82CE', #苗栗縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=131ABC31-30AD-427A-BE90-EB3C2B594067', #屏東縣
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=338F6C78-AA5E-4977-8D25-527D3C5DC85B', #高雄市
    'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=97B2AF14-5109-4F3F-9B51-9F13CB6FCD19', #南投縣
]

def query(url) :
    print(url)
    
    resp = requests.get(url)
    root = ET.fromstring(resp.text)
    rows = [x for x in root.find('RowDataList').findall('RowData')]
    data = [{x.tag: x.text for x in row} for row in rows]
    df = pd.DataFrame(data)
    df['H_CNT'] = df['H_CNT'].astype('int64')
    df['P_CNT'] = df['P_CNT'].astype('int64')
    df['M_CNT'] = df['M_CNT'].astype('int64')
    df['F_CNT'] = df['F_CNT'].astype('int64')
    return df

dfs = [query(url) for url in urls]
df = pd.concat(dfs)
# =====BigQuery client=====
# For "production", use default credentials.
# For "development", go to https://console.cloud.google.com/apis/credentials
# and find your "Service Accounts" and click in. 
# Click "KEYS" slide, then click "ADD KEY" => "Create New Key" => "JSON", then a json file will be downloaded.
# Save the json file in ./workplace in the dev container.
# $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
client = bigquery.Client()

# Settings for the new table in BigQuery. 
table_id = 'corgis-361708.population.moi_codebase_population'
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("H_CNT", "INT64"),
        bigquery.SchemaField("P_CNT", "INT64"),
        bigquery.SchemaField("M_CNT", "INT64"),
        bigquery.SchemaField("F_CNT", "INT64"),
    ],
    write_disposition = 'WRITE_TRUNCATE',    
)

# Load data.
job = client.load_table_from_dataframe(
    df, 
    table_id, 
    job_config=job_config
)

# Wait for job to complete.
job.result()