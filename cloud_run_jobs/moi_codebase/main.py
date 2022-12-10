import requests
import os
import geopandas as gpd
from google.cloud import bigquery
import shapely
import json

# =====BigQuery client=====
# For "production", use default credentials.
# For "development", go to https://console.cloud.google.com/apis/credentials
# and find your "Service Accounts" and click in. 
# Click "KEYS" slide, then click "ADD KEY" => "Create New Key" => "JSON", then a json file will be downloaded.
# Save the json file in ./workplace in the dev container.
# $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
client = bigquery.Client()

# test if the outbound connection is working ...
url = 'https://httpbin.org/ip'
try:
    resp = requests.get(url)
    print(resp.content)
except:
    print(f'unable to connect to {url}')

# data source
# 政府資料開放平台: 最小統計區圖: 全國最小統計區圖
# shp壓縮成rar檔
url = 'https://data.moi.gov.tw/MoiOD/System/DownloadFile.aspx?DATA=D6AA982D-9833-48EC-8CA1-1D3D13505AF1'

# get data and save
resp = requests.get(url, allow_redirects=True)
with open('0.rar', 'wb') as f:
    f.write(resp.content)

# decompress rar file into ./shp folder
os.system('mkdir ./shp && cd shp && unrar e ../0.rar')

# read data into GeoPandas dataframe
gdf = gpd.read_file('shp')

# Projection from TWD97 to WGS84
gdf.set_crs(epsg = 3826, inplace = True)
gdf.to_crs(epsg = 4326, inplace = True)

# force 3D to 2D, since BigQuery GEOGRAPHY type only accept 2D data. 
def shape_to_2D_geojson(x):
    x2 = shapely.ops.transform(lambda x ,y, z: (x,y), x)
    return json.dumps(shapely.geometry.mapping( x2))
gdf['geo'] = gdf.geometry.map( shape_to_2D_geojson )
gdf.drop('geometry', axis = 1, inplace = True)



# Settings for the new table in BigQuery. 
table_id = 'corgis-361708.administrative_division.moi_codebase2'
job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField("geo", "GEOGRAPHY")],
    write_disposition = 'WRITE_TRUNCATE',    
)

# Load data.
job = client.load_table_from_dataframe(
    gdf, 
    table_id, 
    job_config=job_config
)

# Wait for job to complete.
job.result()

# BigQuery accept WKB, not EWKB.
# EWKB vs WKB
# https://gis.stackexchange.com/questions/412274/how-to-import-postgis-into-bigquery-gis
