from datetime import timedelta, datetime
import logging
import pendulum
import pandas as pd
from sqlalchemy import create_engine
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import queries.etl_to_datamart_spotify as etl

conn_postgre = 'local_postgresql'

log = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env = Variable.get("my_credential", deserialize_json=True)
var_client_id = env["client_id"]
var_client_secret = env["client_secret"]

def get_spotify_token():
    client_id = var_client_id
    client_secret = var_client_secret
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    auth_response = requests.post(auth_url, data=auth_data)
    auth_response.raise_for_status()
    token = auth_response.json()['access_token']
    logging.info("Spotify token retrieved successfully.")
    return token

def setup_requests_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def fetch_spotify_data():
    token = get_spotify_token()
    session = setup_requests_session()
    base_url = 'https://api.spotify.com/v1/search'
    headers = {'Authorization': f'Bearer {token}'}
    # Get Internal Data
    excel_data = pd.read_excel('dags/internal_data/[DE TS] Song Catalog Data.xlsx', sheet_name='Data')

    data_list = []
    seen_tracks = set()
    for _, row in excel_data.iterrows():
        song_title = str(row['SONG TITLE']).strip()
        artist = str(row['ORIGINAL ARTIST']).strip() if pd.notna(row['ORIGINAL ARTIST']) else ''
        query = f"{song_title} {artist}".strip() if artist else song_title
        params = {'q': query, 'type': 'track', 'limit': 50}

        try:
            response = session.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json().get('tracks', {}).get('items', [])
            if data:
                for item in data:
                    track_id = item.get('id', 'N/A')
                    track_name = item.get('name', 'Unknown')
                    track_artist = item.get('artists', [{}])[0].get('name', 'Unknown')
                    track_key = (track_name.lower(), track_artist.lower())
                    if track_key in seen_tracks:
                        continue
                    seen_tracks.add(track_key)
                    track_info = {
                        'track_id': track_id,
                        'isrc': item.get('external_ids', {}).get('isrc', 'N/A'),
                        'song_title': track_name,
                        'artist': track_artist,
                        'album': item.get('album', {}).get('name', 'Unknown'),
                        'popularity': item.get('popularity', 0),
                        'release_date': item.get('album', {}).get('release_date', 'Unknown'),
                        'label': item.get('album', {}).get('label', 'Unknown')
                    }
                    data_list.append(track_info)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for query: {query}. Error: {e}")

    result_df = pd.DataFrame(data_list)
    logging.info(f"Fetched {len(result_df)} records from Spotify.")
    # Insert data into PostgreSQL
    engine = create_engine('postgresql+psycopg2://postgres:123@host.docker.internal:5432/postgres')
    table_name = 'landing_spotify'
    result_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Inserted {len(result_df)} rows into {table_name}")

with DAG(
        dag_id='dag_get_spotify',
        start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Jakarta'),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
) as dag:
    get_data = PythonOperator(
        task_id = 'get_spotify_data',
        python_callable = fetch_spotify_data,
    )

    delete_datamart = SQLExecuteQueryOperator(
        task_id = 'delete_spotify_datamart',
        conn_id = conn_postgre,
        sql = etl.DELETE_DATA
    )

    process_to_datamart = SQLExecuteQueryOperator(
        task_id = 'process_data_to_spotify_datamart',
        conn_id = conn_postgre,
        sql = etl.INSERT_DATA
    )
    get_data >> delete_datamart >> process_to_datamart
