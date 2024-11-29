from datetime import timedelta, datetime
import logging
import itertools
import pendulum
import pandas as pd
from sqlalchemy import create_engine
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env = Variable.get("my_credential", deserialize_json=True)
cred_youtube_1 = env["youtube_cred1"]
cred_youtube_2 = env["youtube_cred2"]
cred_youtube_3 = env["youtube_cred3"]
cred_youtube_4 = env["youtube_cred4"]
cred_youtube_5 = env["youtube_cred5"]
cred_youtube_6 = env["youtube_cred6"]
cred_youtube_7 = env["youtube_cred7"]
api_keys = [
    cred_youtube_1, cred_youtube_2, cred_youtube_3, cred_youtube_4,
    cred_youtube_5, cred_youtube_6, cred_youtube_7
]
key_cycle = itertools.cycle(api_keys)

def setup_requests_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

def fetch_youtube_data():
    BASE_URL = "https://www.googleapis.com/youtube/v3/search"

    db_uri = 'postgresql+psycopg2://postgres:123@host.docker.internal:5432/postgres'
    engine = create_engine(db_uri)

    with engine.connect() as conn:
        query = f"""SELECT song_title, artist FROM spotify_datamart"""
        df = pd.read_sql(query, conn)

    session = setup_requests_session()
    current_key = next(key_cycle)
    collect_data = []

    for index, row in df.iterrows():
        song_title = row['song_title']
        artist = row['artist']
        query = f"{artist} {song_title}".strip()
        params = {
            'part': 'snippet',
            'q': query,
            'type': 'video',
            'maxResults': 1,
            'key': current_key
        }
        while True:
            response = session.get(BASE_URL, params=params)
            if response.status_code == 200:
                video_items = response.json().get('items', [])
                if video_items:
                    snippet = video_items[0]['snippet']
                    collect_data.append({
                        'video_id': video_items[0]['id']['videoId'],
                        'video_title': snippet['title'],
                        'channel_id': snippet['channelId'],
                        'channel_name': snippet['channelTitle'],
                        'song_title': song_title,
                        'artist': artist,
                        'descriptions':snippet['description'],
                    })
                if len(collect_data) >= 10:
                    insert_data(engine, collect_data)
                    collect_data = []
                break
            elif response.status_code == 403:  # Quota exceeded
                logging.warning(f"Quota exceeded for API key: {current_key}")
                current_key = next(key_cycle)  # Switch to the next key
                params['key'] = current_key  # Update the key and retry
            else:
                logging.error(f"API call failed for query: {query}, Status Code: {response.status_code}")
                break
    if collect_data:
        insert_data(engine, collect_data)

def insert_data(engine, data):
    try:
        result_df = pd.DataFrame(data)
        result_df.to_sql('landing_youtube', engine, if_exists='append', index=False)
        logging.info("Batch inserted into landing_youtube successfully.")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")

with DAG(
        dag_id='dag_get_youtube',
        start_date=pendulum.datetime(2024, 11, 25, tz='Asia/Jakarta'),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
) as dag:
    get_data = PythonOperator(
        task_id='get_spotify_data',
        python_callable=fetch_youtube_data,
    )
    get_data
