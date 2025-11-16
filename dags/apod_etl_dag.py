from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
import subprocess
import os

DATA_DIR = "/opt/airflow/data"
CSV_PATH = f"{DATA_DIR}/apod_data.csv"
NASA_URL = "https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "apod_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

def extract_data():
    response = requests.get(NASA_URL)
    response.raise_for_status()
    data = response.json()
    pd.DataFrame([data]).to_json(f"{DATA_DIR}/raw.json", orient="records")

def transform_data():
    df = pd.read_json(f"{DATA_DIR}/raw.json")
    df = df[["date", "title", "url", "explanation"]]
    df.to_csv(CSV_PATH, index=False)

def load_into_postgres():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS apod (
            date TEXT,
            title TEXT,
            url TEXT,
            explanation TEXT
        );
    """)

    df = pd.read_csv(CSV_PATH)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO apod (date, title, url, explanation)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()

def dvc_add():
    subprocess.run(["dvc", "add", CSV_PATH], cwd="/opt/airflow/project")

def git_commit():
    subprocess.run(["git", "add", "."], cwd="/opt/airflow/project")
    subprocess.run(["git", "commit", "-m", "Updated APOD data via Airflow"], cwd="/opt/airflow/project")

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_into_postgres,
    dag=dag
)

dvc_task = PythonOperator(
    task_id="dvc_add_csv",
    python_callable=dvc_add,
    dag=dag
)

git_task = PythonOperator(
    task_id="git_commit_dvc",
    python_callable=git_commit,
    dag=dag
)

extract_task >> transform_task >> load_task >> dvc_task >> git_task
