import json
import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s3_to_postgres import S3ToPostgresOperator
from airflow.utils.task_group import TaskGroup
import re


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 6, 1),
}

   
def _fetch_status_data(**context):
    # Crée une instance de S3Hook
    s3_hook = S3Hook(aws_conn_id="aws_default")
    # Spécifie le nom du bucket et le préfixe du chemin sous lequel les fichiers JSON sont stockés
    bucket_name = Variable.get("S3BucketName")
    path_prefix = "topics/LIME_V2/year=2024/month=02/day=23/hour=10"
    # Liste les fichiers dans le dossier spécifié
    file_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=path_prefix)

    filenames = []  # Liste pour stocker les noms des fichiers traités

    # Pour chaque fichier trouvé, lire son contenu et effectuer des opérations
    for file_key in file_keys:
        file_content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
        # Génère un nom de fichier unique basé sur la clé du fichier et l'heure actuelle
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        filename = f"{timestamp}_{file_key.split('/')[-1]}"
        full_path_to_file = f"/tmp/{filename}"

        with open(full_path_to_file, "w") as f:
            f.write(file_content)
        
        # Ajoute le nom du fichier à la liste des fichiers traités
        filenames.append(filename)

        # Charge le fichier dans S3
        s3_hook.load_file(filename=full_path_to_file, key=filename, bucket_name=bucket_name, replace=True)

    # Pousse la liste des noms de fichiers dans XComs d'Airflow
    context["task_instance"].xcom_push(key="status_filenames", value=filenames)

def _transform_status_data(**context):
    # Supposons que "status_filename" contient une liste de noms de fichiers.
    filenames = context["task_instance"].xcom_pull(key="status_filenames")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    bucket_name = Variable.get("S3BucketName")
    data_list = []

    for filename in filenames:
        # Télécharger chaque fichier depuis un bucket S3 spécifié
        returned_filename = s3_hook.download_file(key=filename, bucket_name=bucket_name, local_path="/tmp")
        
        # Ouvrir chaque fichier téléchargé en mode lecture
        with open(returned_filename, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    data_list.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")

    # Convertit la liste de dictionnaires en DataFrame
    df = pd.DataFrame(data_list)
    
    # Convertit les colonnes "time" en format datetime
    df["time"] = pd.to_datetime(df["time"])
    
    # Pour chaque fichier, générer un fichier CSV correspondant
    for filename in filenames:
        csv_filename = filename.split(".")[0] + ".csv"
        csv_filename_full_path = f"/tmp/{csv_filename}"
        
        # Sauvegarde le DataFrame en fichier CSV
        df.to_csv(csv_filename_full_path, index=False)
        
        # Téléverse le fichier CSV dans le même bucket S3
        s3_hook.load_file(filename=csv_filename_full_path, key=csv_filename, bucket_name=bucket_name)
        
        # Pousse le nom du fichier CSV dans XComs pour chaque fichier traité
        context["task_instance"].xcom_push(key=f"status_csv_filename_{filename}", value=csv_filename)

with DAG(dag_id="S3_to_RDS", default_args=default_args, schedule_interval="@hourly", catchup=False) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup(group_id="status_branch") as status_branch:
        fetch_status_data = PythonOperator(task_id="fetch_status_data", python_callable=_fetch_status_data)

        transform_status_data = PythonOperator(task_id="transform_status_data", python_callable=_transform_status_data)

        create_status_table = PostgresOperator(
            task_id="create_status_table",
            sql="""
            CREATE TABLE IF NOT EXISTS status_station (
                id SERIAL PRIMARY KEY,
                station_id VARCHAR,
                last_reported TIMESTAMP,
                bikes_available INTEGER,
                docks_available INTEGER
            )
            """,
            postgres_conn_id="postgres_default",
        )

        transfer_status_data_to_postgres = S3ToPostgresOperator(
            task_id="transfer_status_data_to_postgres",
            table="status_station",
            bucket="{{ var.value.S3BucketName }}",
            key="{{ task_instance.xcom_pull(key='status_csv_filename') }}",
            postgres_conn_id="postgres_default",
            aws_conn_id="aws_default",
        )

        fetch_status_data >> transform_status_data >> create_status_table >> transfer_status_data_to_postgres

    end = DummyOperator(task_id="end")

    start >>  status_branch >> end
