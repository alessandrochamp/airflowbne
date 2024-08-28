import os
import pyodbc
import psycopg2
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def get_sqlserver_connection() -> pyodbc.Connection:
    """
    Estabelece uma conexão com o SQL Server usando as credenciais fornecidas no arquivo .env.

    Retorna:
        pyodbc.Connection: Objeto de conexão com o SQL Server.
    """
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQLSERVER_HOST')};"
        f"DATABASE={os.getenv('SQLSERVER_DB')};"
        f"UID={os.getenv('SQLSERVER_USER')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD')}"
    )
    return pyodbc.connect(connection_string)


def get_postgres_connection() -> psycopg2.extensions.connection:
    """
    Estabelece uma conexão com o PostgreSQL usando as credenciais fornecidas no arquivo .env.

    Retorna:
        psycopg2.extensions.connection: Objeto de conexão com o PostgreSQL.
    """
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )


def fetch_data_from_db(connection, sql_file_path: str) -> pd.DataFrame:
    """
    Executa uma consulta SQL em uma conexão de banco de dados e retorna os resultados como um DataFrame.

    Args:
        connection: Objeto de conexão com o banco de dados.
        sql_file_path (str): Caminho para o arquivo SQL que contém a consulta.

    Retorna:
        pd.DataFrame: Resultados da consulta em um DataFrame.
    """
    with open(sql_file_path, 'r') as file:
        query = file.read()
    return pd.read_sql_query(query, connection)


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, merge_column: str) -> pd.DataFrame:
    """
    Realiza o merge de dois DataFrames com base em uma coluna comum.

    Args:
        df1 (pd.DataFrame): Primeiro DataFrame.
        df2 (pd.DataFrame): Segundo DataFrame.
        merge_column (str): Nome da coluna usada para realizar o merge.

    Retorna:
        pd.DataFrame: DataFrame resultante do merge.
    """
    return pd.merge(df1, df2, on=merge_column)


def get_azure_data_lake_client() -> DataLakeServiceClient:
    """
    Estabelece uma conexão com o Azure Data Lake Storage Gen2 usando as credenciais fornecidas no arquivo .env.

    Retorna:
        DataLakeServiceClient: Objeto de cliente do Azure Data Lake Storage Gen2.
    """
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )
    return service_client


def upload_to_azure_datalake(service_client: DataLakeServiceClient, container_name: str, 
                             file_path: str, data: str) -> None:
    """
    Carrega um arquivo para o Azure Data Lake Storage Gen2.

    Args:
        service_client (DataLakeServiceClient): Objeto de cliente do Azure Data Lake Storage Gen2.
        container_name (str): Nome do contêiner no Azure Data Lake.
        file_path (str): Caminho do arquivo no Azure Data Lake onde os dados serão salvos.
        data (str): Dados a serem carregados.
    """
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    directory_client = file_system_client.get_directory_client("")
    file_client = directory_client.get_file_client(file_path)

    file_client.upload_data(data, overwrite=True)


def load_and_merge_data() -> None:
    """
    Executa o processo de conexão com SQL Server e PostgreSQL, busca os dados, realiza o merge e 
    salva os dados resultantes no Azure Data Lake Storage Gen2.
    """
    # Conectando ao SQL Server e PostgreSQL
    sqlserver_conn = get_sqlserver_connection()
    postgres_conn = get_postgres_connection()
    
    # Buscando dados
    sqlserver_data = fetch_data_from_db(sqlserver_conn, 'dags/sql/query_sqlserver.sql')
    postgres_data = fetch_data_from_db(postgres_conn, 'dags/sql/query_postgres.sql')
    
    # Realizando o merge dos DataFrames
    merged_data = merge_dataframes(sqlserver_data, postgres_data, 'id')  # Substitua 'id' pela coluna de join
    merged_csv = merged_data.to_csv(index=False)
    
    # Conectando ao Azure Data Lake e fazendo upload dos dados
    azure_client = get_azure_data_lake_client()
    upload_to_azure_datalake(azure_client, 'your_container_name', 'merged_data.csv', merged_csv)


# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    'piloto',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    piloto = PythonOperator(
        task_id='load_and_merge_data',
        python_callable=load_and_merge_data,
    )