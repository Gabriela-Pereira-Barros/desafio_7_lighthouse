from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get("my_email")],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def read_orders_to_csv(**context):
    # Pegar a data de execução
    execution_date = context['ds_nodash']
    
    # Conectar ao banco de dados
    conn = sqlite3.connect('/home/gabriela/Documents/Indicium/desafio_7/desafio_7_lighthouse/data/Northwind_small.sqlite')
    
    # Query para obter dados da tabela Orders
    query = "SELECT * FROM 'Order';"
    
    # Ler dados da tabela para um DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Fechar a conexão com o banco
    conn.close()
    
    # Escrever o DataFrame em um arquivo CSV
    df.to_csv(f'/home/gabriela/Documents/Indicium/desafio_7/desafio_7_lighthouse/data/stage/output_orders-{execution_date}.csv', index=False)

def join_orders_and_details(**context):
    # Pegar a data de execução
    execution_date = context['ds_nodash']
    
    # Conectar ao banco de dados
    conn = sqlite3.connect('/home/gabriela/Documents/Indicium/desafio_7/desafio_7_lighthouse/data/Northwind_small.sqlite')

    # Query para obter dados da tabela OrderDetail
    query = "SELECT * FROM 'OrderDetail';"

    # Ler dados da tabela para um DataFrame
    df_details = pd.read_sql_query(query, conn)

    # Fechar a conexão com o banco
    conn.close()

    # Ler arquivo csv contendo Orders
    df_order = pd.read_csv(f'/home/gabriela/Documents/Indicium/desafio_7/desafio_7_lighthousedata/stage/output_orders-{execution_date}.csv')

    # Join entre as tabelas
    resultado = df_order.merge(df_details, left_on='Id', right_on='OrderId', how='left')

    # Filtrando resultado
    resultado = resultado = resultado[resultado['ShipCity'] == 'Rio de Janeiro']
        
    # Calculando resultado final
    resultado = str(resultado['Quantity'].sum())

    # Escrever o resultado no arquivo de texto
    arquivo_resultado = (f'/home/gabriela/Documents/Indicium/desafio_7/desafio_7_lighthouse/data/target/final_output-{execution_date}.txt')
    with open(arquivo_resultado, 'w') as arquivo:
        arquivo.write(resultado)

    
with DAG(
    'ELT_Desafio_7',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Lighthouse'],
) as dag:
    
    extract_postgres_task = PythonOperator(
        task_id='extract_postgres',
        python_callable=read_orders_to_csv,
    )

    export_final_output = PythonOperator(
        task_id='final_output',
        python_callable=join_orders_and_details,
    )

extract_postgres_task >> export_final_output