from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import sqlite3

# task1: le os dados da tabela order e exportar para CSV
def export_orders_to_csv():
    # conecta com o banco de dados
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    # seleciona a tebla order
    orders_df = pd.read_sql_query("SELECT * FROM 'Order'", conn)
    # escreve o arquivo csv
    orders_df.to_csv('output_orders.csv', index=False)
    # fecha a conexao
    conn.close()

# task2: calcula a soma da quantidade vendida para o RJ
def calculate_quantity_for_rio():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    # seleciona a tabela orderdetail e o arquivo csv
    order_details_df = pd.read_sql_query("SELECT * FROM 'OrderDetail'", conn)
    orders_df = pd.read_csv('output_orders.csv')
    # faz o JOIN entre as duas tabelas
    merged_df = pd.merge(order_details_df, orders_df, left_on='OrderId', right_on='Id')
    # filtra para RJ e calcula a soma da quantidade
    quantity_sum = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
    # fecha a conexao
    conn.close()
    with open('count.txt', 'w') as f:
        f.write(str(quantity_sum))

# task3: cria um arquivo final de output com um texto codificado
def export_final_output():
    with open('count.txt', 'r') as f:
        quantity_sum = f.read().strip()

    with open('final_output.txt', 'w') as f:
        f.write(quantity_sum)

# configuração da dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_luiza',
    default_args=default_args,
    description='Desafio 7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 8),
    catchup=False,
    tags=['example'],
) as dag:

    export_orders = PythonOperator(
        task_id='export_orders',
        python_callable=export_orders_to_csv,
    )

    calculate_rio_quantity = PythonOperator(
        task_id='calculate_rio_quantity',
        python_callable=calculate_quantity_for_rio,
    )

    export_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output,
    )

    export_orders >> calculate_rio_quantity >> export_output
