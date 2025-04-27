from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='extract_fact_sales',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    create_fact_sales = PostgresOperator(
        task_id='create_fact_sales_task',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS fact_sales AS
        SELECT 
            "sls_ord_num" AS order_number,
            "sls_prd_key" AS product_key,
            "sls_cust_id" AS customer_id,
            "sls_order_dt" AS order_date,
            "sls_ship_dt" AS ship_date,
            "sls_due_dt" AS due_date,
            "sls_sales" AS sales_amount,
            "sls_quantity" AS quantity,
            "sls_price" AS price
        FROM sales_details
        WHERE "sls_ord_num" IS NOT NULL;
        """,
    )
