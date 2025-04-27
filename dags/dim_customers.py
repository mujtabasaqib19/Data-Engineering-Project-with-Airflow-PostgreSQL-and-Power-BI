from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dim_customer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    create_dim_customer = PostgresOperator(
        task_id='create_dim_customer_task',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS dim_customer AS
        SELECT 
            ci."cst_key" AS customer_key,
            ci."cst_firstname" AS first_name,
            ci."cst_lastname" AS last_name,
            ci."cst_marital_status" AS marital_status,
            ca."BDATE" AS birthdate,
            COALESCE(ci."cst_gndr", ca."GEN") AS gender,
            la."CNTRY" AS country
        FROM cust_info ci
        LEFT JOIN cust_az12 ca ON ci."cst_key" = SUBSTRING(ca."CID" FROM 4)
        LEFT JOIN loc_a101 la ON ci."cst_key" = SUBSTRING(la."CID" FROM 4)
        WHERE ci."cst_key" IS NOT NULL;
        """,
    )
