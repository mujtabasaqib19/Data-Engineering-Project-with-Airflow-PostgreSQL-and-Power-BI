from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='dim_product',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    create_dim_product = PostgresOperator(
        task_id='create_dim_product_task',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS dim_product AS
        SELECT 
            pi."prd_key" AS product_key,
            pi."prd_nm" AS product_name,
            pi."prd_cost" AS cost,
            pi."prd_line" AS product_line,
            pi."prd_start_dt" AS start_date,
            pi."prd_end_dt" AS end_date,
            pc."CAT" AS category,
            pc."SUBCAT" AS subcategory,
            pc."MAINTENANCE" AS maintenance
        FROM prd_info pi
        LEFT JOIN px_cat_g1v2 pc ON LEFT(pi."prd_key", 5) = pc."CID"
        WHERE pi."prd_key" IS NOT NULL;
        """,
    )
