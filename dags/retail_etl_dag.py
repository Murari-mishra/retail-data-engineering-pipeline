import sys

sys.path.insert(0, "/opt/airflow")

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts.extract import extract_data
from scripts.transform import transform_data, save_transformed_data
from scripts.load import load_data_to_postgresql


RAW_FILE = "/opt/airflow/data/retail_orders_data.csv"

TRANSFORMED_FILE = (
    "/opt/airflow/data/retail_transformed_data.csv"
)


DB_PARAMS = {

    "host": "postgres",
    "database": "datacraftinglab_db",
    "user": "postgres",
    "password": "postgres123",
    "port": "5432",
}


def extract_task():

    df = extract_data(
        RAW_FILE
    )

    if df is None or df.empty:

        raise ValueError(
            "Extracted data is empty."
        )


def transform_task():

    df = extract_data(
        RAW_FILE
    )

    transformed_df = transform_data(
        df
    )

    save_transformed_data(
        transformed_df,
        TRANSFORMED_FILE
    )


def load_task():

    load_data_to_postgresql(
        TRANSFORMED_FILE,
        DB_PARAMS,
        "retail_orders"
    )


default_args = {

    "owner": "data-engineering",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(
        minutes=2
    ),
}


with DAG(

    dag_id="retail_sales_etl",

    default_args=default_args,

    description=(
        "Daily retail sales ETL pipeline "
        "using Python, PostgreSQL, SQL and Bash"
    ),

    start_date=datetime(
        2026,
        9,
        1
    ),

    schedule="0 6 * * *",

    catchup=False,

    tags=[
        "retail",
        "etl",
        "python",
        "postgresql"
    ],

) as dag:

    extract = PythonOperator(

        task_id="extract_data",

        python_callable=extract_task,
    )


    transform = PythonOperator(

        task_id="transform_data",

        python_callable=transform_task,
    )


    load = PythonOperator(

        task_id="load_to_postgresql",

        python_callable=load_task,
    )


    sql_validation = SQLExecuteQueryOperator(

        task_id="validate_data",

        conn_id="retail_postgres",

        sql="""

        SELECT
            COUNT(*) AS total_records
        FROM retail_orders;

        SELECT
            COUNT(*) AS missing_order_ids
        FROM retail_orders
        WHERE order_id IS NULL
           OR TRIM(order_id) = '';

        SELECT
            COUNT(*) - COUNT(DISTINCT order_id)
            AS duplicate_order_ids
        FROM retail_orders;

        """,

        split_statements=True,
    )


    check_file = BashOperator(

        task_id="check_transformed_file",

        bash_command=f"""

        echo "Checking transformed file..."

        if [ -f "{TRANSFORMED_FILE}" ]; then

            echo "SUCCESS: Transformed file exists."

            ls -lh "{TRANSFORMED_FILE}"

        else

            echo "ERROR: Transformed file does not exist."

            exit 1

        fi

        """,
    )


    extract >> transform >> load >> sql_validation >> check_file