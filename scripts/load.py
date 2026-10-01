import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


def load_data_to_postgresql(
    csv_file_path,
    db_conn_params,
    table_name
):

    conn = None
    cur = None

    try:

        # Read transformed CSV
        df = pd.read_csv(
            csv_file_path,
            dtype=str
        )

        print(
            f"Data read successfully from "
            f"{csv_file_path}"
        )

        if df.empty:

            raise ValueError(
                "CSV file is empty."
            )

        # Clean column names
        df.columns = [
            c.strip().lower()
            for c in df.columns
        ]

        # Convert NaN to None
        df = df.astype(object).where(
            df.notnull(),
            None
        )

        # Connect PostgreSQL
        conn = psycopg2.connect(
            **db_conn_params
        )

        print(
            "PostgreSQL connection successful!"
        )

        cur = conn.cursor()

        columns = df.columns.tolist()

        # Create table
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(
                    sql.Identifier(col)
                )
                for col in columns
            )
        )

        cur.execute(create_table_query)

        # Prevent duplicate data during Airflow retries
        cur.execute(
            sql.SQL(
                "TRUNCATE TABLE {}"
            ).format(
                sql.Identifier(table_name)
            )
        )

        conn.commit()

        print(
            f"Table {table_name} is ready."
        )

        # Insert data
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES %s"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                map(
                    sql.Identifier,
                    columns
                )
            )
        )

        rows_to_insert = list(
            df.itertuples(
                index=False,
                name=None
            )
        )

        execute_values(
            cur,
            insert_query,
            rows_to_insert,
            page_size=1000
        )

        conn.commit()

        print(
            f"Successfully loaded "
            f"{len(rows_to_insert)} rows."
        )

        # Verify load
        cur.execute(
            sql.SQL(
                "SELECT COUNT(*) FROM {}"
            ).format(
                sql.Identifier(table_name)
            )
        )

        total_rows = cur.fetchone()[0]

        print(
            f"Total rows in PostgreSQL: "
            f"{total_rows}"
        )

    except Exception as e:

        if conn:
            conn.rollback()

        print(
            f"Error loading data into PostgreSQL: {e}"
        )

        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

        print(
            "PostgreSQL connection closed."
        )


if __name__ == "__main__":

    csv_file_path = (
        "/opt/airflow/data/"
        "retail_transformed_data.csv"
    )

    db_conn_params = {

        "host": "postgres",
        "database": "datacraftinglab_db",
        "user": "postgres",
        "password": "postgres123",
        "port": "5432",
    }

    table_name = "retail_orders"

    load_data_to_postgresql(
        csv_file_path,
        db_conn_params,
        table_name
    )