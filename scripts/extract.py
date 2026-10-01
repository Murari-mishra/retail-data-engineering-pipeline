import pandas as pd


def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)

        print("Data extracted successfully!")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        return df

    except Exception as e:
        print(f"Error extracting data: {e}")
        raise


if __name__ == "__main__":

    file_path = "/opt/airflow/data/retail_orders_data.csv"

    raw_data = extract_data(file_path)

    print("\nFirst 10 rows:")
    print(raw_data.head(10))

    print("\nShape:")
    print(raw_data.shape)

    print("\nColumns:")
    print(raw_data.columns.tolist())