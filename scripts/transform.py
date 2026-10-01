import pandas as pd


def transform_data(df):

    try:

        # Clean column names
        df.columns = [
            c.strip().lower()
            for c in df.columns
        ]

        # Integer columns
        int_cols = [
            "order_id",
            "customer_id",
            "product_id",
            "quantity"
        ]

        for col in int_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                ).astype("Int64")

        # Numeric columns
        num_cols = [
            "unit_price",
            "discount_percent",
            "tax_rate_percent",
            "gross_amount",
            "net_amount",
            "shipping_cost",
            "total_amount"
        ]

        for col in num_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

        # Date columns
        if "order_date" in df.columns:

            df["order_date"] = pd.to_datetime(
                df["order_date"],
                errors="coerce"
            ).dt.date

        if "order_datetime" in df.columns:

            df["order_datetime"] = pd.to_datetime(
                df["order_datetime"],
                errors="coerce"
            )

        if "delivered_date" in df.columns:

            df["delivered_date"] = pd.to_datetime(
                df["delivered_date"],
                errors="coerce"
            )

        # Boolean columns
        bool_cols = [
            "is_first_order",
            "is_refunded",
            "is_high_value"
        ]

        for col in bool_cols:

            if col in df.columns:

                s = (
                    df[col]
                    .astype("string")
                    .str.strip()
                    .str.lower()
                )

                df[col] = s.map({
                    "1": True,
                    "0": False,
                    "true": True,
                    "false": False
                }).astype("boolean")

        # Text columns
        text_cols = [
            "order_status",
            "customer_name",
            "customer_email",
            "customer_segment",
            "customer_country",
            "customer_city",
            "product_category",
            "product_subcategory",
            "product_name",
            "payment_method",
            "payment_status",
            "sales_channel",
            "shipping_method",
            "currency",
            "promo_code"
        ]

        for col in text_cols:

            if col in df.columns:

                df[col] = (
                    df[col]
                    .astype("string")
                    .str.strip()
                )

        # Remove rows with missing order_id
        before = len(df)

        if "order_id" in df.columns:

            df = df[
                df["order_id"].notna()
            ].copy()

        dropped = before - len(df)

        print("Data transformed successfully!")
        print(
            f"Dropped rows missing order_id: {dropped}"
        )

        return df

    except Exception as e:

        print(f"Error transforming data: {e}")
        raise


def save_transformed_data(df, output_path):

    try:

        df.to_csv(
            output_path,
            index=False
        )

        print(
            f"Transformed data saved to: {output_path}"
        )

    except Exception as e:

        print(f"Error saving transformed data: {e}")
        raise


if __name__ == "__main__":

    input_path = (
        "/opt/airflow/data/"
        "retail_orders_data.csv"
    )

    output_path = (
        "/opt/airflow/data/"
        "retail_transformed_data.csv"
    )

    raw_data = pd.read_csv(input_path)

    transformed_data = transform_data(
        raw_data
    )

    print("\nTransformed data:")
    print(transformed_data.head(10))

    print("\nData types:")
    print(transformed_data.dtypes)

    print("\nShape:")
    print(transformed_data.shape)

    save_transformed_data(
        transformed_data,
        output_path
    )