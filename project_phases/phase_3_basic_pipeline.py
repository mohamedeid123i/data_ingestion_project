import duckdb
import dlt
import json
from datetime import date
from pathlib import Path



@dlt.resource(name="orders",primary_key="order_id", write_disposition="merge")
def orders_resource():
    """Return the order records that will be loaded into DuckDB."""
    json_path = Path(__file__).resolve().parent / "data" / "orders" / "orders.json"

    # Load data from JSON
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        for row in data:
            yield {
                "order_id": int(row["order_id"]),
                "customer_id": int(row["customer_id"]),
                "order_date": date.fromisoformat(row["order_date"]),
                "amount": float(row["amount"]),
                "status": row["status"],
            }

def load_orders():
        for row in json.load(json_path.open("r", encoding="utf-8")):
            yield {
                "order_id": int(row["order_id"]),
                "customer_id": int(row["customer_id"]),
                "order_date": date.fromisoformat(row["order_date"]),
                "amount": float(row["amount"]),
                "status": row["status"],
            }

def load_orders():
    """Run the dlt pipeline and replace the target table on each run."""
    pipeline = dlt.pipeline(
        pipeline_name="orders_pipeline",
        destination="duckdb",
        dataset_name="orders_data",
    )
    load_info = pipeline.run(
        orders_resource,
        write_disposition="replace",
    )

    print("Load completed:")
    print(load_info)    


def verify_orders(expected_count=12):
    """Verify that DuckDB contains the expected orders table and row count."""
    connection = duckdb.connect("orders_pipeline.duckdb", read_only=True)

    try:
        print("\nTables:")
        print(connection.sql("SHOW TABLES"))

        print("\nSchema:")
        print(connection.sql("DESCRIBE orders_data.orders"))

        row_count = connection.sql(
            "SELECT COUNT(*) AS row_count FROM orders_data.orders"
        ).fetchone()[0]

        print(f"Rows in orders_data.orders: {row_count}")

        if row_count != expected_count:
            raise AssertionError(
                f"Expected {expected_count} rows, but found {row_count}"
            )

        print("\nSample data:")
        print(connection.sql("SELECT * FROM orders_data.orders ORDER BY order_id"))
        print("\nVerification passed.")
    finally:
        connection.close()


def main():
    load_orders()
    verify_orders()


if __name__ == "__main__":
    main()
    