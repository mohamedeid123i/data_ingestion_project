import duckdb
import dlt
import json
from datetime import date
from pathlib import Path

@dlt.resource(name="orders", primary_key="order_id", write_disposition="merge")
def orders_resource():
    """Return the order records that will be loaded into DuckDB."""
    json_path = Path(__file__).resolve().parent / "data" / "orders" / "orders.json"

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        for row in data:
            yield {
                "order_id": int(row["order_id"]),
                "order_date": date.fromisoformat(row["order_date"]),
                "customer": row["customer"],
                "items": row["items"],
            }

def load_orders():
    """Run the dlt pipeline and merge the target table on each run."""
    pipeline = dlt.pipeline(
        pipeline_name="orders_pipeline_merge",
        destination="duckdb",
        dataset_name="orders_data"     
    )
    load_info = pipeline.run(
        orders_resource,
        write_disposition="merge",
        schema_contract={
            "tables": "evolve",
            "columns": "evolve",
            "data_type": "evolve",
        },
    )

    print("Load completed:")
    print(load_info)    


def verify_orders(expected_count=12):
    """Verify that DuckDB contains the expected orders table and row count."""
    connection = duckdb.connect("orders_pipeline_merge.duckdb", read_only=True)

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
        print("\nOrder items:")
        print(connection.sql(
            "SELECT * FROM orders_data.orders__items "
            "ORDER BY _dlt_parent_id, _dlt_list_idx"
        ))
        print("\nOrders with customer and items:")
        print(connection.sql("""
            SELECT
                o.order_id,
                o.order_date,
                o.customer__customer_id AS customer_id,
                o.customer__name AS customer_name,
                o.customer__country AS customer_country,
                i.product_id,
                i.product_name,
                i.quantity,
                i.price,
                i.discount,
                i.warranty,
                i.stock,
                i.quantity * i.price AS line_total
            FROM orders_data.orders AS o
            JOIN orders_data.orders__items AS i
                ON i._dlt_parent_id = o._dlt_id
            ORDER BY o.order_id, i._dlt_list_idx
        """))
        print("\nVerification passed.")
    finally:
        connection.close()


def main():
    load_orders()
    verify_orders()


if __name__ == "__main__":
    main()
    