import argparse
import json
from pathlib import Path

import duckdb
import dlt


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ORDERS_JSON_PATH = PROJECT_ROOT / "data" / "orders" / "orders.json"
DATABASE_PATH = PROJECT_ROOT / "phase_7_orders_merge_pipeline.duckdb"
PIPELINES_DIR = PROJECT_ROOT / "project_phases" / ".dlt" / "pipelines"


@dlt.resource(
    name="orders",
    primary_key="order_id",
    write_disposition="merge",
)
def orders_resource(
    updated_at=dlt.sources.incremental(
        "updated_at",
        initial_value="2025-01-01T00:00:00Z",
    ),
):
    """Load records newer than the persisted updated_at cursor."""
    with ORDERS_JSON_PATH.open("r", encoding="utf-8") as file:
        orders = json.load(file)

    print(f"Incremental cursor last_value: {updated_at.last_value}")
    print(f"Incremental cursor range: {updated_at.get_current_range()}")

    for order in sorted(orders, key=lambda item: item["updated_at"]):
        yield order


@dlt.resource(
    name="orders",
    primary_key="order_id",
    write_disposition="merge",
)
def orders_backfill_resource(backfill_start, backfill_end):
    """Load all source records inside an explicit order_date backfill range."""
    with ORDERS_JSON_PATH.open("r", encoding="utf-8") as file:
        orders = json.load(file)

    print(f"Backfill order_date range: {backfill_start} to {backfill_end}")

    for order in sorted(orders, key=lambda item: item["updated_at"]):
        if backfill_start <= order["order_date"] <= backfill_end:
            yield order


def build_pipeline():
    return dlt.pipeline(
        pipeline_name="phase_7_orders_merge_pipeline",
        destination=dlt.destinations.duckdb(credentials=str(DATABASE_PATH)),
        dataset_name="orders_data",
        pipelines_dir=str(PIPELINES_DIR),
    )


def load_orders():
    pipeline = build_pipeline()
    load_info = pipeline.run(orders_resource)

    if load_info.has_failed_jobs:
        load_info.raise_on_failed_jobs()

    print("Incremental load completed:")
    print(load_info)


def load_backfill(backfill_start, backfill_end):
    pipeline = build_pipeline()
    load_info = pipeline.run(
        orders_backfill_resource(
            backfill_start=backfill_start,
            backfill_end=backfill_end,
        )
    )

    if load_info.has_failed_jobs:
        load_info.raise_on_failed_jobs()

    print("Backfill load completed:")
    print(load_info)


def verify_orders():
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found: {DATABASE_PATH}. Run the pipeline first."
        )

    connection = duckdb.connect(str(DATABASE_PATH), read_only=True)

    try:
        row_count = connection.sql(
            "SELECT COUNT(*) FROM orders_data.orders"
        ).fetchone()[0]
        unique_ids = connection.sql(
            "SELECT COUNT(DISTINCT order_id) FROM orders_data.orders"
        ).fetchone()[0]

        print(f"\nRows: {row_count}")
        print(f"Unique order_id values: {unique_ids}")

        if unique_ids != row_count:
            raise AssertionError(
                f"Expected {row_count} unique order_id values, "
                f"but found {unique_ids}"
            )

        print("\nOrders:")
        print(connection.sql("""
            SELECT order_id, order_date, updated_at, status, amount
            FROM orders_data.orders
            ORDER BY order_id
        """))
        print("\nVerification passed.")
    finally:
        connection.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Phase 7: incremental and backfill merge for local orders"
    )
    parser.add_argument(
        "action",
        nargs="?",
        choices=("load", "backfill"),
        default="load",
    )
    parser.add_argument("--start", help="Inclusive backfill start: YYYY-MM-DD")
    parser.add_argument("--end", help="Inclusive backfill end: YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.action == "backfill":
        if not args.start or not args.end:
            raise ValueError("Backfill requires --start and --end.")
        load_backfill(args.start, args.end)
    else:
        load_orders()
    verify_orders()


if __name__ == "__main__":
    main()
