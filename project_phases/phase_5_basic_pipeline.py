import json
from pathlib import Path

import duckdb
import dlt


USERS_JSON_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "orders"
    / "users.json"
)


def alert(message):
    """Print a simple pipeline alert with the error details."""
    print(f"\n[PIPELINE ALERT] {message}")


@dlt.resource(
    name="users",
    primary_key="id",
    write_disposition="merge",
)
def users_resource():
    """Load all fields from local users JSON records."""
    with USERS_JSON_PATH.open("r", encoding="utf-8") as file:
        users = json.load(file)

    for user in users:
        yield user


def load_users():
    """Run the users pipeline and replace the destination table."""
    pipeline = dlt.pipeline(
        pipeline_name="phase_5_users_pipeline",
        destination="duckdb",
        dataset_name="users_data",
        pipelines_dir=str(Path(__file__).resolve().parent / ".dlt" / "pipelines"),
    )

    try:
        load_info = pipeline.run(
            users_resource,
            write_disposition="merge",
            schema_contract={
                "tables": "evolve",
                "columns": "freeze",
                "data_type": "evolve",
            },
        )

        if load_info.has_failed_jobs:
            load_info.raise_on_failed_jobs()

        print("Load completed:")
        print(load_info)
    except Exception as error:
        alert(f"{type(error).__name__}: {error}")
        raise


def verify_users(expected_count=2):
    """Verify the loaded row count, unique IDs, and sample rows."""
    database_path = (
        Path(__file__).resolve().parent.parent
        / "phase_5_users_pipeline.duckdb"
    )
    connection = duckdb.connect(str(database_path), read_only=True)

    try:
        print("\nTables:")
        print(connection.sql("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """))

        row_count = connection.sql(
            "SELECT COUNT(*) FROM users_data.users"
        ).fetchone()[0]
        unique_ids = connection.sql(
            "SELECT COUNT(DISTINCT id) FROM users_data.users"
        ).fetchone()[0]

        print(f"\nRows: {row_count}")
        print(f"Unique ids: {unique_ids}")

        if row_count != expected_count:
            raise AssertionError(
                f"Expected {expected_count} users, but found {row_count}"
            )
        if unique_ids != row_count:
            raise AssertionError(
                f"Expected {row_count} unique ids, but found {unique_ids}"
            )

        print("\nUsers:")
        print(connection.sql("""
            SELECT *
            FROM users_data.users
            ORDER BY id
        """))
        print("\nVerification passed.")
    finally:
        connection.close()


def main():
    load_users()
    verify_users()


if __name__ == "__main__":
    main()