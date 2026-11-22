import duckdb
import dlt



@dlt.resource(name="users",primary_key="id", write_disposition="merge")
def users_resource():
    """Return the user records that will be loaded into DuckDB."""
    yield [
        {"id": 1, "name": "Ahmed ali", "country": "Egypt"},
        {"id": 2, "name": "Sara ali", "country": "Egypt"},
        {"id": 3, "name": "John", "country": "USA"},
        {"id": 4, "name": "Jane sina", "country": "USA"},
        {"id": 5, "name": "Ali mohamed", "country": "UAE"},
        {"id": 6, "name": "Fatima", "country": "UAE"},
        {"id": 7, "name": "Mohammed", "country": "KSA"},
        {"id": 8, "name": "Aisha", "country": "KSA"},
        {"id": 9, "name": "Omar magdi", "country": "KSA"},
        {"id": 10, "name": "Layla", "country": "KSA"},
        {"id": 11, "name": "Youssef", "country": "KSA"},
        {"id": 12, "name": "Mariam", "country": "KSA"},
        {"id": 13, "name": "Hassan", "country": "KSA"},
        {"id": 14, "name": "Noura", "country": "KSA"},
        {"id": 15, "name": "Tariq", "country": "KSA"},
        {"id": 16, "name": "Salma", "country": "KSA"},
        {"id": 17, "name": "Karim", "country": "KSA"},
        {"id": 18, "name": "Rania", "country": "KSA"},
        {"id": 19, "name": "Fadi", "country": "KSA"},
        {"id": 20, "name": "Lina", "country": "KSA"}
    ]


def load_users():
    """Run the dlt pipeline and replace the target table on each run."""
    pipeline = dlt.pipeline(
        pipeline_name="users_pipeline",
        destination="duckdb",
        dataset_name="users_data",
    )

    load_info = pipeline.run(
        users_resource,
        write_disposition="merge",
    )

    print("Load completed:")
    print(load_info)


def verify_users(expected_count=20):
    """Verify that DuckDB contains the expected users table and row count."""
    connection = duckdb.connect("users_pipeline.duckdb", read_only=True)

    try:
        print("\nTables:")
        print(connection.sql("SHOW TABLES"))

        row_count = connection.sql(
            "SELECT COUNT(*) AS row_count FROM users_data.users"
        ).fetchone()[0]

        print(f"Rows in users_data.users: {row_count}")

        if row_count != expected_count:
            raise AssertionError(
                f"Expected {expected_count} rows, but found {row_count}"
            )

        print("\nSample data:")
        print(connection.sql("SELECT * FROM users_data.users ORDER BY id"))
        print("\nVerification passed.")
    finally:
        connection.close()


def main():
    load_users()
    verify_users()


if __name__ == "__main__":
    main()
