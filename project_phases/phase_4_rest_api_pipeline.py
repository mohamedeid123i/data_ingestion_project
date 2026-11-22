import duckdb
import dlt
import requests

from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.rest_api import RESTClient


API_URL = "https://dummyjson.com/products"
PAGE_SIZE = 10
USERS_API_URL = "https://dummyjson.com/users"
POSTS_API_URL = "https://dummyjson.com/posts"


@dlt.resource(
    name="products",
    primary_key="id",
    write_disposition="replace",
)
def products_resource():
    """Fetch and yield DummyJSON products one page at a time."""
    client = RESTClient(
        base_url="https://dummyjson.com",
        paginator=OffsetPaginator(
            limit=PAGE_SIZE,
            offset=0,
            offset_param="skip",
            limit_param="limit",
            total_path="total",
            stop_after_empty_page=True,
        ),
        data_selector="products",
    )

    for page in client.paginate("/products"):
        yield list(page)


@dlt.resource(
    name="users",
    primary_key="id",
    write_disposition="replace",
)
def users_resource():
    """Fetch and yield DummyJSON users one page at a time."""
    client = RESTClient(
        base_url="https://dummyjson.com",
        paginator=OffsetPaginator(
            limit=PAGE_SIZE,
            offset=0,
            offset_param="skip",
            limit_param="limit",
            total_path="total",
            stop_after_empty_page=True,
        ),
        data_selector="users",
    )

    for page in client.paginate("/users"):
        yield list(page)


@dlt.resource(
    name="posts",
    primary_key="id",
    write_disposition="replace",
)
def posts_resource():
    """Fetch and yield DummyJSON posts one page at a time."""
    client = RESTClient(
        base_url="https://dummyjson.com",
        paginator=OffsetPaginator(
            limit=PAGE_SIZE,
            offset=0,
            offset_param="skip",
            limit_param="limit",
            total_path="total",
            stop_after_empty_page=True,
        ),
        data_selector="posts",
    )

    for page in client.paginate("/posts"):
        yield list(page)


def load_products():
    """Load DummyJSON products, users, and posts into DuckDB."""
    pipeline = dlt.pipeline(
        pipeline_name="products_pipeline",
        destination="duckdb",
        dataset_name="dummyjson_data",
    )

    load_info = pipeline.run(
        [products_resource, users_resource, posts_resource],
        write_disposition="replace",
    )

    print("Load completed:")
    print(load_info)


def fetch_api_total(api_url):
    """Get the API-reported total and fail clearly on HTTP/API errors."""
    response = requests.get(api_url, params={"limit": 1, "skip": 0}, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if "total" not in payload:
        raise AssertionError("DummyJSON response did not contain 'total'")

    return int(payload["total"])


def verify_products():
    """Verify API totals, table uniqueness, and sample SQL queries."""
    connection = duckdb.connect("products_pipeline.duckdb", read_only=True)

    try:
        print("\nTables:")
        print(connection.sql("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """))

        resources = (
            ("products", API_URL, "Products"),
            ("users", USERS_API_URL, "Users"),
            ("posts", POSTS_API_URL, "Posts"),
        )

        for table_name, api_url, label in resources:
            expected_total = fetch_api_total(api_url)
            row_count = connection.sql(
                f"SELECT COUNT(*) FROM dummyjson_data.{table_name}"
            ).fetchone()[0]
            unique_ids = connection.sql(
                f"SELECT COUNT(DISTINCT id) FROM dummyjson_data.{table_name}"
            ).fetchone()[0]

            print(f"\n{label}:")
            print(f"API total: {expected_total}")
            print(f"Rows: {row_count}")
            print(f"Unique ids: {unique_ids}")

            if row_count != expected_total:
                raise AssertionError(
                    f"Expected {expected_total} {table_name}, but found {row_count}"
                )
            if unique_ids != row_count:
                raise AssertionError(
                    f"Expected {row_count} unique ids, but found {unique_ids}"
                )

        print("\nProducts sample:")
        print(connection.sql("""
            SELECT id, title, category, price, brand, rating, stock
            FROM dummyjson_data.products
            ORDER BY id
            LIMIT 10
        """))
        print("\nUsers sample:")
        print(connection.sql("""
            SELECT *
            FROM dummyjson_data.users
            ORDER BY id
            LIMIT 10
        """))
        print("\nPosts sample:")
        print(connection.sql("""
            SELECT id, title, user_id, views, reactions__likes
            FROM dummyjson_data.posts
            ORDER BY id
            LIMIT 10
        """))
        print("\nVerification passed.")
    finally:
        connection.close()


def main():
    load_products()
    verify_products()


if __name__ == "__main__":
    main()
