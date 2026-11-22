import argparse
import json
import shutil
from pathlib import Path

import duckdb
import dlt


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENTS_JSON_PATH = PROJECT_ROOT / "data" / "events" / "events.json"
PIPELINES_DIR = PROJECT_ROOT / "project_phases" / ".dlt" / "pipelines"

PIPELINE_NAME = "phase_6_events_lag_pipeline"
DATASET_NAME = "events_data"
DATABASE_PATH = PROJECT_ROOT / "phase_6_events_lag_pipeline.duckdb"


@dlt.resource(
    name="events",
    primary_key="event_id",
    write_disposition="merge",
)
def events_resource(
    created_at=dlt.sources.incremental(
        "created_at",
        initial_value="2025-01-01T00:00:00Z",
        row_order="asc",
        lag=604800,
    ),
):
    """Read local events and merge records inside the 7-day lag window."""
    with EVENTS_JSON_PATH.open("r", encoding="utf-8") as file:
        events = json.load(file)

    print(f"Incremental cursor last_value: {created_at.last_value}")
    print(f"Incremental cursor range: {created_at.get_current_range()}")

    for event in sorted(events, key=lambda item: item["created_at"]):
        yield event


def build_pipeline():
    return dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(credentials=str(DATABASE_PATH)),
        dataset_name=DATASET_NAME,
        pipelines_dir=str(PIPELINES_DIR),
    )


def load_events():
    pipeline = build_pipeline()
    load_info = pipeline.run(events_resource)

    if load_info.has_failed_jobs:
        load_info.raise_on_failed_jobs()

    print("Load completed:")
    print(load_info)


def append_event(event):
    EVENTS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    if EVENTS_JSON_PATH.exists():
        with EVENTS_JSON_PATH.open("r", encoding="utf-8") as file:
            events = json.load(file)
    else:
        events = []

    existing_ids = {item["event_id"] for item in events}
    if event["event_id"] in existing_ids:
        raise ValueError(f"Event ID already exists: {event['event_id']}")

    events.append(event)
    events.sort(key=lambda item: item["created_at"])

    with EVENTS_JSON_PATH.open("w", encoding="utf-8") as file:
        json.dump(events, file, ensure_ascii=False, indent=2)
        file.write("\n")

    print(f"Added event {event['event_id']} with created_at={event['created_at']}")


def add_new_event():
    append_event(
        {
            "event_id": "evt-004",
            "created_at": "2025-04-10T10:15:00Z",
            "event_type": "checkout",
            "user_id": "user-103",
        }
    )


def add_old_event():
    append_event(
        {
            "event_id": "evt-005",
            "created_at": "2024-12-25T08:00:00Z",
            "event_type": "account_created",
            "user_id": "user-104",
        }
    )


def update_new_event():
    EVENTS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    with EVENTS_JSON_PATH.open("r", encoding="utf-8") as file:
        events = json.load(file)

    matching_events = [
        event for event in events
        if event["event_id"] == "evt-004"
    ]
    if not matching_events:
        raise ValueError("evt-004 does not exist. Run add-new first.")

    matching_events[0]["event_type"] = "payment_confirmed"
    matching_events[0]["user_id"] = "user-103-updated"

    with EVENTS_JSON_PATH.open("w", encoding="utf-8") as file:
        json.dump(events, file, ensure_ascii=False, indent=2)
        file.write("\n")

    print("Updated evt-004 inside the source file.")


def reset_experiment():
    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()

    pipeline_state_dir = PIPELINES_DIR / PIPELINE_NAME
    if pipeline_state_dir.exists():
        shutil.rmtree(pipeline_state_dir)

    baseline_events = [
        {
            "event_id": "evt-001",
            "created_at": "2025-01-10T09:00:00Z",
            "event_type": "login",
            "user_id": "user-101",
        },
        {
            "event_id": "evt-002",
            "created_at": "2025-02-15T12:30:00Z",
            "event_type": "purchase",
            "user_id": "user-102",
        },
        {
            "event_id": "evt-003",
            "created_at": "2025-03-20T18:45:00Z",
            "event_type": "logout",
            "user_id": "user-101",
        },
    ]
    EVENTS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS_JSON_PATH.open("w", encoding="utf-8") as file:
        json.dump(baseline_events, file, ensure_ascii=False, indent=2)
        file.write("\n")

    print("Reset completed: restored the 3 baseline events and cleared dlt state.")


def verify_events():
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found: {DATABASE_PATH}. Run the load first."
        )

    connection = duckdb.connect(str(DATABASE_PATH), read_only=True)
    try:
        row_count = connection.sql(
            f"SELECT COUNT(*) FROM {DATASET_NAME}.events"
        ).fetchone()[0]
        unique_ids = connection.sql(
            f"SELECT COUNT(DISTINCT event_id) FROM {DATASET_NAME}.events"
        ).fetchone()[0]
        duplicate_ids = connection.sql(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT event_id)
            FROM {DATASET_NAME}.events
            """
        ).fetchone()[0]

        print(f"\nLoaded rows: {row_count}")
        print(f"Unique event_id values: {unique_ids}")
        print(f"Duplicate event_id rows: {duplicate_ids}")
        print("\nEvents:")
        print(connection.sql(
            f"""
            SELECT event_id, created_at, event_type, user_id
            FROM {DATASET_NAME}.events
            ORDER BY created_at, event_id
            """
        ))
    finally:
        connection.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Phase 6: cursor-based incremental loading with local events JSON"
    )
    parser.add_argument(
        "action",
        nargs="?",
        choices=("load", "add-new", "update-new", "add-old", "verify", "reset"),
        default="load",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.action == "load":
        load_events()
        verify_events()
    elif args.action == "add-new":
        add_new_event()
    elif args.action == "update-new":
        update_new_event()
    elif args.action == "add-old":
        add_old_event()
    elif args.action == "verify":
        verify_events()
    elif args.action == "reset":
        reset_experiment()


if __name__ == "__main__":
    main()
