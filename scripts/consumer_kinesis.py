

import json
import time
from typing import Any

import boto3


STREAM_NAME = "fleetpulse-telemetry-stream"
REGION_NAME = "us-east-1"


def read_from_kinesis(limit: int = 20, wait_seconds: int = 2) -> None:
    """
    Reads a small batch of records from Amazon Kinesis and prints them.

    limit: maximum number of records to print
    wait_seconds: short wait before reading so Kinesis has time to make records available
    """

    kinesis = boto3.client("kinesis", region_name=REGION_NAME)

    stream_description = kinesis.describe_stream(StreamName=STREAM_NAME)
    shard_id = stream_description["StreamDescription"]["Shards"][0]["ShardId"]

    print(f"Reading from stream: {STREAM_NAME}")
    print(f"Using shard: {shard_id}")
    print(f"Waiting {wait_seconds} seconds before reading...\n")
    time.sleep(wait_seconds)

    shard_iterator_response = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON",
    )
    shard_iterator = shard_iterator_response["ShardIterator"]

    records_read = 0

    while records_read < limit and shard_iterator:
        records_response: dict[str, Any] = kinesis.get_records(
            ShardIterator=shard_iterator,
            Limit=limit - records_read,
        )

        records = records_response.get("Records", [])
        shard_iterator = records_response.get("NextShardIterator")

        if not records:
            print("No records available yet. Waiting 1 second...")
            time.sleep(1)
            continue

        for record in records:
            payload = json.loads(record["Data"].decode("utf-8"))
            records_read += 1

            print(
                f"Record {records_read}: "
                f"vehicle={payload.get('vehicle_id')} "
                f"timestamp={payload.get('timestamp')} "
                f"speed={payload.get('speed_kmh')} "
                f"battery={payload.get('battery_pct')} "
                f"temp={payload.get('motor_temp_c')} "
                f"fault={payload.get('fault_code')}"
            )

            if records_read >= limit:
                break

    print(f"\nDone. Read {records_read} records from {STREAM_NAME}.")


if __name__ == "__main__":
    read_from_kinesis(limit=20, wait_seconds=2)