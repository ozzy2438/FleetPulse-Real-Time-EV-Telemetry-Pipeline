import csv
import json
import time
import boto3
from pathlib import Path


STREAM_NAME = "fleetpulse-telemetry-stream"
REGION_NAME = "us-east-1"
CSV_PATH = Path("data/telemetry.csv")


def send_to_kinesis(limit: int = 20, delay_seconds: float = 0.2) -> None:
    """
    Reads telemetry rows from CSV and sends them to Amazon Kinesis.

    limit: how many rows to send for the first test
    delay_seconds: small pause to simulate live streaming
    """

    kinesis = boto3.client("kinesis", region_name=REGION_NAME)

    sent_count = 0

    with CSV_PATH.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            if sent_count >= limit:
                break

            # Convert row to JSON bytes for Kinesis
            payload = json.dumps(row).encode("utf-8")

            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=payload,
                PartitionKey=row["vehicle_id"],
            )

            sent_count += 1

            print(
                f"Sent {sent_count}: vehicle={row['vehicle_id']} "
                f"shard={response['ShardId']} "
                f"sequence={response['SequenceNumber']}"
            )

            time.sleep(delay_seconds)

    print(f"\nDone. Sent {sent_count} records to {STREAM_NAME}.")


if __name__ == "__main__":
    send_to_kinesis(limit=20, delay_seconds=0.2)