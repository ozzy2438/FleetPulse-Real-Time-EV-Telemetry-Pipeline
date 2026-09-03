#!/usr/bin/env python3
"""FleetPulse telemetry validation and fleet-health classification.

This is the reference implementation of the logic that runs inside the
Kinesis-triggered Lambda. Keeping it as a standalone, runnable script means
every number quoted in the README can be reproduced from the committed
dataset without deploying any AWS infrastructure.

Two jobs:

1. Data quality. Every record is checked against schema, type, range and
   duplicate rules. Records that pass go to the trusted zone; records that
   fail go to a quarantine zone WITH the reason attached, rather than being
   dropped. Quarantine matters because a malformed record is itself a
   signal - a sensor that keeps emitting nulls is a maintenance job, not
   noise to be discarded.

2. Fleet health. Trusted records are classified HEALTHY / WATCH / CRITICAL
   against the operating thresholds, so the operations team gets a
   prioritised view instead of a raw feed.

Usage:
    python scripts/validate_telemetry.py \
        --input data/telemetry.csv --out-dir data
"""

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REQUIRED_FIELDS = [
    "vehicle_id", "timestamp", "speed_kmh", "battery_pct",
    "motor_temp_c", "lat", "lon",
]

# Physically plausible operating envelope. Anything outside is a sensor
# fault, not a vehicle in an extreme state.
VALID_RANGES = {
    "speed_kmh": (0.0, 200.0),
    "battery_pct": (0.0, 100.0),
    "motor_temp_c": (-40.0, 150.0),
    "lat": (-45.0, -33.0),   # Victoria
    "lon": (140.0, 150.0),
}

TEMP_WARN_C, TEMP_CRIT_C = 95.0, 110.0
BATT_WARN_PCT, BATT_CRIT_PCT = 20.0, 10.0


def validate(row: dict, seen_keys: set) -> tuple[bool, str]:
    """Return (is_valid, reject_reason). Reason is '' when valid."""
    key = (row.get("vehicle_id"), row.get("timestamp"))
    if key in seen_keys:
        return False, "DUPLICATE_RECORD"
    seen_keys.add(key)

    for field in REQUIRED_FIELDS:
        if row.get(field, "") == "":
            return False, "MISSING_FIELD"

    for field, (low, high) in VALID_RANGES.items():
        try:
            value = float(row[field])
        except (TypeError, ValueError):
            return False, "TYPE_ERROR"
        if not low <= value <= high:
            return False, "OUT_OF_RANGE"

    return True, ""


def classify(row: dict) -> str:
    """Prioritise a trusted record for the operations team."""
    temp = float(row["motor_temp_c"])
    battery = float(row["battery_pct"])
    if temp > TEMP_CRIT_C or battery < BATT_CRIT_PCT:
        return "CRITICAL"
    if temp > TEMP_WARN_C or battery < BATT_WARN_PCT:
        return "WATCH"
    return "HEALTHY"


def main():
    p = argparse.ArgumentParser(description="Validate FleetPulse telemetry")
    p.add_argument("--input", type=Path, default=Path("data/telemetry.csv"))
    p.add_argument("--out-dir", type=Path, default=Path("data"))
    p.add_argument(
        "--write-zones",
        action="store_true",
        help="also write trusted/ and quarantine/ CSV extracts",
    )
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    seen_keys: set = set()
    reject_reasons = Counter()
    health = Counter()
    fault_codes = Counter()
    per_vehicle_critical = Counter()
    total = valid = 0

    trusted_writer = quarantine_writer = None
    trusted_file = quarantine_file = None
    if args.write_zones:
        trusted_file = (args.out_dir / "trusted_telemetry.csv").open("w", newline="")
        quarantine_file = (args.out_dir / "quarantine.csv").open("w", newline="")

    with args.input.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            is_valid, reason = validate(row, seen_keys)

            if is_valid:
                valid += 1
                status = classify(row)
                health[status] += 1
                if status == "CRITICAL":
                    per_vehicle_critical[row["vehicle_id"]] += 1
                if row.get("fault_code"):
                    fault_codes[row["fault_code"]] += 1
                if trusted_file is not None:
                    if trusted_writer is None:
                        trusted_writer = csv.DictWriter(
                            trusted_file, fieldnames=list(row) + ["health_status"]
                        )
                        trusted_writer.writeheader()
                    trusted_writer.writerow({**row, "health_status": status})
            else:
                reject_reasons[reason] += 1
                if quarantine_file is not None:
                    if quarantine_writer is None:
                        quarantine_writer = csv.DictWriter(
                            quarantine_file, fieldnames=list(row) + ["reject_reason"]
                        )
                        quarantine_writer.writeheader()
                    quarantine_writer.writerow({**row, "reject_reason": reason})

    for handle in (trusted_file, quarantine_file):
        if handle is not None:
            handle.close()

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input": str(args.input),
        "records_processed": total,
        "records_trusted": valid,
        "records_quarantined": total - valid,
        "clean_data_rate_pct": round(100.0 * valid / total, 2) if total else 0.0,
        "quarantine_reasons": dict(reject_reasons.most_common()),
        "fleet_health": dict(health),
        "fleet_health_pct": {
            k: round(100.0 * v / valid, 2) for k, v in health.most_common()
        },
        "fault_codes": dict(fault_codes.most_common()),
        "top_critical_vehicles": dict(per_vehicle_critical.most_common(10)),
        "thresholds": {
            "motor_temp_warn_c": TEMP_WARN_C,
            "motor_temp_critical_c": TEMP_CRIT_C,
            "battery_warn_pct": BATT_WARN_PCT,
            "battery_critical_pct": BATT_CRIT_PCT,
        },
    }

    report_path = args.out_dir / "quality_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    print(f"\nReport written to {report_path}")


if __name__ == "__main__":
    main()
