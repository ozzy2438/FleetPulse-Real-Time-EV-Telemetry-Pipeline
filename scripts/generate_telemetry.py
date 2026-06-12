#!/usr/bin/env python3
"""FleetPulse synthetic EV fleet telemetry generator.

Simulates a fleet of electric vehicles emitting telemetry at 1 Hz:
    vehicle_id, timestamp, speed_kmh, battery_pct, motor_temp_c,
    lat, lon, fault_code

Each vehicle is a stateful agent cycling through IDLE / DRIVING / CHARGING
phases, with physically plausible dynamics:
  - speed follows a bounded random walk while driving
  - battery drains as a function of speed (plus idle drain), recharges
    when the vehicle docks at a charging state
  - motor temperature rises with sustained speed and cools when idle
  - GPS position integrates heading + speed around the Melbourne metro area

Realistic data-quality "dirt" is injected on purpose so the downstream
pipeline (Lambda validation / quarantine) has something to catch:
  - missing fields (sensor dropout)
  - signal gaps (tunnels / dead zones: whole records skipped)
  - outlier spikes (e.g. motor_temp 200+ degC, negative speed)
  - duplicated records (at-least-once delivery semantics)
  - malformed numeric strings

Usage:
    python scripts/generate_telemetry.py \
        --vehicles 50 --duration-min 90 --seed 42 --out-dir data

Output:
    data/telemetry.csv          full dataset, time-ordered
    data/fleet_metadata.csv     per-vehicle static attributes
    data/generation_summary.json  row counts, dirt stats, parameters
"""

import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Melbourne CBD - the fleet's home depot area
DEPOT_LAT = -37.8136
DEPOT_LON = 144.9631

FAULT_CODES = [
    # (code, weight) - higher weight = more frequent
    ("F-BAT-LOW", 30),       # battery below warning threshold
    ("F-MOT-TEMP", 25),      # motor temperature above warning threshold
    ("F-BRAKE-WEAR", 15),    # brake pad wear sensor
    ("F-TPMS-01", 12),       # tyre pressure
    ("F-INV-COMM", 8),       # inverter communication glitch
    ("F-GPS-DRIFT", 6),      # GPS accuracy degraded
    ("F-CHG-INTERLOCK", 4),  # charge port interlock fault
]

VEHICLE_MODELS = [
    ("AEV-Blanc-100", 0.55),   # cargo robot platform
    ("AEV-Blanc-300", 0.30),   # passenger shuttle
    ("AEV-Utility-X", 0.15),   # utility / maintenance unit
]

IDLE, DRIVING, CHARGING = "IDLE", "DRIVING", "CHARGING"


class Vehicle:
    """Stateful per-vehicle simulator producing one record per tick."""

    def __init__(self, index: int, rng: random.Random):
        self.rng = rng
        self.vehicle_id = f"FP-{index:04d}"
        self.model = self._pick_model()
        # Vehicles start scattered within ~8 km of the depot
        self.lat = DEPOT_LAT + rng.uniform(-0.07, 0.07)
        self.lon = DEPOT_LON + rng.uniform(-0.09, 0.09)
        self.heading = rng.uniform(0, 2 * math.pi)
        self.speed = 0.0
        self.battery = rng.uniform(55.0, 100.0)
        self.motor_temp = rng.uniform(22.0, 35.0)
        self.ambient = rng.uniform(12.0, 24.0)
        self.state = IDLE
        self.state_ticks_left = rng.randint(10, 120)
        self.gap_ticks_left = 0  # >0 means in a signal dead zone
        self.active_fault = None
        self.fault_ticks_left = 0

    def _pick_model(self) -> str:
        r = self.rng.random()
        acc = 0.0
        for name, w in VEHICLE_MODELS:
            acc += w
            if r <= acc:
                return name
        return VEHICLE_MODELS[-1][0]

    def _transition(self):
        """Choose next operating state when the current phase ends."""
        rng = self.rng
        if self.battery < 12.0 and rng.random() < 0.85:
            self.state = CHARGING
            self.state_ticks_left = rng.randint(600, 1800)
        elif self.state == DRIVING:
            self.state = IDLE
            self.state_ticks_left = rng.randint(30, 300)
        else:
            if rng.random() < 0.75:
                self.state = DRIVING
                self.state_ticks_left = rng.randint(120, 900)
                self.heading = rng.uniform(0, 2 * math.pi)
            else:
                self.state = IDLE
                self.state_ticks_left = rng.randint(30, 300)

    def tick(self):
        """Advance the simulation by one second of vehicle time."""
        rng = self.rng
        self.state_ticks_left -= 1
        if self.state_ticks_left <= 0:
            self._transition()

        if self.state == DRIVING:
            target = 38.0 if "Blanc-100" in self.model else 52.0
            self.speed += rng.gauss(0.18 * (target - self.speed) / target, 1.6)
            self.speed = max(0.0, min(self.speed, 92.0))
            # ~0.16% battery per km at 50 km/h, scaled by speed
            self.battery -= (self.speed / 50.0) * 0.0022 + 0.0003
            self.motor_temp += 0.012 * (self.speed / 10.0) + rng.gauss(0, 0.05)
            # integrate position; gentle heading wander
            self.heading += rng.gauss(0, 0.06)
            dist_deg = (self.speed / 3.6) / 111_000  # m/s -> degrees
            self.lat += dist_deg * math.cos(self.heading)
            self.lon += dist_deg * math.sin(self.heading) / math.cos(
                math.radians(self.lat)
            )
        elif self.state == CHARGING:
            self.speed = 0.0
            self.battery = min(100.0, self.battery + 0.028)
            self.motor_temp += (self.ambient - self.motor_temp) * 0.004
            if self.battery >= rng.uniform(88.0, 100.0):
                self.state_ticks_left = 0
        else:  # IDLE
            self.speed = max(0.0, self.speed - rng.uniform(1.5, 4.0))
            self.battery -= 0.00008
            self.motor_temp += (self.ambient - self.motor_temp) * 0.006

        self.battery = max(0.0, self.battery)
        self.motor_temp = max(self.ambient - 2.0, min(self.motor_temp, 135.0))

    def current_fault(self) -> str:
        """Threshold-driven and random transient fault codes."""
        rng = self.rng
        if self.fault_ticks_left > 0:
            self.fault_ticks_left -= 1
            return self.active_fault
        if self.battery < 10.0 and rng.random() < 0.30:
            self.active_fault, self.fault_ticks_left = "F-BAT-LOW", rng.randint(5, 40)
            return self.active_fault
        if self.motor_temp > 95.0 and rng.random() < 0.25:
            self.active_fault, self.fault_ticks_left = "F-MOT-TEMP", rng.randint(5, 40)
            return self.active_fault
        if rng.random() < 0.0009:  # rare transient fault
            codes, weights = zip(*FAULT_CODES)
            self.active_fault = rng.choices(codes, weights=weights)[0]
            self.fault_ticks_left = rng.randint(3, 25)
            return self.active_fault
        return ""


def make_record(v: Vehicle, ts: datetime) -> dict:
    return {
        "vehicle_id": v.vehicle_id,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "speed_kmh": round(v.speed, 1),
        "battery_pct": round(v.battery, 2),
        "motor_temp_c": round(v.motor_temp, 1),
        "lat": round(v.lat, 6),
        "lon": round(v.lon, 6),
        "fault_code": v.current_fault(),
    }


def inject_dirt(record: dict, rng: random.Random, stats: dict) -> dict:
    """Corrupt a small fraction of records to exercise data-quality checks."""
    r = rng.random()
    if r < 0.010:  # sensor dropout: blank 1-2 random measurement fields
        fields = rng.sample(
            ["speed_kmh", "battery_pct", "motor_temp_c", "lat", "lon"],
            k=rng.randint(1, 2),
        )
        for f in fields:
            record[f] = ""
        stats["missing_fields"] += 1
    elif r < 0.013:  # physically impossible outlier
        choice = rng.random()
        if choice < 0.4:
            record["motor_temp_c"] = round(rng.uniform(180.0, 260.0), 1)
        elif choice < 0.7:
            record["speed_kmh"] = round(rng.uniform(-80.0, -5.0), 1)
        else:
            record["battery_pct"] = round(rng.uniform(101.0, 140.0), 2)
        stats["outliers"] += 1
    elif r < 0.015:  # malformed numeric payload
        record["speed_kmh"] = rng.choice(["ERR", "NaN", "##", "-9999"])
        stats["malformed"] += 1
    return record


def main():
    p = argparse.ArgumentParser(description="Generate synthetic EV fleet telemetry")
    p.add_argument("--vehicles", type=int, default=50)
    p.add_argument("--duration-min", type=int, default=90, help="simulated minutes")
    p.add_argument("--hz", type=float, default=1.0, help="records per vehicle per second")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=Path, default=Path("data"))
    p.add_argument(
        "--start",
        default="2026-06-11T00:00:00Z",
        help="simulation start timestamp (ISO 8601, UTC)",
    )
    args = p.parse_args()

    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    ticks = int(args.duration_min * 60 * args.hz)
    step = timedelta(seconds=1.0 / args.hz)

    vehicles = [Vehicle(i + 1, random.Random(rng.getrandbits(64))) for i in range(args.vehicles)]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    telemetry_path = args.out_dir / "telemetry.csv"
    metadata_path = args.out_dir / "fleet_metadata.csv"
    summary_path = args.out_dir / "generation_summary.json"

    stats = {
        "rows_written": 0,
        "missing_fields": 0,
        "outliers": 0,
        "malformed": 0,
        "duplicates": 0,
        "signal_gap_rows_dropped": 0,
        "fault_rows": 0,
    }

    fieldnames = [
        "vehicle_id", "timestamp", "speed_kmh", "battery_pct",
        "motor_temp_c", "lat", "lon", "fault_code",
    ]
    with telemetry_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        ts = start
        for _ in range(ticks):
            for v in vehicles:
                v.tick()
                # signal gap: vehicle in a tunnel / dead zone emits nothing
                if v.gap_ticks_left > 0:
                    v.gap_ticks_left -= 1
                    stats["signal_gap_rows_dropped"] += 1
                    continue
                if v.state == DRIVING and v.rng.random() < 0.0006:
                    v.gap_ticks_left = v.rng.randint(20, 180)

                record = inject_dirt(make_record(v, ts), v.rng, stats)
                if record["fault_code"]:
                    stats["fault_rows"] += 1
                writer.writerow(record)
                stats["rows_written"] += 1
                # at-least-once delivery: occasional duplicate record
                if v.rng.random() < 0.002:
                    writer.writerow(record)
                    stats["rows_written"] += 1
                    stats["duplicates"] += 1
            ts += step

    with metadata_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["vehicle_id", "model", "battery_capacity_kwh", "depot", "commissioned_date"]
        )
        meta_rng = random.Random(args.seed + 1)
        for v in vehicles:
            capacity = {"AEV-Blanc-100": 48, "AEV-Blanc-300": 75, "AEV-Utility-X": 60}[v.model]
            commissioned = start - timedelta(days=meta_rng.randint(60, 900))
            writer.writerow(
                [v.vehicle_id, v.model, capacity, "MEL-DEPOT-01",
                 commissioned.strftime("%Y-%m-%d")]
            )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "vehicles": args.vehicles,
            "duration_min": args.duration_min,
            "hz": args.hz,
            "seed": args.seed,
            "start": args.start,
        },
        "stats": stats,
        "files": {
            "telemetry": str(telemetry_path),
            "fleet_metadata": str(metadata_path),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")

    size_mb = telemetry_path.stat().st_size / 1_048_576
    print(f"Wrote {stats['rows_written']:,} rows to {telemetry_path} ({size_mb:.1f} MB)")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
