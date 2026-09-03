#!/usr/bin/env python3
"""FleetPulse synthetic EV fleet telemetry generator.

Simulates a fleet of electric vehicles emitting telemetry over a full
operating shift:
    vehicle_id, timestamp, speed_kmh, battery_pct, motor_temp_c,
    lat, lon, fault_code

Each vehicle is a stateful agent cycling through IDLE / DRIVING / CHARGING
phases, with physically plausible dynamics:
  - speed follows a bounded random walk while driving
  - battery drains as a function of speed and recharges when the vehicle
    docks, so a full shift takes vehicles through real discharge cycles
  - motor temperature relaxes towards a speed-dependent equilibrium; a
    subset of vehicles suffer degraded cooling and genuinely overheat
  - GPS position integrates heading + speed around the Melbourne metro area

Fault codes are causally derived from vehicle state, not sprinkled at
random: F-BAT-LOW only fires on a genuinely depleted battery, F-MOT-TEMP
only on a genuinely hot motor, F-CHG-INTERLOCK only while charging. This
matters because the downstream pipeline is judged on whether it can tell a
real asset fault from bad sensor data — if the fault codes did not track
the telemetry, that question would be meaningless.

Realistic data-quality "dirt" is injected on purpose so the downstream
validation stage has something to catch:
  - missing fields (sensor dropout)
  - signal gaps (tunnels / dead zones: whole records skipped)
  - outlier spikes (e.g. motor_temp 200+ degC, negative speed)
  - duplicated records (at-least-once delivery semantics)
  - malformed numeric strings

Usage:
    python scripts/generate_telemetry.py \
        --vehicles 50 --duration-min 480 --hz 0.2 --seed 42 --out-dir data

Output:
    data/telemetry.csv            full dataset, time-ordered
    data/fleet_metadata.csv       per-vehicle static attributes
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

# Transient faults that are not derived from a telemetry threshold.
TRANSIENT_FAULTS = [
    ("F-TPMS-01", 30),       # tyre pressure
    ("F-BRAKE-WEAR", 25),    # brake pad wear sensor
    ("F-INV-COMM", 20),      # inverter communication glitch
    ("F-GPS-DRIFT", 15),     # GPS accuracy degraded
    ("F-COOL-PUMP", 10),     # coolant pump underperforming
]

VEHICLE_MODELS = [
    ("AEV-Blanc-100", 0.55),   # cargo robot platform
    ("AEV-Blanc-300", 0.30),   # passenger shuttle
    ("AEV-Utility-X", 0.15),   # utility / maintenance unit
]

IDLE, DRIVING, CHARGING = "IDLE", "DRIVING", "CHARGING"

# Operational thresholds - the same numbers the alerting layer uses.
TEMP_WARN_C = 95.0
TEMP_CRIT_C = 110.0
BATT_WARN_PCT = 20.0
BATT_CRIT_PCT = 10.0


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
        self.battery = rng.uniform(70.0, 100.0)
        self.ambient = rng.uniform(12.0, 24.0)
        self.motor_temp = self.ambient + rng.uniform(2.0, 10.0)
        self.state = IDLE
        self.state_secs_left = rng.uniform(60, 900)
        self.gap_secs_left = 0.0  # >0 means in a signal dead zone

        # A minority of the fleet has a degrading cooling circuit. These are
        # the units that will genuinely overheat under sustained load - the
        # signal the maintenance team actually wants surfaced.
        self.cooling_health = 1.0
        self.cooling_fault_prone = rng.random() < 0.18

        self.active_fault = ""
        self.fault_secs_left = 0.0

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
        if self.battery < 15.0:
            self.state = CHARGING
            self.state_secs_left = rng.uniform(1800, 5400)
        elif self.state == DRIVING:
            self.state = IDLE
            self.state_secs_left = rng.uniform(60, 600)
        elif self.state == CHARGING:
            self.state = IDLE
            self.state_secs_left = rng.uniform(60, 300)
        else:
            if rng.random() < 0.80:
                self.state = DRIVING
                self.state_secs_left = rng.uniform(600, 3600)
                self.heading = rng.uniform(0, 2 * math.pi)
            else:
                self.state = IDLE
                self.state_secs_left = rng.uniform(60, 600)

    def _equilibrium_temp(self) -> float:
        """Steady-state motor temperature for the current load.

        A healthy unit at 60 km/h settles around 70-80 degC. As the cooling
        circuit degrades (cooling_health -> 0.5) the same load pushes the
        equilibrium past the 110 degC critical threshold.
        """
        load_rise = 0.95 * self.speed / max(self.cooling_health, 0.35)
        return self.ambient + load_rise

    def tick(self, dt: float):
        """Advance the simulation by `dt` seconds of vehicle time."""
        rng = self.rng
        self.state_secs_left -= dt
        if self.state_secs_left <= 0:
            self._transition()

        if self.state == DRIVING:
            target = 38.0 if "Blanc-100" in self.model else 52.0
            self.speed += rng.gauss(0.9 * (target - self.speed) / target, 1.6) * dt
            self.speed = max(0.0, min(self.speed, 92.0))
            # ~0.50 %/min at 50 km/h -> a full shift is a real discharge cycle
            self.battery -= ((self.speed / 50.0) * 0.0050 + 0.0004) * dt

            # Sustained load slowly degrades a susceptible cooling circuit.
            if self.cooling_fault_prone and self.speed > 25.0:
                self.cooling_health = max(0.45, self.cooling_health - 0.000045 * dt)

            # First-order thermal relaxation towards the load equilibrium.
            tau_heat = 420.0  # seconds
            self.motor_temp += (
                (self._equilibrium_temp() - self.motor_temp) * (dt / tau_heat)
                + rng.gauss(0, 0.15)
            )

            # integrate position; gentle heading wander
            self.heading += rng.gauss(0, 0.06) * math.sqrt(dt)
            dist_deg = (self.speed / 3.6) * dt / 111_000  # m -> degrees
            self.lat += dist_deg * math.cos(self.heading)
            self.lon += dist_deg * math.sin(self.heading) / math.cos(
                math.radians(self.lat)
            )
        elif self.state == CHARGING:
            self.speed = 0.0
            self.battery = min(100.0, self.battery + 0.028 * dt)
            self.motor_temp += (self.ambient - self.motor_temp) * (dt / 600.0)
            # Sitting on the charger lets the cooling circuit recover a little.
            self.cooling_health = min(1.0, self.cooling_health + 0.00002 * dt)
            if self.battery >= rng.uniform(88.0, 100.0):
                self.state_secs_left = 0
        else:  # IDLE
            self.speed = max(0.0, self.speed - rng.uniform(1.5, 4.0) * dt)
            self.battery -= 0.00012 * dt
            self.motor_temp += (self.ambient - self.motor_temp) * (dt / 450.0)

        self.battery = max(0.0, self.battery)
        self.motor_temp = max(self.ambient - 2.0, min(self.motor_temp, 145.0))

    def current_fault(self, dt: float) -> str:
        """Fault code derived from the vehicle's actual condition.

        Threshold faults are latched for a short window so they appear as
        sustained events rather than single-record flickers - which is how a
        real telematics unit reports them, and what makes response-time
        analysis on this data meaningful.
        """
        rng = self.rng
        if self.fault_secs_left > 0:
            self.fault_secs_left -= dt
            return self.active_fault

        if self.battery < BATT_CRIT_PCT:
            self.active_fault, self.fault_secs_left = "F-BAT-LOW", rng.uniform(60, 600)
            return self.active_fault
        if self.motor_temp > TEMP_WARN_C:
            self.active_fault, self.fault_secs_left = "F-MOT-TEMP", rng.uniform(60, 600)
            return self.active_fault
        if self.state == CHARGING and rng.random() < 0.00004 * dt:
            self.active_fault = "F-CHG-INTERLOCK"
            self.fault_secs_left = rng.uniform(30, 240)
            return self.active_fault
        if rng.random() < 0.00006 * dt:  # rare transient, unrelated to thresholds
            codes, weights = zip(*TRANSIENT_FAULTS)
            self.active_fault = rng.choices(codes, weights=weights)[0]
            self.fault_secs_left = rng.uniform(30, 300)
            return self.active_fault

        self.active_fault = ""
        return ""


def make_record(v: Vehicle, ts: datetime, dt: float) -> dict:
    return {
        "vehicle_id": v.vehicle_id,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "speed_kmh": round(v.speed, 1),
        "battery_pct": round(v.battery, 2),
        "motor_temp_c": round(v.motor_temp, 1),
        "lat": round(v.lat, 6),
        "lon": round(v.lon, 6),
        "fault_code": v.current_fault(dt),
    }


def inject_dirt(record: dict, rng: random.Random, stats: dict) -> dict:
    """Corrupt a small fraction of records to exercise data-quality checks."""
    r = rng.random()
    if r < 0.060:  # sensor dropout: blank 1-2 random measurement fields
        fields = rng.sample(
            ["speed_kmh", "battery_pct", "motor_temp_c", "lat", "lon"],
            k=rng.randint(1, 2),
        )
        for f in fields:
            record[f] = ""
        stats["missing_fields"] += 1
    elif r < 0.082:  # physically impossible outlier
        choice = rng.random()
        if choice < 0.4:
            record["motor_temp_c"] = round(rng.uniform(180.0, 260.0), 1)
        elif choice < 0.7:
            record["speed_kmh"] = round(rng.uniform(-80.0, -5.0), 1)
        else:
            record["battery_pct"] = round(rng.uniform(101.0, 140.0), 2)
        stats["outliers"] += 1
    elif r < 0.092:  # malformed numeric payload
        record["speed_kmh"] = rng.choice(["ERR", "NaN", "##", "-9999"])
        stats["malformed"] += 1
    return record


def main():
    p = argparse.ArgumentParser(description="Generate synthetic EV fleet telemetry")
    p.add_argument("--vehicles", type=int, default=50)
    p.add_argument("--duration-min", type=int, default=480, help="simulated minutes")
    p.add_argument("--hz", type=float, default=0.2, help="records per vehicle per second")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=Path, default=Path("data"))
    p.add_argument(
        "--start",
        default="2026-06-11T06:00:00Z",
        help="simulation start timestamp (ISO 8601, UTC)",
    )
    args = p.parse_args()

    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    ticks = int(args.duration_min * 60 * args.hz)
    dt = 1.0 / args.hz  # seconds of vehicle time per tick
    step = timedelta(seconds=dt)

    vehicles = [
        Vehicle(i + 1, random.Random(rng.getrandbits(64))) for i in range(args.vehicles)
    ]

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
        "overheat_rows": 0,
        "critical_battery_rows": 0,
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
                v.tick(dt)
                # signal gap: vehicle in a tunnel / dead zone emits nothing
                if v.gap_secs_left > 0:
                    v.gap_secs_left -= dt
                    stats["signal_gap_rows_dropped"] += 1
                    continue
                if v.state == DRIVING and v.rng.random() < 0.0025:
                    v.gap_secs_left = v.rng.uniform(60, 600)

                if v.motor_temp > TEMP_CRIT_C:
                    stats["overheat_rows"] += 1
                if v.battery < BATT_CRIT_PCT:
                    stats["critical_battery_rows"] += 1

                record = inject_dirt(make_record(v, ts, dt), v.rng, stats)
                if record["fault_code"]:
                    stats["fault_rows"] += 1
                writer.writerow(record)
                stats["rows_written"] += 1
                # at-least-once delivery: occasional duplicate record
                if v.rng.random() < 0.008:
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
            capacity = {
                "AEV-Blanc-100": 48, "AEV-Blanc-300": 75, "AEV-Utility-X": 60
            }[v.model]
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
