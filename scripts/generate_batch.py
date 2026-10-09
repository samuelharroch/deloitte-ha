"""
Write a new batch to data/users.csv and data/addresses.csv, based on the current
state of the silver tables:
  - status flips for existing users
  - city moves for existing addresses
  - new users, each with one address

City moves stay among the minority cities and half of the new users go to the
majority city, so its ~50% share is preserved.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = "deloitte-ha"
LOCATION = "us-central1"
SILVER_DATASET = "silver"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

USER_COLUMNS = [
    "user_id", "first_name", "last_name", "email", "phone", "status", "created_at", "updated_at",
]
ADDRESS_COLUMNS = [
    "address_id", "user_id", "street", "city", "state", "postal_code", "country", "created_at", "updated_at",
]
EMAIL_DOMAINS = ["example.com", "gmail.com", "yahoo.com", "outlook.com", "mail.co.il"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a new users/addresses batch from silver.")
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--location", default=LOCATION)
    parser.add_argument("--silver-dataset", default=SILVER_DATASET)
    parser.add_argument("--updates", type=int, default=15, help="Existing users whose status flips")
    parser.add_argument("--moves", type=int, default=5, help="Existing addresses that move city")
    parser.add_argument("--new", type=int, default=10, help="New users, each with one address")
    parser.add_argument("--seed", type=int, default=None, help="Random seed, for a reproducible batch")
    return parser.parse_args(argv)


def fetch(bq: bigquery.Client, table: str) -> list[dict]:
    rows = bq.query(f"SELECT * EXCEPT (ingestion_time) FROM `{table}`").result()
    return [dict(row) for row in rows]


def random_phone(rng: random.Random) -> str:
    return f"+972-5{rng.randint(0, 9)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {k: v.strftime(TS_FORMAT) if isinstance(v, datetime) else v for k, v in row.items()}
            )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

    bq = bigquery.Client(project=args.project, location=args.location)
    dataset = f"{args.project}.{args.silver_dataset}"
    users = fetch(bq, f"{dataset}.users")
    addresses = fetch(bq, f"{dataset}.addresses")

    # City -> (state, postal code prefix), and the city sizes, taken from the current data.
    cities: dict[str, tuple[str, str]] = {}
    city_counts: dict[str, int] = {}
    for a in addresses:
        cities.setdefault(a["city"], (a["state"], a["postal_code"][:2]))
        city_counts[a["city"]] = city_counts.get(a["city"], 0) + 1
    majority = max(city_counts, key=city_counts.get)
    minority = [c for c in cities if c != majority]

    first_names = sorted({u["first_name"] for u in users})
    last_names = sorted({u["last_name"] for u in users})
    street_names = sorted({a["street"].split(" ", 1)[1] for a in addresses})

    def new_address_fields(city: str) -> dict:
        state, prefix = cities[city]
        return {
            "street": f"{rng.randint(1, 250)} {rng.choice(street_names)}",
            "city": city,
            "state": state,
            "postal_code": f"{prefix}{rng.randint(0, 9999):04d}",
        }

    user_rows: list[dict] = []
    address_rows: list[dict] = []

    for u in rng.sample(users, args.updates):
        u["status"] = "inactive" if u["status"] == "active" else "active"
        u["updated_at"] = now
        user_rows.append(u)

    movable = [a for a in addresses if a["city"] != majority]
    for a in rng.sample(movable, args.moves):
        a.update(new_address_fields(rng.choice([c for c in minority if c != a["city"]])))
        a["updated_at"] = now
        address_rows.append(a)

    next_user_id = max(u["user_id"] for u in users) + 1
    next_address_id = max(a["address_id"] for a in addresses) + 1
    for i in range(args.new):
        user_id, address_id = next_user_id + i, next_address_id + i
        first, last = rng.choice(first_names), rng.choice(last_names)
        user_rows.append({
            "user_id": user_id,
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{user_id}@{rng.choice(EMAIL_DOMAINS)}",
            "phone": random_phone(rng),
            "status": "active",
            "created_at": now,
            "updated_at": now,
        })
        city = majority if i % 2 == 0 else rng.choice(minority)
        address_rows.append({
            "address_id": address_id,
            "user_id": user_id,
            **new_address_fields(city),
            "country": "Israel",
            "created_at": now,
            "updated_at": now,
        })

    write_csv(DATA_DIR / "users.csv", USER_COLUMNS, user_rows)
    write_csv(DATA_DIR / "addresses.csv", ADDRESS_COLUMNS, address_rows)

    print(f"Silver now: {len(users)} users, {len(addresses)} addresses ({majority}: {city_counts[majority]})")
    print(f"users.csv:     {args.updates} status flips + {args.new} new = {len(user_rows)} rows")
    print(f"addresses.csv: {args.moves} city moves + {args.new} new = {len(address_rows)} rows")


if __name__ == "__main__":
    main()
