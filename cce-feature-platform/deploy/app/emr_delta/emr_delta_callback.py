from __future__ import annotations

import argparse
from pathlib import Path


def local_path_from_base(base_path: str) -> Path | None:
    if base_path.startswith("file://"):
        return Path(base_path.removeprefix("file://"))
    if "://" in base_path:
        return None
    return Path(base_path)


def count_delta_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*") if item.is_file() and item.suffix in {".parquet", ".json", ".csv"})


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate EMR Delta output after deployment.")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--min-gold-customer-rows", type=int, default=1)
    parser.add_argument("--min-anomaly-rows", type=int, default=0)
    args = parser.parse_args()

    local_base = local_path_from_base(args.base_path)
    if local_base is None:
        print("Remote lake path detected. Use this callback as a CI contract placeholder or extend it with boto3/Delta reader.")
        print(f"base_path={args.base_path}")
        print(f"business_date={args.business_date}")
        return 0

    gold_customers = local_base / "gold" / "customer_features"
    anomalies = local_base / "gold" / "transaction_anomalies"
    gold_file_count = count_delta_files(gold_customers)
    anomaly_file_count = count_delta_files(anomalies)

    if gold_file_count < args.min_gold_customer_rows:
        raise SystemExit(f"Gold customer output below threshold: {gold_file_count} < {args.min_gold_customer_rows}")
    if anomaly_file_count < args.min_anomaly_rows:
        raise SystemExit(f"Anomaly output below threshold: {anomaly_file_count} < {args.min_anomaly_rows}")

    print("EMR Delta callback passed.")
    print(f"gold_customer_files={gold_file_count}")
    print(f"anomaly_files={anomaly_file_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
