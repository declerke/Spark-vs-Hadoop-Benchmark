#!/usr/bin/env python3
import os
import sys
import csv
import random
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PAYSIM_EXPECTED_COLS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud"
]

BANKSIM_TX_EXPECTED_COLS = [
    "step", "customer", "age", "gender", "zipcodeOri",
    "merchant", "zipMerchant", "category", "amount", "fraud"
]

BANKSIM_NET_EXPECTED_COLS = ["Source", "Target", "Weight", "typeTrans", "fraud"]


def validate_csv(filepath: str, expected_cols: list, name: str) -> dict:
    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        return {"valid": False, "reason": "File not found"}

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    row_count = 0
    fraud_count = 0
    amount_sum = 0.0
    parse_errors = 0

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = None
        try:
            header = next(reader)
        except StopIteration:
            return {"valid": False, "reason": "Empty file"}

        header_clean = [h.strip().strip("'") for h in header]

        if len(header_clean) != len(expected_cols):
            log.warning(
                f"{name}: Column count mismatch. Got {len(header_clean)}, "
                f"expected {len(expected_cols)}"
            )

        fraud_col_idx = None
        amount_col_idx = None
        for i, col in enumerate(header_clean):
            if col.lower() in ("fraud", "isfraud"):
                fraud_col_idx = i
            if col.lower() == "amount":
                amount_col_idx = i

        for row in reader:
            row_count += 1
            try:
                if fraud_col_idx is not None and len(row) > fraud_col_idx:
                    val = row[fraud_col_idx].strip().strip("'")
                    if val in ("1", "1.0"):
                        fraud_count += 1
                if amount_col_idx is not None and len(row) > amount_col_idx:
                    amt_str = row[amount_col_idx].strip().strip("'")
                    amount_sum += float(amt_str)
            except (ValueError, IndexError):
                parse_errors += 1

    fraud_rate = (fraud_count / row_count * 100) if row_count > 0 else 0.0
    avg_amount = (amount_sum / row_count) if row_count > 0 else 0.0

    log.info(
        f"{name}: {row_count:,} rows | {size_mb:.1f} MB | "
        f"fraud rate={fraud_rate:.2f}% | avg_amount={avg_amount:.2f} | "
        f"parse_errors={parse_errors}"
    )

    return {
        "valid": True,
        "path": filepath,
        "size_mb": size_mb,
        "row_count": row_count,
        "fraud_count": fraud_count,
        "fraud_rate_pct": fraud_rate,
        "avg_amount": avg_amount,
        "parse_errors": parse_errors
    }


def create_sample(input_path: str, output_path: str, fraction: float, seed: int = 42):
    random.seed(seed)
    sampled = 0
    total = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8", newline="") as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            total += 1
            if random.random() < fraction:
                fout.write(line)
                sampled += 1

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    log.info(
        f"Sample created: {os.path.basename(output_path)} "
        f"({sampled:,}/{total:,} rows, {size_mb:.1f} MB, fraction={fraction})"
    )
    return sampled


def prepare_scaling_samples(data_dir: str, fractions: list = None):
    if fractions is None:
        fractions = [0.1, 0.25, 0.5]

    samples_dir = os.path.join(data_dir, "samples")
    os.makedirs(samples_dir, exist_ok=True)

    source_files = {
        "paysim": os.path.join(data_dir, "paysim_transactions.csv"),
        "banksim_tx": os.path.join(data_dir, "banksim_transactions.csv"),
        "banksim_net": os.path.join(data_dir, "banksim_network.csv"),
    }

    for frac in fractions:
        frac_pct = int(frac * 100)
        for name, path in source_files.items():
            if not os.path.exists(path):
                log.warning(f"Skipping sample {name} (file not found: {path})")
                continue
            out_path = os.path.join(samples_dir, f"{name}_{frac_pct}pct.csv")
            if os.path.exists(out_path):
                log.info(f"Sample already exists: {out_path}")
                continue
            create_sample(path, out_path, frac)


def rename_datasets(data_dir: str):
    rename_map = {
        "PS_20174392719_1491204439457_log.csv": "paysim_transactions.csv",
        "bs140513_032310.csv": "banksim_transactions.csv",
        "bsNET140513_032310.csv": "banksim_network.csv",
    }

    for src_name, dst_name in rename_map.items():
        src = os.path.join(data_dir, src_name)
        dst = os.path.join(data_dir, dst_name)
        if os.path.exists(src) and not os.path.exists(dst):
            os.rename(src, dst)
            log.info(f"Renamed: {src_name} -> {dst_name}")
        elif os.path.exists(dst):
            log.info(f"Already exists: {dst_name}")
        else:
            log.warning(f"Source not found: {src_name}")


def main():
    parser = argparse.ArgumentParser(description="Prepare benchmark datasets")
    parser.add_argument("--data-dir", default="/benchmark/data", help="Data directory")
    parser.add_argument("--validate", action="store_true", help="Validate existing CSV files")
    parser.add_argument("--rename", action="store_true", help="Rename datasets to standard names")
    parser.add_argument(
        "--samples", action="store_true",
        help="Create fractional samples for scaling benchmarks"
    )
    parser.add_argument(
        "--fractions", nargs="+", type=float, default=[0.1, 0.25, 0.5],
        help="Sample fractions to create"
    )
    args = parser.parse_args()

    if args.rename:
        rename_datasets(args.data_dir)

    if args.validate:
        files_to_check = [
            (os.path.join(args.data_dir, "paysim_transactions.csv"), PAYSIM_EXPECTED_COLS, "PaySim"),
            (os.path.join(args.data_dir, "banksim_transactions.csv"), BANKSIM_TX_EXPECTED_COLS, "BankSim Transactions"),
            (os.path.join(args.data_dir, "banksim_network.csv"), BANKSIM_NET_EXPECTED_COLS, "BankSim Network"),
        ]
        all_valid = True
        for filepath, cols, name in files_to_check:
            result = validate_csv(filepath, cols, name)
            if not result.get("valid"):
                all_valid = False
        if not all_valid:
            log.error("One or more files failed validation.")
            sys.exit(1)
        else:
            log.info("All files validated successfully.")

    if args.samples:
        prepare_scaling_samples(args.data_dir, args.fractions)
        log.info("Sample preparation complete.")


if __name__ == "__main__":
    main()