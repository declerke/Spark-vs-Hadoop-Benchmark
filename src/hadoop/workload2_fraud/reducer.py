#!/usr/bin/env python3
import sys
import math

VELOCITY_THRESHOLD = 5
AMOUNT_Z_THRESHOLD = 2.5

current_prefix = None
current_bucket = None
records = []

global_log_amounts = []
global_mean = 0.0
global_std = 1.0

def compute_global_stats(log_amounts):
    if not log_amounts:
        return 0.0, 1.0
    n = len(log_amounts)
    mean = sum(log_amounts) / n
    variance = sum((x - mean) ** 2 for x in log_amounts) / n
    std = math.sqrt(variance) if variance > 0 else 1.0
    return mean, std

def flag_velocity(records_in_bucket, threshold):
    count = len(records_in_bucket)
    return count >= threshold, count

def flag_amount(log_amount, mean, std, z_threshold):
    if std == 0:
        return False, 0.0
    z = (log_amount - mean) / std
    return z > z_threshold, z

# Collecting all lines because the logic requires global stats 
# before individual record flagging
all_lines = []
for line in sys.stdin:
    line = line.strip()
    if line:
        all_lines.append(line)

# First pass: Extract global log amounts for statistics
for line in all_lines:
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    prefix = parts[0]
    if prefix == "AMT":
        try:
            global_log_amounts.append(float(parts[2]))
        except ValueError:
            pass

global_mean, global_std = compute_global_stats(global_log_amounts)

# Second pass: Process Velocity buckets
bucket_records = {}
for line in all_lines:
    parts = line.split("\t")
    if len(parts) < 8:
        continue
    prefix = parts[0]
    if prefix != "VEL":
        continue

    bucket = parts[1]
    try:
        amount = float(parts[2])
    except ValueError:
        continue
    dataset = parts[3]
    account = parts[4]
    step = parts[5]
    tx_type = parts[6]
    is_fraud = parts[7]

    if bucket not in bucket_records:
        bucket_records[bucket] = []
    bucket_records[bucket].append({
        "amount": amount,
        "dataset": dataset,
        "account": account,
        "step": step,
        "tx_type": tx_type,
        "is_fraud": is_fraud
    })

# Final output: Apply thresholds and emit CSV format
for bucket, recs in bucket_records.items():
    vel_flag, vel_count = flag_velocity(recs, VELOCITY_THRESHOLD)
    for rec in recs:
        log_amt = math.log1p(rec["amount"]) if rec["amount"] > 0 else 0.0
        amt_flag, z_score = flag_amount(log_amt, global_mean, global_std, AMOUNT_Z_THRESHOLD)
        fraud_proxy = 1 if (vel_flag or amt_flag) else 0
        print(
            f"{rec['dataset']},{rec['account']},{rec['step']},{rec['tx_type']},"
            f"{rec['amount']:.2f},{vel_count},{z_score:.4f},{fraud_proxy},{rec['is_fraud']}"
        )
