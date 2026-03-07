#!/usr/bin/env python3
import sys
import csv
import math

VELOCITY_THRESHOLD = 5
AMOUNT_MULTIPLIER = 3.0

def parse_line(line):
    reader = csv.reader([line])
    try:
        fields = next(reader)
    except StopIteration:
        return None

    if len(fields) == 11:
        step = fields[0].strip()
        tx_type = fields[1].strip()
        amount = fields[2].strip()
        name_orig = fields[3].strip()
        old_bal_orig = fields[4].strip()
        new_bal_orig = fields[5].strip()
        is_fraud = fields[9].strip()

        if not step.lstrip("-").isdigit():
            return None

        try:
            step_int = int(step)
            hour = step_int % 24
            amt = float(amount)
        except ValueError:
            return None

        return {
            "dataset": "PAYSIM",
            "account": name_orig,
            "step": step_int,
            "hour_bucket": f"{name_orig}_{hour}",
            "amount": amt,
            "tx_type": tx_type,
            "is_fraud": is_fraud
        }

    elif len(fields) >= 10:
        step = fields[0].strip().strip("'")
        customer = fields[1].strip().strip("'")
        # Fixed index for amount in BankSim (8) and fraud (9)
        amount_raw = fields[8].strip().strip("'")
        fraud = fields[9].strip().strip("'")

        if not step.lstrip("-").replace(".", "").isdigit():
            return None

        try:
            step_float = float(step)
            hour = int(step_float) % 24
            amt = float(amount_raw)
        except ValueError:
            return None

        return {
            "dataset": "BANKSIM",
            "account": customer,
            "step": step_float,
            "hour_bucket": f"{customer}_{hour}",
            "amount": amt,
            "tx_type": fields[7].strip().strip("'") if len(fields) > 7 else "unknown",
            "is_fraud": fraud
        }

    return None

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    rec = parse_line(line)
    if rec is None:
        continue

    hour_bucket = rec["hour_bucket"]
    amount = rec["amount"]
    dataset = rec["dataset"]
    account = rec["account"]
    step = rec["step"]
    tx_type = rec["tx_type"]
    is_fraud = rec["is_fraud"]

    # Emitting with prefixes to allow the reducer to distinguish between 
    # Global statistics gathering and Velocity bucket processing
    print(f"VEL\t{hour_bucket}\t{amount}\t{dataset}\t{account}\t{step}\t{tx_type}\t{is_fraud}")

    log_amount = math.log1p(amount) if amount > 0 else 0.0
    print(f"AMT\tGLOBAL\t{log_amount}\t{dataset}\t{account}\t{step}\t{tx_type}\t{is_fraud}")