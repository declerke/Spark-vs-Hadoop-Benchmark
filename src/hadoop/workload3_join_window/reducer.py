#!/usr/bin/env python3
import sys

WINDOW_SIZE_STEPS = 24

current_join_key = None
tx_records = []
dim_records = []

def compute_rolling_window(transactions, window_size):
    # Sort by step to ensure time-based windowing is accurate
    transactions_sorted = sorted(transactions, key=lambda r: float(r["step"]))
    results = []
    for i, tx in enumerate(transactions_sorted):
        current_step = float(tx["step"])
        window_start = current_step - window_size
        
        # Calculate windowed metrics for this specific transaction
        rolling_sum = 0.0
        rolling_count = 0
        for t in transactions_sorted:
            t_step = float(t["step"])
            if window_start <= t_step <= current_step:
                rolling_sum += float(t["amount"])
                rolling_count += 1
                
        results.append({
            **tx, 
            "rolling_24h_sum": rolling_sum, 
            "rolling_24h_count": rolling_count
        })
    return results

def emit_joined(join_key, tx_records, dim_records):
    if not tx_records:
        return

    # Create a lookup for dimensions (Join Logic)
    dim_lookup = {}
    for dim in dim_records:
        dim_lookup[dim.get("target", "")] = dim

    enriched_tx = []
    for tx in tx_records:
        # Join on counterparty/merchant
        dim_info = dim_lookup.get(tx.get("counterparty", ""), {})
        enriched_tx.append({
            **tx,
            "dim_type": dim_info.get("type", "NONE"),
            "dim_weight": dim_info.get("weight", 0.0)
        })

    # Apply rolling window logic per join key (customer/account)
    windowed = compute_rolling_window(enriched_tx, WINDOW_SIZE_STEPS)

    for rec in windowed:
        print(
            f"{join_key},{rec.get('dataset','')},{rec.get('step','')},{rec.get('type','')},"
            f"{rec.get('amount',0):.2f},{rec.get('rolling_24h_sum',0):.2f},"
            f"{rec.get('rolling_24h_count',0)},{rec.get('counterparty','')},"
            f"{rec.get('dim_type','')},{rec.get('dim_weight',0)},"
            f"{rec.get('is_fraud',0)}"
        )

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 4:
        continue

    join_key = parts[0]
    tag = parts[1]

    # Partition boundary check
    if join_key != current_join_key:
        if current_join_key is not None:
            emit_joined(current_join_key, tx_records, dim_records)
        current_join_key = join_key
        tx_records = []
        dim_records = []

    if tag == "TX":
        dataset = parts[2]
        step = parts[3]
        tx_type = parts[4]
        amount = parts[5]
        # Counterparty is at index 8 based on mapper output
        counterparty = parts[8] if len(parts) > 8 else ""
        is_fraud = parts[9] if len(parts) > 9 else "0"

        try:
            amt_float = float(amount)
        except ValueError:
            amt_float = 0.0

        tx_records.append({
            "dataset": dataset,
            "step": step,
            "type": tx_type,
            "amount": amt_float,
            "counterparty": counterparty,
            "is_fraud": is_fraud
        })

    elif tag == "DIM":
        target = parts[3]
        dim_type = parts[4]
        weight_str = parts[5]
        is_fraud = parts[9] if len(parts) > 9 else "0"

        try:
            weight = float(weight_str)
        except ValueError:
            weight = 0.0

        dim_records.append({
            "target": target,
            "type": dim_type,
            "weight": weight,
            "is_fraud": is_fraud
        })

# Flush remaining records
if current_join_key is not None:
    emit_joined(current_join_key, tx_records, dim_records)
