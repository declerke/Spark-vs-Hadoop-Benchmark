#!/usr/bin/env python3
import sys

current_key = None
total_amount = 0.0
tx_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 6:
        continue

    dataset = parts[0]
    account = parts[1]
    day = parts[2]
    tx_type = parts[3]
    key = f"{dataset}\t{account}\t{day}\t{tx_type}"

    try:
        amount = float(parts[4])
        count = int(parts[5])
    except (ValueError, IndexError):
        continue

    if key == current_key:
        total_amount += amount
        tx_count += count
    else:
        if current_key is not None:
            key_parts = current_key.split("\t")
            print(f"{key_parts[0]},{key_parts[1]},{key_parts[2]},{key_parts[3]},{total_amount:.2f},{tx_count}")
        current_key = key
        total_amount = amount
        tx_count = count

if current_key is not None:
    key_parts = current_key.split("\t")
    print(f"{key_parts[0]},{key_parts[1]},{key_parts[2]},{key_parts[3]},{total_amount:.2f},{tx_count}")
