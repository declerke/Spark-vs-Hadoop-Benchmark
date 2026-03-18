#!/usr/bin/env python3
import sys
import csv

def parse_paysim_transaction(fields):
    if len(fields) != 11:
        return None
    try:
        step = int(fields[0].strip())
        tx_type = fields[1].strip()
        amount = float(fields[2].strip())
        name_orig = fields[3].strip()
        old_bal_orig = float(fields[4].strip())
        new_bal_orig = float(fields[5].strip())
        name_dest = fields[7].strip()
        is_fraud = int(fields[9].strip())
    except (ValueError, IndexError):
        return None

    return {
        "tag": "TX",
        "join_key": name_orig,
        "step": step,
        "type": tx_type,
        "amount": amount,
        "old_bal": old_bal_orig,
        "new_bal": new_bal_orig,
        "counterparty": name_dest,
        "is_fraud": is_fraud
    }

def parse_banksim_transaction(fields):
    if len(fields) < 10:
        return None
    try:
        step = float(fields[0].strip().strip("'"))
        customer = fields[1].strip().strip("'")
        age = fields[2].strip().strip("'")
        gender = fields[3].strip().strip("'")
        zip_ori = fields[4].strip().strip("'")
        merchant = fields[5].strip().strip("'")
        zip_merch = fields[6].strip().strip("'")
        category = fields[7].strip().strip("'")
        amount = float(fields[8].strip().strip("'"))
        fraud = int(float(fields[9].strip().strip("'")))
    except (ValueError, IndexError, TypeError):
        return None

    return {
        "tag": "TX",
        "join_key": customer,
        "step": step,
        "type": category,
        "amount": amount,
        "age": age,
        "gender": gender,
        "zip_ori": zip_ori,
        "merchant": merchant,
        "is_fraud": fraud
    }

def parse_banksim_network(fields):
    if len(fields) < 5:
        return None
    try:
        source = fields[0].strip().strip("'")
        target = fields[1].strip().strip("'")
        weight = float(fields[2].strip().strip("'"))
        trans_type = fields[3].strip().strip("'")
        fraud = int(float(fields[4].strip().strip("'")))
    except (ValueError, IndexError, TypeError):
        return None

    return {
        "tag": "DIM",
        "join_key": source,
        "target": target,
        "weight": weight,
        "type": trans_type,
        "is_fraud": fraud
    }

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    reader = csv.reader([line])
    try:
        fields = next(reader)
    except StopIteration:
        continue

    if not fields:
        continue

    first = fields[0].strip().strip("'").lower()
    if first in ("step", "source", ""):
        continue

    num_fields = len(fields)

    if num_fields == 11:
        rec = parse_paysim_transaction(fields)
        if rec:
            jk = rec["join_key"]
            print(
                f"{jk}\tTX\tPAYSIM\t{rec['step']}\t{rec['type']}\t"
                f"{rec['amount']}\t{rec['old_bal']}\t{rec['new_bal']}\t"
                f"{rec['counterparty']}\t{rec['is_fraud']}"
            )

    elif num_fields >= 10:
        rec = parse_banksim_transaction(fields)
        if rec:
            jk = rec["join_key"]
            print(
                f"{jk}\tTX\tBANKSIM\t{rec['step']}\t{rec['type']}\t"
                f"{rec['amount']}\t{rec.get('age', '')}\t{rec.get('gender', '')}\t"
                f"{rec.get('merchant', '')}\t{rec['is_fraud']}"
            )

    elif num_fields == 5:
        rec = parse_banksim_network(fields)
        if rec:
            jk = rec["join_key"]
            print(
                f"{jk}\tDIM\tNETWORK\t{rec['target']}\t{rec['type']}\t"
                f"{rec['weight']}\t\t\t\t{rec['is_fraud']}"
            )
