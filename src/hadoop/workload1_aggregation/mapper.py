#!/usr/bin/env python3
import sys
import csv

HEADER_FIELDS = {"step", "type", "amount", "nameOrig", "oldbalanceOrg",
                 "newbalanceOrig", "nameDest", "oldbalanceDest",
                 "newbalanceDest", "isFraud", "isFlaggedFraud"}

def parse_paysim(line):
    reader = csv.reader([line])
    fields = next(reader)
    if len(fields) != 11:
        return None
    return {
        "step": fields[0],
        "type": fields[1],
        "amount": fields[2],
        "nameOrig": fields[3],
        "nameDest": fields[7]
    }

def parse_banksim(line):
    reader = csv.reader([line])
    fields = next(reader)
    if len(fields) < 10:
        return None
    return {
        "step": fields[0].strip("'"),
        # Agreed Fix: Standard BankSim schema has category at 7 and amount at 8
        "type": fields[7].strip("'"),
        "amount": fields[8].strip("'"),
        "nameOrig": fields[1].strip("'"),
        "nameDest": fields[5].strip("'")
    }

def emit(key, amount):
    try:
        amt = float(amount)
        # Standardizing output for the reducer to include the count '1'
        print(f"{key}\t{amt}\t1")
    except (ValueError, TypeError):
        pass

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    first_field = line.split(",")[0].strip().strip("'")
    if first_field.lower() in ("step", ""):
        continue

    parts = line.split(",")
    if len(parts) == 11:
        rec = parse_paysim(line)
        if rec:
            day = rec["step"]
            account = rec["nameOrig"]
            amount = rec["amount"]
            tx_type = rec["type"]
            key = f"PAYSIM\t{account}\t{day}\t{tx_type}"
            emit(key, amount)
    elif len(parts) >= 10:
        # Using the parse_banksim function for consistency
        rec = parse_banksim(line)
        if rec:
            step = rec["step"]
            customer = rec["nameOrig"]
            category = rec["type"]
            amount = rec["amount"]
            if step and customer:
                key = f"BANKSIM\t{customer}\t{step}\t{category}"
                emit(key, amount)