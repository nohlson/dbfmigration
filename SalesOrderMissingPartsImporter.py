#!/usr/bin/env python3
"""
Sales Order Missing Parts Importer

Reads a list of warning messages identifying missing part numbers,
looks up those part records in a legacy parts JSON file (and optional supplier JSON),
and inserts any not-yet-present parts into the MongoDB parts collection.
Supports dry-run mode to preview insert operations without writing to the database.
"""
import argparse
import json
import re
import sys
from datetime import datetime
from pymongo import MongoClient


def safe_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()

def safe_convert_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_convert_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_warnings(warnings_file):
    """Extract unique part numbers from warning lines."""
    pattern = re.compile(r"Part\s+(\S+)\s+not found")
    missing = set()
    with open(warnings_file, 'r', encoding='utf-8') as f:
        for line in f:
            m = pattern.search(line)
            if m:
                missing.add(m.group(1))
    return missing


def main():
    parser = argparse.ArgumentParser(
        description="Import missing parts based on warning list into MongoDB"
    )
    parser.add_argument("--warnings-file", required=True,
                        help="Text file with warning lines identifying missing parts")
    parser.add_argument("--parts-file", required=True,
                        help="JSON file with legacy parts definitions")
    parser.add_argument("--suppliers-file", default=None,
                        help="Optional JSON file with supplier definitions for lookup")
    parser.add_argument("--mongo-hostname", default="localhost",
                        help="MongoDB hostname")
    parser.add_argument("--mongo-port", type=int, default=27017,
                        help="MongoDB port")
    parser.add_argument("--database-name", default="migrationtest",
                        help="MongoDB database name")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview inserts without writing to MongoDB")
    args = parser.parse_args()

    # Load missing part numbers
    missing_parts = parse_warnings(args.warnings_file)
    if not missing_parts:
        print("No missing parts found in warnings file.")
        sys.exit(0)
    print(f"Found {len(missing_parts)} unique missing part numbers")

    # Load legacy parts data
    parts_data = json.load(open(args.parts_file, encoding='utf-8'))
    parts_lookup = {safe_str(p.get("ITEM")): p for p in parts_data}

    # Optionally load suppliers JSON to seed database lookup keys
    supplier_map = {}
    if args.suppliers_file:
        suppliers_data = json.load(open(args.suppliers_file, encoding='utf-8'))
        for s in suppliers_data:
            code = safe_str(s.get("VENDNO"))
            supplier_map[code] = None  # placeholder

    # Connect to MongoDB
    print(f"Connecting to MongoDB at {args.mongo_hostname}:{args.mongo_port}...")
    client = MongoClient(
        host=args.mongo_hostname,
        port=args.mongo_port,
        socketTimeoutMS=60000,
        connectTimeoutMS=30000,
        serverSelectionTimeoutMS=30000,
    )
    db = client[args.database_name]
    try:
        db.command("ping")
    except Exception as e:
        print(f"ERROR: Could not connect to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    # Resolve supplier ObjectIds if needed
    if args.suppliers_file:
        for code in list(supplier_map):
            sup = db.suppliers.find_one({"vendor_number": code})
            if sup:
                supplier_map[code] = sup.get("_id")
            else:
                print(f"WARNING: Supplier {code} not found in MongoDB.")

    # Process each missing part
    inserted = 0
    skipped = 0
    for item_no in missing_parts:
        if db.parts.count_documents({"item_number": item_no}, limit=1) > 0:
            print(f"SKIP: Part {item_no} already exists in database.")
            skipped += 1
            continue
        legacy = parts_lookup.get(item_no)
        if not legacy:
            print(f"ERROR: No definition for part {item_no} in parts file.")
            continue

        # Build new part doc
        doc = {
            "item_number": item_no,
            "description": safe_str(legacy.get("DESCRIP")),
            "quantity_on_hand": safe_convert_float(legacy.get("ONHAND"), 0.0),
            "default_price": int(safe_convert_float(legacy.get("PRICE"), 0.0) * 100),
            "location": safe_str(legacy.get("SEQ")),
            "notes": safe_str(legacy.get("VPARTNO")),
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        }
        # Attach supplier ref if available
        sup_code = safe_str(legacy.get("SUPPLIER"))
        sup_id = supplier_map.get(sup_code)
        if sup_id:
            doc["suppliers"] = [sup_id]

        if args.dry_run:
            print(f"DRY RUN: Would insert part doc: {doc}")
        else:
            db.parts.insert_one(doc)
            print(f"Inserted part {item_no} into database.")
        inserted += 1

    print(f"Done. Inserted: {inserted}, Skipped: {skipped}.")

if __name__ == "__main__":
    main()
