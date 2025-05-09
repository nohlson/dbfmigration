#!/usr/bin/env python3
"""
Sales Order History Migrator Script

This script reads legacy sales order invoice data and part lines from two JSON files:
  - SalesOrderInvoice.json (primary)
  - SalesOrderParts.json (for original ORDATE lookup)
It groups invoice entries by sales order number, looks up customer and part ObjectIDs,
and inserts Order documents into MongoDB with correct dates, parts, and freight methods.
All dates are converted from Eastern Time at 12:00 to UTC to preserve local date.
Supports custom timestamps and dry-run preview.
"""
import argparse
import json
import sys
from datetime import datetime, time
from zoneinfo import ZoneInfo
from pymongo import MongoClient, InsertOne

# Timezones
eastern = ZoneInfo("America/New_York")
utc = ZoneInfo("UTC")

def safe_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def safe_convert_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_date(value, default=None):
    if not value:
        return default
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d")
    except ValueError:
        return default


def eastern_noon_utc(date_str, default=None):
    """Return a UTC datetime corresponding to 12:00 noon Eastern of the given YYYY-MM-DD string."""
    d = parse_date(date_str)
    if not d:
        return default
    dt_local = datetime.combine(d.date(), time(hour=12), tzinfo=eastern)
    return dt_local.astimezone(utc)


def migrate_sales_orders(
    invoice_file,
    parts_file,
    db,
    sales_person_id,
    account_type,
    account_number,
    terms,
    batch_size=1000,
    dry_run=False
):
    print(f"Loading invoice data from {invoice_file}...")
    invoices = json.load(open(invoice_file, encoding="utf-8"))
    print(f"Found {len(invoices)} invoice lines in JSON file")

    print(f"Loading parts data from {parts_file} for ORDATE lookup...")
    parts_lines = json.load(open(parts_file, encoding="utf-8"))
    print(f"Found {len(parts_lines)} part lines in JSON file")

    # Build ORDATE UTC lookup by SONO
    order_date_map = {}
    for line in parts_lines:
        sono = safe_str(line.get("SONO"))
        if sono not in order_date_map and line.get("ORDATE"):
            dt_utc = eastern_noon_utc(line.get("ORDATE"))
            if dt_utc:
                order_date_map[sono] = dt_utc

    # Group invoice entries by SONO
    orders_map = {}
    for inv in invoices:
        sono = safe_str(inv.get("SONO"))
        orders_map.setdefault(sono, []).append(inv)
    print(f"Grouped into {len(orders_map)} distinct orders from invoice data")

    if not dry_run:
        db.orders.create_index("order_number", unique=True, background=True)

    bulk_ops = []
    processed = 0

    for sono, inv_lines in orders_map.items():
        # Lookup customer
        custno = safe_str(inv_lines[0].get("CUSTNO"))
        customer = db.customers.find_one({"customer_number": custno})
        if not customer:
            print(f"WARNING: Customer {custno} not found for order {sono}. Skipping.")
            continue
        customer_id = customer.get("_id")

        # Determine dates
        order_date = order_date_map.get(
            sono,
            eastern_noon_utc(inv_lines[0].get("ORDATE"), datetime.utcnow().replace(tzinfo=utc))
        )
        ship_date = eastern_noon_utc(
            inv_lines[0].get("SHIPDATE"),
            datetime.utcnow().replace(tzinfo=utc)
        )

        parts_list = []
        shipped_parts = []
        for inv in inv_lines:
            item_no = safe_str(inv.get("ITEM"))
            part = db.parts.find_one({"item_number": item_no})
            if not part:
                print(f"WARNING: Part {item_no} not found for order {sono}. Skipping line.")
                continue
            part_id = part.get("_id")
            qty_shp = safe_convert_float(inv.get("QTYSHP"), 0.0)
            price = safe_convert_float(inv.get("PRICE"), 0.0)

            price = int(price * 100)  # Convert to cents

            parts_list.append({"part_id": part_id, "quantity": qty_shp, "price": price})
            shipped_parts.append({"part_id": part_id, "quantity": qty_shp, "price": price})

        if not parts_list:
            print(f"WARNING: No valid parts for order {sono}. Skipping.")
            continue

        freight_method = {
            "account_type": account_type,
            "account_number": account_number,
            "shipped_parts": shipped_parts,
            "returned_parts": [],
            "shipping_cost": 0,
            "shipping_date": ship_date,
            "comments": "",
            "paid": True,
            "date_paid": None,
            "check_number": "",
            "invoice_number": 1
        }

        total_price = sum(p["quantity"] * p["price"] for p in parts_list)

        order_doc = {
            "order_number": sono,
            "customer": customer_id,
            "date": order_date,
            "parts": parts_list,
            "freight_methods": [freight_method],
            "dropship_methods": [],
            "terms": terms,
            "sales_person": sales_person_id,
            "notes": "",
            "total_price": total_price,
            "status": "Completed",
            "customer_purchase_order_number": None,
            "createdAt": order_date,
            "updatedAt": ship_date
        }

        bulk_ops.append(InsertOne(order_doc))
        processed += 1

        if len(bulk_ops) >= batch_size:
            if dry_run:
                print(f"DRY RUN: Would insert {len(bulk_ops)} orders")
            else:
                print(f"Inserting batch of {len(bulk_ops)} orders...")
                db.orders.bulk_write(bulk_ops, ordered=False)
            bulk_ops = []

    if bulk_ops:
        if dry_run:
            print(f"DRY RUN: Would insert {len(bulk_ops)} orders")
        else:
            print(f"Inserting final batch of {len(bulk_ops)} orders...")
            db.orders.bulk_write(bulk_ops, ordered=False)

    print(f"Processed {processed} orders total.")


def main():
    parser = argparse.ArgumentParser(
        description="Sales Order History Migrator - imports legacy sales orders into MongoDB"
    )
    parser.add_argument("--mongo-hostname", default="localhost", help="MongoDB hostname")
    parser.add_argument("--mongo-port", type=int, default=27017, help="MongoDB port")
    parser.add_argument("--database-name", default="migrationtest", help="MongoDB database name")
    parser.add_argument("--invoice-file", required=True, help="JSON file with sales invoice data")
    parser.add_argument("--parts-file", required=True, help="JSON file with original sales parts data for ORDATE lookup")
    parser.add_argument("--sales-person-id", required=True, help="User ObjectId to assign as sales_person")
    parser.add_argument("--account-type", required=True, help="Freight account type")
    parser.add_argument("--account-number", required=True, help="Freight account number")
    parser.add_argument("--terms", default="Net 30", help="Payment terms")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")
    parser.add_argument("--dry-run", action="store_true", help="Preview operations without making changes")
    args = parser.parse_args()

    try:
        print(f"Connecting to MongoDB at {args.mongo_hostname}:{args.mongo_port}...")
        client = MongoClient(
            host=args.mongo_hostname,
            port=args.mongo_port,
            socketTimeoutMS=60000,
            connectTimeoutMS=30000,
            serverSelectionTimeoutMS=30000,
            maxPoolSize=10
        )
        db = client[args.database_name]
        db.command("ping")
        print(f"Connected to database '{args.database_name}'")

        if args.dry_run:
            print("*** DRY RUN MODE: No changes will be made ***")

        migrate_sales_orders(
            invoice_file=args.invoice_file,
            parts_file=args.parts_file,
            db=db,
            sales_person_id=args.sales_person_id,
            account_type=args.account_type,
            account_number=args.account_number,
            terms=args.terms,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
