#!/usr/bin/env python3
"""
Sales Order History Migrator Script

This script reads legacy sales order line data from a JSON file, groups entries by sales order number, and inserts corresponding Order documents into MongoDB.

Capabilities:
1. Loads sales lines from JSON
2. Groups lines by SONO into individual orders
3. Looks up Part and Customer ObjectIDs in MongoDB
4. Builds Order documents with parts, a single freight method, and custom timestamps
5. Supports dry-run mode to preview operations without modifying the database
"""
import argparse
import json
import sys
from datetime import datetime
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
from datetime import datetime, time
from zoneinfo import ZoneInfo

eastern = ZoneInfo("America/New_York")

def safe_str(value, default=""):
    """Convert a value to string safely, return default if None."""
    if value is None:
        return default
    return str(value).strip()

def safe_convert_float(value, default=0.0):
    """Convert a value to float safely, return default if conversion fails."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def parse_date(value, default=None):
    """Parse a YYYY-MM-DD date string into a datetime, return default on failure."""
    if not value:
        return default
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d")
    except ValueError:
        return default

def migrate_sales_orders(
    sales_file,
    db,
    sales_person_id,
    account_type,
    account_number,
    terms,
    batch_size=1000,
    dry_run=False
):
    print(f"Loading sales data from {sales_file}...")
    sales_data = json.load(open(sales_file, encoding="utf-8"))
    print(f"Found {len(sales_data)} sales lines in JSON file")

    # Group lines by sales order number
    orders_map = {}
    for line in sales_data:
        sono = safe_str(line.get("SONO"))
        orders_map.setdefault(sono, []).append(line)

    print(f"Grouped into {len(orders_map)} distinct orders")

    # Ensure unique index on order_number
    if not dry_run:
        db.orders.create_index("order_number", unique=True, background=True)

    bulk_ops = []
    processed = 0

    for sono, lines in orders_map.items():
        # Lookup customer by CUSTNO
        custno = safe_str(lines[0].get("CUSTNO"))
        customer = db.customers.find_one({"customer_number": custno})
        if not customer:
            #print(f"WARNING: Customer {custno} not found for order {sono}. Skipping.")
            continue
        customer_id = customer.get("_id")

        # Build parts list and freight shipped parts
        parts_list = []
        shipped_parts = []
        for line in lines:
            item_no = safe_str(line.get("ITEM"))
            part = db.parts.find_one({"item_number": item_no})
            if not part:
                #print(f"WARNING: Part {item_no} not found for order {sono}. Skipping line.")
                continue
            part_id = part.get("_id")
            qty_ord = safe_convert_float(line.get("QTYORD"), 0.0)
            qty_shp = safe_convert_float(line.get("QTYSHP"), 0.0)
            price = safe_convert_float(line.get("PRICE"), 0.0)

            price = int(price * 100) if price else 0

            # total quantity for order.parts
            total_qty = qty_ord + qty_shp
            parts_list.append({
                "part_id": part_id,
                "quantity": total_qty,
                "price": price
            })

            # shipped_parts for freight
            shipped_parts.append({
                "part_id": part_id,
                "quantity": qty_shp,
                "price": price
            })

        if not parts_list:
            print(f"WARNING: No valid parts for order {sono}. Skipping.")
            continue

        # Parse dates
        _raw_order = lines[0].get("ORDATE").strip()
        _raw_ship  = lines[0].get("SHIPDATE").strip()

        # build a noon-local datetime, then convert to UTC
        order_date = (
            datetime.combine(datetime.strptime(_raw_order, "%Y-%m-%d").date(),
                            time(hour=12, minute=0),
                            tzinfo=eastern)
            .astimezone(ZoneInfo("UTC"))
        )

        ship_date = (
            datetime.combine(datetime.strptime(_raw_ship, "%Y-%m-%d").date(),
                            time(hour=12, minute=0),
                            tzinfo=eastern)
            .astimezone(ZoneInfo("UTC"))
        )
        # Build freight method record
        freight_method = {
            "account_type": account_type,
            "account_number": account_number,
            "shipped_parts": shipped_parts,
            "returned_parts": [],
            "shipping_cost": 0,
            "shipping_date": ship_date,
            "comments": "",
            "paid": True,
            "date_paid": ship_date,
            "check_number": "",
            "invoice_number": 1
        }

        # Compute total order price
        total_price = sum(p["quantity"] * p["price"] for p in parts_list)

        # Construct order document with custom timestamps
        order_doc = {
            "order_number": sono,
            "customer": customer_id,
            "date": order_date,
            "parts": parts_list,
            "freight_methods": [freight_method],
            "dropship_methods": [],
            "terms": terms,
            "sales_person": sales_person_id,
            "notes": "Imported from VisionPoint",
            "total_price": total_price,
            "status": "Draft",
            "customer_purchase_order_number": None,
            # MongoDB timestamps override
            "createdAt": order_date,
            "updatedAt": ship_date
        }

        bulk_ops.append(InsertOne(order_doc))
        processed += 1

        # Execute batches
        if len(bulk_ops) >= batch_size:
            if dry_run:
                print(f"DRY RUN: Would insert {len(bulk_ops)} orders")
            else:
                print(f"Inserting batch of {len(bulk_ops)} orders...")
                db.orders.bulk_write(bulk_ops)
            bulk_ops = []

    # Final batch
    if bulk_ops:
        if dry_run:
            print(f"DRY RUN: Would insert {len(bulk_ops)} orders")
        else:
            print(f"Inserting final batch of {len(bulk_ops)} orders...")
            try:
                # unordered ⇒ continue on errors (e.g. dup keys)
                result = db.orders.bulk_write(bulk_ops, ordered=False)
                print(f"Inserted {result.inserted_count} new orders (skipped collisions).")
            except BulkWriteError as bwe:
                # bwe.details['writeErrors'] contains the errors that occurred
                inserted = bwe.details.get("nInserted", 0)
                skips = len(bwe.details.get("writeErrors", []))
                print(f"Inserted {inserted} new orders, skipped {skips} duplicates.")

                print(f"Processed {processed} orders total.")


def main():
    parser = argparse.ArgumentParser(
        description="Sales Order History Migrator - imports legacy sales orders into MongoDB"
    )
    parser.add_argument(
        "--mongo-hostname", default="localhost", help="MongoDB hostname"
    )
    parser.add_argument(
        "--mongo-port", type=int, default=27017, help="MongoDB port"
    )
    parser.add_argument(
        "--database-name", default="migrationtest", help="MongoDB database name"
    )
    parser.add_argument(
        "--sales-file", required=True, help="JSON file with sales order line data"
    )
    parser.add_argument(
        "--sales-person-id", required=True,
        help="User ObjectId to assign as sales_person"
    )
    parser.add_argument(
        "--account-type", required=True, help="Freight account type"
    )
    parser.add_argument(
        "--account-number", required=True, help="Freight account number"
    )
    parser.add_argument(
        "--terms", default="Net 30", help="Payment terms"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for inserts"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview operations without making changes"
    )
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
        print(f"Successfully connected to database '{args.database_name}'")

        if args.dry_run:
            print("*** DRY RUN MODE: No changes will be made to the database ***")

        migrate_sales_orders(
            sales_file=args.sales_file,
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
        import traceback
        traceback.print_exc()
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
