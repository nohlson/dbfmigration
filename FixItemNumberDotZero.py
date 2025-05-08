#!/usr/bin/env python3
"""
Fast Item-Number Fixer with Progress

For each legacy JSON record whose ITEM ends in ".0":
  • If the exact ".0" value still exists in the new JSON → skip
  • Else if the stripped ITEM (drop ".0") does exist in the new JSON → rename in MongoDB

Prints scan progress and flush progress, plus a summary at the end.
"""

import argparse
import json
import sys
import time
from pymongo import MongoClient, UpdateOne


def safe_str(value):
    """Convert a JSON ITEM to its canonical string without trailing .0."""
    if value is None:
        return ""
    s = str(value).strip()
    return s[:-2] if s.endswith(".0") else s


def update_item_numbers(old_file, new_file, db, batch_size=500, dry_run=False):
    import time
    from pymongo import UpdateOne

    old_parts = json.load(open(old_file, encoding="utf-8"))
    new_parts = json.load(open(new_file, encoding="utf-8"))

    new_raw      = {str(p.get("ITEM")).strip()    for p in new_parts}
    new_stripped = {safe_str(p.get("ITEM"))       for p in new_parts}

    total = len(old_parts)
    progress_interval = max(1, total // 20)

    applied = []
    skipped_up_to_date = []
    skipped_no_match = []
    bulk_ops = []
    updated = 0
    start = time.time()

    print(f"Scanning {total} parts for “.0” entries…")
    for idx, part in enumerate(old_parts, start=1):
        raw_old = str(part.get("ITEM")).strip()

        if raw_old.endswith(".0"):
            # strip .0
            candidate = raw_old[:-2]

            if raw_old in new_raw:
                skipped_up_to_date.append(raw_old)

            else:
                # first try the plain stripped candidate
                if candidate in new_raw or candidate in new_stripped:
                    new_item = candidate

                else:
                    # second pass: try with a leading zero
                    alt = "0" + candidate
                    if alt in new_raw or alt in new_stripped:
                        new_item = alt
                    else:
                        skipped_no_match.append(raw_old)
                        continue  # no match at all

                # schedule the rename
                bulk_ops.append(
                    UpdateOne(
                        {"item_number": raw_old},
                        {"$set": {"item_number": new_item}}
                    )
                )
                applied.append((raw_old, new_item))

        # flush batches
        if len(bulk_ops) >= batch_size:
            if dry_run:
                print(f"[{idx}/{total}] DRY RUN: would apply {len(bulk_ops)} renames")
            else:
                res = db.parts.bulk_write(bulk_ops)
                updated += res.modified_count
                print(f"[{idx}/{total}] Flushed → matched {res.matched_count}, modified {res.modified_count}")
            bulk_ops.clear()

        # progress update
        if idx % progress_interval == 0:
            pct = idx/total*100
            queued = len(applied) - updated
            print(f" Scanned {idx}/{total} ({pct:.0f}%), queued {queued} renames")

    # final flush
    if bulk_ops:
        if dry_run:
            print(f"[{total}/{total}] DRY RUN: would apply final {len(bulk_ops)} renames")
        else:
            res = db.parts.bulk_write(bulk_ops)
            updated += res.modified_count
            print(f"[{total}/{total}] Final batch → matched {res.matched_count}, modified {res.modified_count}")

    # summary
    duration = time.time() - start
    mode = "DRY RUN" if dry_run else "APPLIED"
    print(f"\n{mode} SUMMARY in {duration:.1f}s")
    print(f" • Renames applied:         {len(applied)}")
    print(f" • Skipped (still in new):  {len(skipped_up_to_date)}")
    print(f" • Skipped (no match):      {len(skipped_no_match)}\n")

    if applied:
        print("APPLIED:")
        for old, new in applied:
            print(f"  {old} → {new}")
        print()

    if skipped_up_to_date:
        print("SKIPPED (already correct .0 entries):")
        for old in skipped_up_to_date:
            print(f"  {old}")
        print()

    if skipped_no_match:
        print("SKIPPED (no .0 or leading-0 match):")
        for old in skipped_no_match:
            print(f"  {old}")
        print()


def main():
    p = argparse.ArgumentParser(description="Fast .0→no-.0 item_number fixer with progress")
    p.add_argument("--mongo-hostname", default="localhost")
    p.add_argument("--mongo-port",    type=int, default=27017)
    p.add_argument("--database-name", default="migrationtest")
    p.add_argument("--old-file",      required=True)
    p.add_argument("--new-file",      required=True)
    p.add_argument("--batch-size",    type=int, default=500)
    p.add_argument("--dry-run",       action="store_true",
                   help="Preview changes without writing to MongoDB")
    args = p.parse_args()

    # Connect
    try:
        client = MongoClient(
            host=args.mongo_hostname,
            port=args.mongo_port,
            socketTimeoutMS=60000,
            connectTimeoutMS=30000,
            serverSelectionTimeoutMS=30000
        )
        db = client[args.database_name]
        db.command("ping")
    except Exception as e:
        print(f"ERROR connecting to MongoDB: {e}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("*** DRY RUN – no changes will be written ***\n")

    update_item_numbers(
        old_file=args.old_file,
        new_file=args.new_file,
        db=db,
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()