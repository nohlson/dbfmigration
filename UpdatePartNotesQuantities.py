#!/usr/bin/env python3
"""
Simple Parts Updater Script

This script only updates two things in the MongoDB parts collection:
1. Overwrites quantity_on_hand from the legacy JSON
2. Concatenates SEQ data with notes data (LINE1-6) and writes to the notes field
"""

import argparse
import json
import sys
from datetime import datetime
from pymongo import MongoClient, UpdateOne

def safe_str(value, default=""):
    """Convert a value to string safely, return default if None."""
    if value is None:
        return default
    return str(value).strip()

def safe_convert_int(value, default=0):
    """Convert a value to int safely, return default if conversion fails."""
    try:
        return int(value)
    except (ValueError, TypeError):
        #print(f"WARNING: Failed to convert '{value}' to int. Using default {default}.")
        return default

def update_parts(parts_file, notes_file, db, batch_size=5000, dry_run=False):
    """Update parts in MongoDB with quantities and notes from legacy system."""
    print(f"Loading parts from {parts_file}...")
    parts_data = json.load(open(parts_file, encoding="utf-8"))
    print(f"Found {len(parts_data)} parts in JSON file")
    
    print(f"Loading part notes from {notes_file}...")
    notes_data = json.load(open(notes_file, encoding="utf-8"))
    print(f"Found {len(notes_data)} part notes entries")
    
    # Create a lookup dictionary for notes for faster access
    notes_lookup = {}
    for note in notes_data:
        item_no = safe_str(note.get("ITEM"))
        note_text = "\n".join([
            safe_str(note.get("LINE1")),
            safe_str(note.get("LINE2")), 
            safe_str(note.get("LINE3")),
            safe_str(note.get("LINE4")),
            safe_str(note.get("LINE5")),
            safe_str(note.get("LINE6"))
        ])
        notes_lookup[item_no] = note_text
    
    # Add index on item_number if it doesn't exist
    if not dry_run:
        print("Ensuring index on item_number field...")
        db.parts.create_index("item_number", background=True)
    
    # Process parts in batches
    bulk_operations = []
    processed_count = 0
    updated_count = 0
    
    # Show progress after processing this many parts
    progress_interval = min(10000, max(1000, len(parts_data) // 20))
    
    print(f"Processing {len(parts_data)} parts...")
    start_time = datetime.now()
    
    for part in parts_data:
        item_no = safe_str(part.get("ITEM"))
        qty = safe_convert_int(part.get("ONHAND"), 0)
        seq = safe_str(part.get("SEQ"))
        
        # Get VPARTNO for notes
        vpartno = safe_str(part.get("VPARTNO"))
        
        # Combine VPARTNO with notes if available
        combined_notes = vpartno
        if item_no in notes_lookup and notes_lookup[item_no].strip():
            if combined_notes:
                combined_notes += "\n" + notes_lookup[item_no]
            else:
                combined_notes = notes_lookup[item_no]
        
        # Create update operation
        update_spec = {
            "$set": {
                "quantity_on_hand": qty,
                "location": seq,  # SEQ goes to location field
                "notes": combined_notes,  # VPARTNO + notes
                "updatedAt": datetime.utcnow()
            }
        }
        
        bulk_operations.append(
            UpdateOne({"item_number": item_no}, update_spec, upsert=True)
        )
        
        processed_count += 1
        
        # Execute batch if we've reached the batch size
        if len(bulk_operations) >= batch_size:
            if dry_run:
                print(f"DRY RUN: Would update {len(bulk_operations)} parts")
                if processed_count <= 3:  # Show only first few in dry run
                    for op in bulk_operations[:3]:
                        print(f"  {op.filter} → {op.document}")
            else:
                print(f"Writing batch of {len(bulk_operations)} parts to MongoDB...")
                result = db.parts.bulk_write(bulk_operations)
                updated_count += result.modified_count + result.upserted_count
                print(f"  Updated {result.modified_count} parts, inserted {result.upserted_count} new parts")
            
            bulk_operations = []
        
        # Show progress
        if processed_count % progress_interval == 0:
            elapsed = datetime.now() - start_time
            parts_per_second = processed_count / max(1, elapsed.total_seconds())
            estimated_total = elapsed.total_seconds() * (len(parts_data) / processed_count)
            estimated_remaining = max(0, estimated_total - elapsed.total_seconds())
            
            print(f"Processed {processed_count}/{len(parts_data)} parts "
                  f"({processed_count/len(parts_data)*100:.1f}%)... "
                  f"[{parts_per_second:.1f} parts/sec, "
                  f"~{estimated_remaining/60:.1f} min remaining]")
    
    # Execute any remaining operations
    if bulk_operations:
        if dry_run:
            print(f"DRY RUN: Would update {len(bulk_operations)} parts")
        else:
            print(f"Writing final batch of {len(bulk_operations)} parts to MongoDB...")
            result = db.parts.bulk_write(bulk_operations)
            updated_count += result.modified_count + result.upserted_count
            print(f"  Updated {result.modified_count} parts, inserted {result.upserted_count} new parts")
    
    total_time = datetime.now() - start_time
    print(f"Completed processing {processed_count} parts in {total_time}")
    if not dry_run:
        print(f"Total parts updated in MongoDB: {updated_count}")

def main():
    p = argparse.ArgumentParser(description="Simple Parts Updater - Updates quantities and notes")
    p.add_argument("--mongo-hostname", default="localhost", help="MongoDB hostname")
    p.add_argument("--mongo-port", type=int, default=27017, help="MongoDB port")
    p.add_argument("--database-name", default="migrationtest", help="MongoDB database name")
    p.add_argument("--parts-file", required=True, help="JSON file with parts data")
    p.add_argument("--parts-notes-file", required=True, help="JSON file with parts notes")
    p.add_argument("--batch-size", type=int, default=5000, help="Batch size for updates")
    p.add_argument("--dry-run", action="store_true", help="Preview operations without making changes")
    args = p.parse_args()
    
    try:
        print(f"Connecting to MongoDB at {args.mongo_hostname}:{args.mongo_port}...")
        client = MongoClient(
            host=args.mongo_hostname,
            port=args.mongo_port,
            socketTimeoutMS=60000,  # Longer timeout for large operations
            connectTimeoutMS=30000,
            serverSelectionTimeoutMS=30000,
            maxPoolSize=10  # Increase for more concurrency
        )
        db = client[args.database_name]
        
        # Test connection
        db.command("ping")
        print(f"Successfully connected to MongoDB database '{args.database_name}'")
        
        if args.dry_run:
            print("*** DRY RUN MODE - No changes will be made to the database ***")
        
        # Update parts
        start_time = datetime.now()
        update_parts(
            parts_file=args.parts_file,
            notes_file=args.parts_notes_file,
            db=db,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
        
        print(f"Script completed in {datetime.now() - start_time}")
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())