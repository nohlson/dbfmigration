#!/usr/bin/env python3
"""
Contact Company Reference Updater

This script updates all contacts in the MongoDB database by:
1. Finding contacts with company names but missing company_id/company_type
2. Searching for matching customers or suppliers by company name
3. Adding the appropriate company_id and company_type fields

Supports dry-run mode to preview updates without modifying the database.
"""
import argparse
import sys
import re
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId


def safe_str(value, default=""):
    """Convert value to string safely, returning default if None."""
    if value is None:
        return default
    return str(value).strip()


def main():
    parser = argparse.ArgumentParser(
        description="Update contact company references in MongoDB"
    )
    parser.add_argument("--mongo-hostname", default="localhost",
                        help="MongoDB hostname")
    parser.add_argument("--mongo-port", type=int, default=27017,
                        help="MongoDB port")
    parser.add_argument("--database-name", default="migrationtest",
                        help="MongoDB database name")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview updates without writing to MongoDB")
    args = parser.parse_args()

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

    # Find all contacts
    contacts = list(db.contacts.find())
    if not contacts:
        print("No contacts found in database.")
        sys.exit(0)
    
    print(f"Found {len(contacts)} contacts in database")

    # Process each contact
    updated = 0
    no_company = 0
    already_linked = 0
    not_found = 0

    for contact in contacts:
        contact_id = contact.get("_id")
        company_name = safe_str(contact.get("company"))
        
        # Skip contacts with no company name
        if not company_name:
            print(f"SKIP: Contact {contact_id} ({contact.get('first_name')} {contact.get('last_name')}) has no company.")
            no_company += 1
            continue

        # Skip contacts already linked to a company
        if contact.get("company_id") and contact.get("company_type"):
            print(f"SKIP: Contact {contact_id} already linked to {contact.get('company_type')} ID: {contact.get('company_id')}")
            already_linked += 1
            continue

        # Try to find matching customer
        # Use case-insensitive regex search for more flexible matching
        regex_pattern = re.compile(f"^{re.escape(company_name)}$", re.IGNORECASE)
        customer = db.customers.find_one({"company_name": regex_pattern})
        
        if customer:
            # Found a customer match
            update_data = {
                "company_id": customer["_id"],
                "company_type": "customer",
                "updatedAt": datetime.utcnow()
            }
            
            if args.dry_run:
                print(f"DRY RUN: Would update contact {contact_id} ({contact.get('first_name')} {contact.get('last_name')}) "
                      f"with customer {customer['_id']} (company: {company_name})")
            else:
                db.contacts.update_one(
                    {"_id": contact_id}, 
                    {"$set": update_data}
                )
                print(f"Updated contact {contact_id} with customer {customer['_id']} (company: {company_name})")
            
            updated += 1
            continue
        
        # If no customer found, try supplier
        supplier = db.suppliers.find_one({"company_name": regex_pattern})
        
        if supplier:
            # Found a supplier match
            update_data = {
                "company_id": supplier["_id"],
                "company_type": "supplier",
                "updatedAt": datetime.utcnow()
            }
            
            if args.dry_run:
                print(f"DRY RUN: Would update contact {contact_id} ({contact.get('first_name')} {contact.get('last_name')}) "
                      f"with supplier {supplier['_id']} (company: {company_name})")
            else:
                db.contacts.update_one(
                    {"_id": contact_id}, 
                    {"$set": update_data}
                )
                print(f"Updated contact {contact_id} with supplier {supplier['_id']} (company: {company_name})")
            
            updated += 1
            continue
            
        # No matching customer or supplier found
        print(f"WARNING: No customer or supplier found for company '{company_name}' (Contact: {contact_id})")
        not_found += 1

    print(f"\nSummary:")
    print(f"  Total contacts: {len(contacts)}")
    print(f"  Updated: {updated}")
    print(f"  No company name: {no_company}")
    print(f"  Already linked: {already_linked}")
    print(f"  Company not found: {not_found}")
    
    if args.dry_run:
        print("\nThis was a dry run. No changes were made to the database.")
    else:
        print("\nUpdates completed.")


if __name__ == "__main__":
    main()