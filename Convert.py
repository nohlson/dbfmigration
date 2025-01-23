import json
import pymongo
from collections import defaultdict
from bson.objectid import ObjectId
import argparse

##############################
# Helper Conversion Functions
##############################
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

def safe_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()

##############################
# Helper: parse "CITY, STATE ZIP"
##############################
def parse_city_state_zip(line):
    """
    Expects something like: "NEWTON, NC 28658"
    Returns (city, state, zip) => ("NEWTON", "NC", "28658")
    If parsing fails or format is off, we do best-effort or fallback.
    """
    line = safe_str(line)
    if not line:
        return ("", "", "")
    
    parts = line.split(",")
    if len(parts) < 2:
        # No comma => can't parse well. 
        # We'll put everything in city, leaving state/zip blank
        return (line, "", "")

    # City is everything before the first comma
    city = parts[0].strip()

    # The remainder is typically "NC 28658"
    remainder = parts[1].strip()
    subparts = remainder.split()
    if len(subparts) >= 2:
        state = subparts[0]
        zip_ = subparts[1]
    else:
        # If there's only one subpart, let's treat it as state.
        state = remainder
        zip_ = ""

    return (city, state, zip_)

###################################
# Contact Creation Helper
###################################
def create_contact(db, full_name, phone="", fax="", email="", company="", title=""):
    """
    Creates a new Contact document in 'contacts' collection; returns the _id.
    We'll treat full_name as 'first_name' (leaving last_name blank).
    """
    if not full_name:
        full_name = ""

    contact_doc = {
        "first_name": full_name,
        "last_name": "",
        "email_addresses": [],
        "phone_numbers": [],
        "title": title,
        "company": company
    }

    phone_list = []
    if phone:
        phone_list.append(phone)
    if fax:
        phone_list.append(fax)
    if phone_list:
        contact_doc["phone_numbers"] = phone_list

    if email:
        contact_doc["email_addresses"].append(email)

    result = db.contacts.insert_one(contact_doc)
    return result.inserted_id

###################################
# Load Suppliers
###################################
def load_suppliers(suppliers_file, db):
    with open(suppliers_file, 'r', encoding='utf-8') as f:
        supplier_data = json.load(f)

    supplier_id_map = {}
    new_supplier_docs = []

    for item in supplier_data:
        vendno = safe_str(item.get("VENDNO"))

        # Create a Contact doc for this supplier
        contact_id = create_contact(
            db=db,
            full_name=safe_str(item.get("CONTACT")),
            phone=safe_str(item.get("PHONE")),
            fax=safe_str(item.get("FAXNO")),
            email=safe_str(item.get("EMAIL")),
            company=safe_str(item.get("COMPANY")),
            title=safe_str(item.get("TITLE"))
        )

        supplier_doc = {
            "vendor_number": vendno,
            "company_name": safe_str(item.get("COMPANY")) or "Unknown Company",
            "phone_contacts": [safe_str(item["PHONE"])] if item.get("PHONE") else [],
            "fax_contact": safe_str(item.get("FAXNO")),
            "address": {
                "street": safe_str(item.get("ADDRESS1")),
                "unit": safe_str(item.get("ADDRESS2")),
                "city": safe_str(item.get("CITY")),
                "state": safe_str(item.get("STATE")),
                "country": safe_str(item.get("COUNTRY")),
                "zip": safe_str(item.get("ZIP")),
            },
            "contacts": [],
            "terms": safe_str(item.get("PTERMS")),
            "standard_discount": safe_convert_float(item.get("PDISC"), 0.0),
            "notes": safe_str(item.get("COMMENT")),
            "vendor_email": safe_str(item.get("EMAIL")),
            "payment_method": ""
        }

        if contact_id:
            supplier_doc["contacts"].append(contact_id)

        new_supplier_docs.append(supplier_doc)

    result = db.suppliers.insert_many(new_supplier_docs)

    for old_item, inserted_id in zip(supplier_data, result.inserted_ids):
        vendno = safe_str(old_item.get("VENDNO"))
        supplier_id_map[vendno] = inserted_id

    return supplier_id_map

###################################
# Load Customers
###################################
def load_customers(customers_file, db):
    with open(customers_file, 'r', encoding='utf-8') as f:
        customer_data = json.load(f)

    customer_id_map = {}
    new_customer_docs = []

    for item in customer_data:
        custno = safe_str(item.get("CUSTNO"))

        # Create a contact doc for this customer
        contact_id = create_contact(
            db=db,
            full_name=safe_str(item.get("CONTACT")),
            phone=safe_str(item.get("PHONE")),
            fax=safe_str(item.get("FAXNO")),
            email=safe_str(item.get("EMAIL")),
            company=safe_str(item.get("COMPANY"))
        )

        customer_doc = {
            "customer_number": custno,
            "company_name": safe_str(item.get("COMPANY")) or "Unknown Customer",
            "phone_contacts": [safe_str(item["PHONE"])] if item.get("PHONE") else [],
            "address": {
                "street": safe_str(item.get("ADDRESS1")),
                "unit": safe_str(item.get("ADDRESS2")),
                "city": safe_str(item.get("CITY")),
                "state": safe_str(item.get("STATE")),
                "country": safe_str(item.get("COUNTRY")),
                "zip": safe_str(item.get("ZIP")),
            },
            # new array for shipping addresses is empty by default
            "shipping_addresses": [],
            "contacts": [],
            "collect_account": [],  # Fill in if you have data
            "terms": safe_str(item.get("PTERMS")),
            "notes": safe_str(item.get("COMMENT")),
        }

        if contact_id:
            customer_doc["contacts"].append(contact_id)

        new_customer_docs.append(customer_doc)

    result = db.customers.insert_many(new_customer_docs)

    for old_item, inserted_id in zip(customer_data, result.inserted_ids):
        custno = safe_str(old_item.get("CUSTNO"))
        customer_id_map[custno] = inserted_id

    return customer_id_map

###################################
# Load Parts
###################################
def load_parts(parts_file, db, supplier_id_map):
    with open(parts_file, 'r', encoding='utf-8') as f:
        parts_data = json.load(f)

    new_part_docs = []
    for item in parts_data:
        legacy_supplier_code = safe_str(item.get("SUPPLIER"))
        supplier_ref = supplier_id_map.get(legacy_supplier_code)

        part_doc = {
            "item_number": safe_str(item.get("ITEM")),
            "description": safe_str(item.get("DESCRIP")),
            "suppliers": [],
            "quantity_on_hand": safe_convert_int(item.get("ONHAND"), 0),
            "default_price": int(safe_convert_float(item.get("PRICE"), 0.0) * 100),
            "alternate_part_id": []
        }

        if supplier_ref:
            part_doc["suppliers"].append(supplier_ref)

        new_part_docs.append(part_doc)

    if new_part_docs:
        db.parts.insert_many(new_part_docs)

###################################
# NEW: Load Shipping Addresses
###################################
def load_customer_shipping_addresses(shipping_file, db, customer_id_map):
    """
    Reads a JSON file containing shipping addresses for customers, e.g.:

    {
      "CUSTNO": "UNITED",
      "SONO": "   51550",
      "ADTYPE": "",
      "COMPANY": "UNITED GLOVE COMPANY",
      "ADDRESS1": "2017 N. STEWART AVENUE",
      "ADDRESS2": "",
      "ADDRESS3": "NEWTON, NC 28658"
    }

    We'll parse 'ADDRESS3' for city, state, zip. We'll treat "COMPANY" as
    the 'name' of the shipping address, 'ADDRESS1' = street, 'ADDRESS2' = unit, etc.

    We group addresses by CUSTNO, then do a single update per customer
    pushing all the addresses into "shipping_addresses".
    """
    with open(shipping_file, 'r', encoding='utf-8') as f:
        shipping_data = json.load(f)

    # Group addresses by customer number
    shipping_dict = defaultdict(list)

    for item in shipping_data:
        custno = safe_str(item.get("CUSTNO"))
        name = safe_str(item.get("COMPANY"))
        street = safe_str(item.get("ADDRESS1"))
        unit = safe_str(item.get("ADDRESS2"))
        addr3 = safe_str(item.get("ADDRESS3"))

        city, state, zip_ = parse_city_state_zip(addr3)

        # Build a shipping address document matching addressSchema
        shipping_addr = {
            "name": name,
            "street": street,
            "unit": unit,
            "city": city,
            "state": state,
            "country": "",  # Not provided in this data
            "zip": zip_,
            "isDefault": False  # or True if you have logic for defaults
        }

        shipping_dict[custno].append(shipping_addr)

    # Now update each customer's shipping_addresses array
    for custno, addresses in shipping_dict.items():
        if not addresses:
            continue

        # Find the new ObjectId from the map
        cust_id = customer_id_map.get(custno)
        if not cust_id:
            # If there's no matching customer, skip or log a warning
            print(f"WARNING: no matching customer for CUSTNO='{custno}' - skipping shipping addresses.")
            continue

        db.customers.update_one(
            {"_id": cust_id},
            {
                # Push all addresses at once
                "$push": {
                    "shipping_addresses": {"$each": addresses}
                }
            }
        )

###################################
# Main Migration Script
###################################
def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Migrate data to MongoDB.")
    parser.add_argument("--mongo-hostname", type=str, default="localhost", help="MongoDB hostname")
    parser.add_argument("--database-name", type=str, default="migrationtest", help="Database name")
    parser.add_argument("--suppliers-file", type=str, required=True, help="Path to suppliers JSON file")
    parser.add_argument("--customers-file", type=str, required=True, help="Path to customers JSON file")
    parser.add_argument("--parts-file", type=str, required=True, help="Path to parts JSON file")
    parser.add_argument("--shipping-file", type=str, required=True, help="Path to customer shipping addresses JSON file")

    args = parser.parse_args()

    MONGO_HOSTNAME = args.mongo_hostname
    DATABASE_NAME = args.database_name
    suppliers_file = args.suppliers_file
    customers_file = args.customers_file
    parts_file = args.parts_file
    shipping_file = args.shipping_file

    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://" + MONGO_HOSTNAME + ":27017/")
    db = client[DATABASE_NAME]

    # 1) Suppliers (and their Contacts)
    supplier_id_map = load_suppliers(suppliers_file, db)
    print(f"Imported {len(supplier_id_map)} suppliers.")

    # 2) Customers (and their Contacts)
    customer_id_map = load_customers(customers_file, db)
    print(f"Imported {len(customer_id_map)} customers.")

    # 3) Parts referencing suppliers
    load_parts(parts_file, db, supplier_id_map)
    print("Imported parts.")

    # 4) Shipping addresses for customers
    load_customer_shipping_addresses(shipping_file, db, customer_id_map)
    print("Updated customer shipping addresses.")

    print("Data migration completed successfully.")

if __name__ == "__main__":
    main()