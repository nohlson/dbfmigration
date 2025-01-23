import json
import pymongo
from bson.objectid import ObjectId

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

###################################
# Contact Creation Helper
###################################
def create_contact(db, full_name, phone="", fax="", email="", company="", title=""):
    """
    Creates a new Contact document in MongoDB and returns its ObjectId.
    full_name is stored as first_name, last_name is left blank.
    phone and fax (if present) go into phone_numbers array, email => email_addresses.
    """
    if not full_name:
        # If there's truly no contact name, we might skip creating an empty contact
        # and just return None. But let's create it for consistency.
        full_name = ""

    contact_doc = {
        "first_name": full_name,
        "last_name": "",
        "email_addresses": [],
        "phone_numbers": [],
        "title": title,
        "company": company
    }

    # If phone or fax exist, store them
    phone_list = []
    if phone:
        phone_list.append(phone)
    if fax:
        phone_list.append(fax)
    if phone_list:
        contact_doc["phone_numbers"] = phone_list

    # If email present
    if email:
        contact_doc["email_addresses"].append(email)

    result = db.contacts.insert_one(contact_doc)
    return result.inserted_id

###################################
# Load Suppliers
###################################
def load_suppliers(suppliers_file, db):
    """
    1) Read supplier data from JSON
    2) Create a Contact doc for each 'CONTACT'
    3) Create a Supplier doc referencing that contact
    4) Return a map: { legacy_vendor_code: new_supplier_objectid }
    """
    with open(suppliers_file, 'r', encoding='utf-8') as f:
        supplier_data = json.load(f)

    supplier_id_map = {}
    new_supplier_docs = []

    for item in supplier_data:
        vendno = safe_str(item.get("VENDNO"))

        # Create a contact doc for this supplier, if any
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
            "company_name": safe_str(item.get("COMPANY", "Unknown Company")) or "Unknown Company",
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
            "payment_method": ""  # Put logic here if you have a field for payment_method
        }

        # If we got a valid contact_id, reference it
        if contact_id:
            supplier_doc["contacts"].append(contact_id)

        new_supplier_docs.append(supplier_doc)

    # Insert them all at once
    result = db.suppliers.insert_many(new_supplier_docs)

    # Build map from vendno -> inserted_id
    for old_item, inserted_id in zip(supplier_data, result.inserted_ids):
        vendno = safe_str(old_item.get("VENDNO"))
        supplier_id_map[vendno] = inserted_id

    return supplier_id_map

###################################
# Load Customers
###################################
def load_customers(customers_file, db):
    """
    1) Read customer data from JSON
    2) Create a Contact doc for each 'CONTACT'
    3) Create a Customer doc referencing that contact
    4) Return a map: { legacy_customer_code: new_customer_objectid }
    """
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
            company=safe_str(item.get("COMPANY")),
            title=""  # or item.get("TITLE") if they have it
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
            "contacts": [],
            "collect_account": [],  # If you have to fill in from other fields, do so
            "terms": safe_str(item.get("PTERMS")),
            "notes": safe_str(item.get("COMMENT")),
        }

        # Add contact reference
        if contact_id:
            customer_doc["contacts"].append(contact_id)

        new_customer_docs.append(customer_doc)

    # Insert them
    result = db.customers.insert_many(new_customer_docs)

    # Build map from custno -> inserted_id
    for old_item, inserted_id in zip(customer_data, result.inserted_ids):
        custno = safe_str(old_item.get("CUSTNO"))
        customer_id_map[custno] = inserted_id

    return customer_id_map

###################################
# Load Parts
###################################
def load_parts(parts_file, db, supplier_id_map):
    """
    1) Read parts data from JSON
    2) Lookup part's supplier in supplier_id_map
    3) Create a Part doc referencing the supplier
    """
    with open(parts_file, 'r', encoding='utf-8') as f:
        parts_data = json.load(f)

    new_part_docs = []
    for item in parts_data:
        # This is the old code that references the supplier
        legacy_supplier_code = safe_str(item.get("SUPPLIER"))

        # Look up the new ObjectId from your map
        supplier_ref = supplier_id_map.get(legacy_supplier_code)

        part_doc = {
            "item_number": safe_str(item.get("ITEM")),
            "description": safe_str(item.get("DESCRIP")),
            "suppliers": [],
            "quantity_on_hand": safe_convert_int(item.get("ONHAND"), 0),
            "default_price": int(safe_convert_float(item.get("PRICE"), 0.0) * 100),  # store in cents
            "alternate_part_id": []
        }

        # If we found a supplier
        if supplier_ref:
            part_doc["suppliers"].append(supplier_ref)

        new_part_docs.append(part_doc)

    db.parts.insert_many(new_part_docs)

###################################
# Main Migration Script
###################################
def main():
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["migrationtest"]

    # 1) Suppliers (and their Contacts)
    supplier_id_map = load_suppliers("json/SupplierInfo.json", db)
    print(f"Imported {len(supplier_id_map)} suppliers.")

    # 2) Customers (and their Contacts)
    customer_id_map = load_customers("json/CustomerInfo.json", db)
    print(f"Imported {len(customer_id_map)} customers.")

    # 3) Parts (reference suppliers)
    load_parts("json/Inventory.json", db, supplier_id_map)
    print("Imported parts referencing suppliers.")

    print("Data migration completed successfully.")

if __name__ == "__main__":
    main()