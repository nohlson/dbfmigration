# dbfmigration
DBF to JSON Migration Tool

Inventory.json: ARINVT01
"All parts in inventory"
CustomerInfo.json: ARCUST01
"Customer information like address"
SupplierInfo.json: APVEND01
"Supplier/vendor information like address"
CustomerShipping.json: SOADDR01
"Connects customer numbers to the shipping addresses in sales orders"


For Convert.py:
Create a directory called `json` at top level, include `Inventory.json`, `CustomerInfo.json`, `SupplierInfo.json`, and `CustomerShipping.json`. These will be command line arguments
for `Convert.py`