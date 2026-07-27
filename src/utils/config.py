from pathlib import Path

# ==============================
# Project Root Directory
# ==============================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# ==============================
# Data Directories
# ==============================
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
STAGING_DATA_DIR = DATA_DIR / "staging"
PROCESSED_DIR = DATA_DIR / "processed"


# Warehouse
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"

DIMENSIONS_DIR = WAREHOUSE_DIR / "dimensions"
FACTS_DIR = WAREHOUSE_DIR / "facts"


OUTPUT_DIR = PROJECT_ROOT / "output"

# ==============================
# Raw Data Files
# ==============================

CUSTOMERS_FILE = RAW_DATA_DIR / "customers.csv"
EMPLOYEES_FILE = RAW_DATA_DIR / "employees.csv"
INVENTORY_FILE = RAW_DATA_DIR / "inventory.csv"
ORDER_ITEMS_FILE = RAW_DATA_DIR / "order_items.csv"
ORDERS_FILE = RAW_DATA_DIR / "orders.csv"
PAYMENTS_FILE = RAW_DATA_DIR / "payments.csv"
PRODUCTS_FILE = RAW_DATA_DIR / "products.csv"
PROMOTIONS_FILE = RAW_DATA_DIR / "promotions.csv"
RETURNS_FILE = RAW_DATA_DIR / "returns.csv"
SHIPMENTS_FILE = RAW_DATA_DIR / "shipments.csv"
STORES_FILE = RAW_DATA_DIR / "stores.csv"
SUPPLIERS_FILE = RAW_DATA_DIR / "suppliers.csv"





# ==============================
# Spark Configuration
# ==============================

APP_NAME = "Retail ETL Project"

SHUFFLE_PARTITIONS = 8

LOG_LEVEL = "WARN"

# ==============================
# Create Directories
# ==============================

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
STAGING_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DIMENSIONS_DIR.mkdir(parents=True, exist_ok=True)
FACTS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)