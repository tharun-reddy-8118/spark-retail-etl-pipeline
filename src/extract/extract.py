from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import get_logger
logger = get_logger(__name__)

from src.utils.config import (
    CUSTOMERS_FILE,
    EMPLOYEES_FILE,
    INVENTORY_FILE,
    ORDER_ITEMS_FILE,
    ORDERS_FILE,
    PAYMENTS_FILE,
    PRODUCTS_FILE,
    PROMOTIONS_FILE,
    RETURNS_FILE,
    SHIPMENTS_FILE,
    STORES_FILE,
    SUPPLIERS_FILE,
)


class Extract:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def read_csv(self,file_path: Path)->DataFrame:
        """
        Reads a CSV file into a Spark DataFrame.

        Args:
            file_path (Path): Path to the CSV file.

        Returns:
            DataFrame
        """
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        logger.info(f"Reading {file_path.name}")
        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("escape", "\"")
            .csv(str(file_path))
        )
    def extract_all(self):
        """
        Reads all source CSV files and returns a dictionary of DataFrames.
        """

        logger.info("Starting data extraction...")

        dataframes = {
            "customers": self.read_csv(CUSTOMERS_FILE),
            "employees": self.read_csv(EMPLOYEES_FILE),
            "inventory": self.read_csv(INVENTORY_FILE),
            "order_items": self.read_csv(ORDER_ITEMS_FILE),
            "orders": self.read_csv(ORDERS_FILE),
            "payments": self.read_csv(PAYMENTS_FILE),
            "products": self.read_csv(PRODUCTS_FILE),
            "promotions": self.read_csv(PROMOTIONS_FILE),
            "returns": self.read_csv(RETURNS_FILE),
            "shipments": self.read_csv(SHIPMENTS_FILE),
            "stores": self.read_csv(STORES_FILE),
            "suppliers": self.read_csv(SUPPLIERS_FILE),
        }

        logger.info("Data extraction completed successfully.")

        return dataframes
            

    