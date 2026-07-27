from pathlib import Path

from pyspark.sql import DataFrame

from src.utils.config import (
    PROCESSED_DIR,
    DIMENSIONS_DIR,
    FACTS_DIR
)
from src.utils.logger import get_logger

logger = get_logger(__name__)
class Load:

    def __init__(self):
        logger.info("Load Layer Initialized.")

    def write_parquet(
        self,
        df: DataFrame,
        output_path: Path,
        partition_cols: list[str] | None = None
    ):
        """
        Writes DataFrame to Parquet.
        """

        logger.info(f"Writing -> {output_path}")

        writer = (
            df.write
            .mode("overwrite")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.parquet(str(output_path))

        logger.info(
            f"Successfully written {df.count()} records."
        )

    def load_processed(self,df: DataFrame,table_name: str,partition_cols=None):
        """
        Loads processed datasets.
        """

        output_path = PROCESSED_DIR / table_name

        self.write_parquet(
            df=df,
            output_path=output_path,
            partition_cols=partition_cols
        )
    def load_dimension(
        self,
        df: DataFrame,
        table_name: str
    ):
        """
        Loads a dimension table.
        """

        output_path = DIMENSIONS_DIR / table_name

        self.write_parquet(
            df=df,
            output_path=output_path
        )
    def load_fact(self,df: DataFrame,table_name: str,partition_cols=None):
        """
        Loads fact table.
        """

        output_path = FACTS_DIR / table_name

        self.write_parquet(
            df=df,
            output_path=output_path,
            partition_cols=partition_cols
        )
    def load_all(self, transformed_data: dict):
        """
        Loads all transformed tables.
        """

        logger.info("=" * 70)
        logger.info("Starting Load Layer")
        logger.info("=" * 70)

        # Dimensions
        self.load_processed(
            transformed_data["customers"],
            "customers"
        )

        self.load_processed(
            transformed_data["products"],
            "products"
        )

        self.load_processed(
            transformed_data["stores"],
            "stores"
        )

        self.load_processed(
            transformed_data["suppliers"],
            "suppliers"
        )

        self.load_processed(
            transformed_data["employees"],
            "employees"
        )

        self.load_processed(
            transformed_data["promotions"],
            "promotions"
        )

        # Facts

        self.load_fact(
            transformed_data["orders"],
            "orders",
            partition_cols=[
                "order_year",
                "order_month"
            ]
        )

        self.load_fact(
            transformed_data["order_items"],
            "order_items"
        )

        self.load_fact(
            transformed_data["payments"],
            "payments"
        )

        self.load_fact(
            transformed_data["shipments"],
            "shipments"
        )

        self.load_fact(
            transformed_data["inventory"],
            "inventory"
        )

        self.load_fact(
            transformed_data["returns"],
            "returns"
        )

        logger.info("=" * 70)
        logger.info("Load Layer Completed Successfully.")
        logger.info("=" * 70)

    def load_warehouse(self,dimensions: dict,facts: dict):
        """
        Loads all warehouse tables.
        """

        logger.info("=" * 70)
        logger.info("Loading Warehouse")
        logger.info("=" * 70)

        for table_name, df in dimensions.items():
            self.load_dimension(df, table_name)

        for table_name, df in facts.items():
            self.load_fact(df, table_name)

        logger.info("=" * 70)
        logger.info("Warehouse Loaded Successfully")
        logger.info("=" * 70)
    