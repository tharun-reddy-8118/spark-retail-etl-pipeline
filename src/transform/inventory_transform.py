from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger=get_logger(__name__)

class InventoryTransform:
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans inventory data.
        """

        logger.info("Cleaning Inventory Data...")

        # Remove duplicate inventory IDs
        df = df.dropDuplicates(["inventory_id"])

        # Remove null primary key
        df = df.filter(col("inventory_id").isNotNull())

        # Remove null foreign keys
        df = df.filter(
            col("store_id").isNotNull() &
            col("product_id").isNotNull()
        )

        # Standardize stock status
        df = (
            df
            .withColumn("stock_status", trim(col("stock_status")))
            .withColumn("stock_status", upper(col("stock_status")))
        )

        logger.info("Inventory Cleaning Completed.")

        return df

    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates inventory data.
        """

        logger.info("Validating Inventory Data...")

        valid_status = [
            "IN STOCK",
            "LOW STOCK",
            "OUT OF STOCK"
        ]

        df = (
            df

            .withColumn(
                "is_valid_store_id",
                col("store_id").isNotNull() &
                (col("store_id") > 0)
            )

            .withColumn(
                "is_valid_product_id",
                col("product_id").isNotNull() &
                (col("product_id") > 0)
            )

            .withColumn(
                "is_valid_quantity",
                col("quantity_on_hand") >= 0
            )

            .withColumn(
                "is_valid_reorder_level",
                col("reorder_level") >= 0
            )

            .withColumn(
                "is_valid_stock_status",
                col("stock_status").isin(valid_status)
            )

            .withColumn(
                "is_valid_restock_date",
                col("last_restock_date") <= current_date()
            )

            .withColumn(
                "is_valid_last_updated",
                col("last_updated") >= col("last_restock_date")
            )
        )

        logger.info("Inventory Validation Completed.")

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to inventory data.
        """

        logger.info("Applying Inventory Business Rules...")

        df = (
            df

            .withColumn(
                "needs_restock",
                col("quantity_on_hand") <= col("reorder_level")
            )

            .withColumn(
                "inventory_age_days",
                datediff(
                    current_date(),
                    col("last_restock_date")
                )
            )

            .withColumn(
                "stock_category",
                when(col("quantity_on_hand") == 0, "OUT OF STOCK")
                .when(col("quantity_on_hand") <= col("reorder_level"), "LOW STOCK")
                .when(col("quantity_on_hand") <= (col("reorder_level") * 2), "MEDIUM STOCK")
                .otherwise("HIGH STOCK")
            )

            .withColumn(
                "stock_difference",
                col("quantity_on_hand") - col("reorder_level")
            )

            .withColumn(
                "inventory_health",
                when(col("quantity_on_hand") == 0, "CRITICAL")
                .when(col("quantity_on_hand") <= col("reorder_level"), "ATTENTION")
                .otherwise("HEALTHY")
            )
        )

        logger.info("Inventory Business Rules Applied Successfully.")

        return df
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete inventory transformation pipeline.
        """

        logger.info("Starting Inventory Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Inventory Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records            : {initial_count}")
        logger.info(f"After Cleaning             : {cleaned_count}")
        logger.info(f"Final Records              : {final_count}")
        logger.info(f"Duplicates Removed         : {initial_count - cleaned_count}")

        logger.info(f"Invalid Store IDs          : {df.filter(~col('is_valid_store_id')).count()}")
        logger.info(f"Invalid Product IDs        : {df.filter(~col('is_valid_product_id')).count()}")
        logger.info(f"Invalid Quantity           : {df.filter(~col('is_valid_quantity')).count()}")
        logger.info(f"Invalid Reorder Level      : {df.filter(~col('is_valid_reorder_level')).count()}")
        logger.info(f"Invalid Stock Status       : {df.filter(~col('is_valid_stock_status')).count()}")
        logger.info(f"Invalid Restock Date       : {df.filter(~col('is_valid_restock_date')).count()}")
        logger.info(f"Invalid Last Updated       : {df.filter(~col('is_valid_last_updated')).count()}")

        logger.info("Inventory Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df