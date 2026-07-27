from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger=get_logger(__name__)

class ShipmentTransform:
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans shipment data.
        """

        logger.info("Cleaning Shipment Data...")

        # Remove duplicate shipment IDs
        df = df.dropDuplicates(["shipment_id"])

        # Remove null primary key
        df = df.filter(col("shipment_id").isNotNull())

        # Remove null foreign keys
        df = df.filter(
            col("order_id").isNotNull() &
            col("store_id").isNotNull()
        )

        # Trim string columns
        df = (
            df
            .withColumn("shipping_status", trim(col("shipping_status")))
            .withColumn("courier_name", trim(col("courier_name")))
            .withColumn("tracking_number", trim(col("tracking_number")))
        )

        # Standardize status
        df = (
            df
            .withColumn("shipping_status", upper(col("shipping_status")))
        )

        logger.info("Shipment Cleaning Completed.")

        return df

    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates shipment data.
        """

        logger.info("Validating Shipment Data...")

        tracking_window = Window.partitionBy("tracking_number")

        valid_status = [
            "PENDING",
            "SHIPPED",
            "IN TRANSIT",
            "OUT FOR DELIVERY",
            "DELIVERED",
            "RETURNED",
            "CANCELLED"
        ]

        valid_couriers = [
            "DTDC",
            "BLUE DART",
            "DELHIVERY",
            "EKART",
            "XPRESSBEES",
            "INDIA POST",
            "ECOM EXPRESS"
        ]

        df = (
            df

            .withColumn(
                "is_valid_order_id",
                col("order_id").isNotNull() &
                (col("order_id") > 0)
            )

            .withColumn(
                "is_valid_store_id",
                col("store_id").isNotNull() &
                (col("store_id") > 0)
            )

            .withColumn(
                "is_valid_tracking_number",
                col("tracking_number").isNotNull()
            )

            .withColumn(
                "tracking_count",
                count("*").over(tracking_window)
            )

            .withColumn(
                "is_unique_tracking_number",
                col("tracking_count") == 1
            )

            .withColumn(
                "is_valid_shipment_date",
                col("shipment_date") <= current_date()
            )

            .withColumn(
                "is_valid_delivery_date",
                col("delivery_date") >= col("shipment_date")
            )

            .withColumn(
                "is_valid_shipping_status",
                col("shipping_status").isin(valid_status)
            )

            .withColumn(
                "is_valid_courier",
                col("courier_name").isin(valid_couriers)
            )

            .drop("tracking_count")
        )

        logger.info("Shipment Validation Completed.")

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to shipment data.
        """

        logger.info("Applying Shipment Business Rules...")

        df = (
            df

            .withColumn(
                "delivery_days",
                datediff(
                    col("delivery_date"),
                    col("shipment_date")
                )
            )

            .withColumn(
                "delivery_speed",
                when(col("delivery_days") <= 2, "EXPRESS")
                .when(col("delivery_days") <= 5, "STANDARD")
                .otherwise("ECONOMY")
            )

            .withColumn(
                "is_delivered",
                col("shipping_status") == "DELIVERED"
            )

            .withColumn(
                "is_in_transit",
                col("shipping_status") == "IN TRANSIT"
            )

            .withColumn(
                "is_pending",
                col("shipping_status") == "PENDING"
            )

            .withColumn(
                "is_returned",
                col("shipping_status") == "RETURNED"
            )

            .withColumn(
                "is_cancelled",
                col("shipping_status") == "CANCELLED"
            )

            .withColumn(
                "shipment_age_days",
                datediff(
                    current_date(),
                    col("shipment_date")
                )
            )
        )

        logger.info("Shipment Business Rules Applied Successfully.")

        return df
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete shipment transformation pipeline.
        """

        logger.info("Starting Shipment Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Shipment Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records              : {initial_count}")
        logger.info(f"After Cleaning               : {cleaned_count}")
        logger.info(f"Final Records                : {final_count}")
        logger.info(f"Duplicates Removed           : {initial_count - cleaned_count}")

        logger.info(f"Invalid Order IDs            : {df.filter(~col('is_valid_order_id')).count()}")
        logger.info(f"Invalid Store IDs            : {df.filter(~col('is_valid_store_id')).count()}")
        logger.info(f"Missing Tracking Numbers     : {df.filter(~col('is_valid_tracking_number')).count()}")
        logger.info(f"Duplicate Tracking Numbers   : {df.filter(~col('is_unique_tracking_number')).count()}")
        logger.info(f"Invalid Shipment Dates       : {df.filter(~col('is_valid_shipment_date')).count()}")
        logger.info(f"Invalid Delivery Dates       : {df.filter(~col('is_valid_delivery_date')).count()}")
        logger.info(f"Invalid Shipping Status      : {df.filter(~col('is_valid_shipping_status')).count()}")
        logger.info(f"Invalid Courier              : {df.filter(~col('is_valid_courier')).count()}")

        logger.info("Shipment Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df