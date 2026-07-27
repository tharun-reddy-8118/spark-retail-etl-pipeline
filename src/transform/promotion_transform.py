from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql.functions import length


logger = get_logger(__name__)

class PromotionTransform:
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans promotion data.
        """

        logger.info("Cleaning Promotion Data...")

        df = df.dropDuplicates(["promotion_id"])

        df = df.filter(col("promotion_id").isNotNull())

        df = (
            df
            .withColumn("promotion_name", trim(col("promotion_name")))
            .withColumn("promotion_type", initcap(trim(col("promotion_type"))))
            .withColumn("applicable_category", initcap(trim(col("applicable_category"))))
            .withColumn("coupon_code", upper(trim(col("coupon_code"))))
            .withColumn("status", upper(trim(col("status"))))
        )

        logger.info("Promotion Cleaning Completed.")

        return df



    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates promotion data.
        """

        logger.info("Validating Promotion Data...")

        valid_types = [
            "Percentage",
            "Flat",
            "Buy One Get One"
        ]

        valid_status = [
            "UPCOMING",
            "ACTIVE",
            "EXPIRED"
        ]

        df = (
            df

            .withColumn(
                "is_valid_promotion_name",
                col("promotion_name").isNotNull()
            )

            .withColumn(
                "is_valid_promotion_type",
                col("promotion_type").isin(valid_types)
            )

            .withColumn(
                "is_valid_discount_value",
                when(
                    col("promotion_type") == "Percentage",
                    col("discount_value").between(0, 100)
                ).otherwise(
                    col("discount_value") >= 0
                )
            )

            .withColumn(
                "is_valid_date_range",
                col("end_date") >= col("start_date")
            )

            .withColumn(
                "is_valid_coupon_code",
                col("coupon_code").isNotNull() &
                (length(col("coupon_code")) > 0)
            )

            .withColumn(
                "is_valid_minimum_order_amount",
                col("minimum_order_amount") >= 0
            )

            .withColumn(
                "is_valid_status",
                col("status").isin(valid_status)
            )

            .withColumn(
                "is_valid_active_dates",
                when(
                    col("status") == "ACTIVE",
                    col("start_date") <= current_date()
                ).otherwise(True)
            )
        )

        logger.info("Promotion Validation Completed.")

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to promotion data.
        """

        logger.info("Applying Promotion Business Rules...")

        df = (
            df

            .withColumn(
                "promotion_duration_days",
                datediff(
                    col("end_date"),
                    col("start_date")
                )
            )

            .withColumn(
                "discount_category",
                when(col("discount_value") < 10, "LOW")
                .when(col("discount_value") < 30, "MEDIUM")
                .when(col("discount_value") < 50, "HIGH")
                .otherwise("MEGA")
            )

            .withColumn(
                "is_percentage_offer",
                col("promotion_type") == "Percentage"
            )

            .withColumn(
                "is_flat_offer",
                col("promotion_type") == "Flat"
            )

            .withColumn(
                "is_bogo_offer",
                col("promotion_type") == "Buy One Get One"
            )

            .withColumn(
                "promotion_lifecycle",
                when(col("status") == "UPCOMING", "PLANNED")
                .when(col("status") == "ACTIVE", "RUNNING")
                .otherwise("COMPLETED")
            )
        )

        logger.info("Promotion Business Rules Applied Successfully.")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete promotion transformation pipeline.
        """

        logger.info("Starting Promotion Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Promotion Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records                  : {initial_count}")
        logger.info(f"After Cleaning                   : {cleaned_count}")
        logger.info(f"Final Records                    : {final_count}")
        logger.info(f"Duplicates Removed               : {initial_count - cleaned_count}")

        logger.info(f"Invalid Promotion Name           : {df.filter(~col('is_valid_promotion_name')).count()}")
        logger.info(f"Invalid Promotion Type           : {df.filter(~col('is_valid_promotion_type')).count()}")
        logger.info(f"Invalid Discount Value           : {df.filter(~col('is_valid_discount_value')).count()}")
        logger.info(f"Invalid Date Range               : {df.filter(~col('is_valid_date_range')).count()}")
        logger.info(f"Missing Coupon Code              : {df.filter(~col('is_valid_coupon_code')).count()}")
        logger.info(f"Invalid Minimum Order Amount     : {df.filter(~col('is_valid_minimum_order_amount')).count()}")
        logger.info(f"Invalid Status                   : {df.filter(~col('is_valid_status')).count()}")
        logger.info(f"Invalid Active Promotion Dates   : {df.filter(~col('is_valid_active_dates')).count()}")

        logger.info("Promotion Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df