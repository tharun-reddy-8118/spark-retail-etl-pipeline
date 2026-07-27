from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame

logger = get_logger(__name__)

class ReturnTransform:
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans return data.
        """

        logger.info("Cleaning Return Data...")

        df = df.dropDuplicates(["return_id"])

        df = df.filter(col("return_id").isNotNull())

        df = df.filter(
            col("order_id").isNotNull() &
            col("product_id").isNotNull()
        )

        df = (
            df
            .withColumn("return_reason", initcap(trim(col("return_reason"))))
            .withColumn("return_status", upper(trim(col("return_status"))))
        )

        logger.info("Return Cleaning Completed.")

        return df
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates return data.
        """

        logger.info("Validating Return Data...")

        valid_status = [
            "REQUESTED",
            "APPROVED",
            "REJECTED",
            "COMPLETED"
        ]

        valid_reasons = [
            "Damaged",
            "Wrong Item",
            "Defective",
            "Quality Issue",
            "Changed Mind",
            "Expired Product",
            "Other"
        ]

        df = (
            df

            .withColumn(
                "is_valid_order_id",
                col("order_id").isNotNull() &
                (col("order_id") > 0)
            )

            .withColumn(
                "is_valid_product_id",
                col("product_id").isNotNull() &
                (col("product_id") > 0)
            )

            .withColumn(
                "is_valid_return_date",
                col("return_date") <= current_date()
            )

            .withColumn(
                "is_valid_return_reason",
                col("return_reason").isin(valid_reasons)
            )

            .withColumn(
                "is_valid_refund_amount",
                col("refund_amount") >= 0
            )

            .withColumn(
                "is_valid_return_status",
                col("return_status").isin(valid_status)
            )
        )

        logger.info("Return Validation Completed.")

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to return data.
        """

        logger.info("Applying Return Business Rules...")

        df = (
            df

            .withColumn(
                "return_age_days",
                datediff(
                    current_date(),
                    col("return_date")
                )
            )

            .withColumn(
                "refund_category",
                when(col("refund_amount") < 1000, "LOW")
                .when(col("refund_amount") < 5000, "MEDIUM")
                .when(col("refund_amount") < 10000, "HIGH")
                .otherwise("PREMIUM")
            )

            .withColumn(
                "is_approved_return",
                col("return_status") == "APPROVED"
            )

            .withColumn(
                "is_completed_return",
                col("return_status") == "COMPLETED"
            )

            .withColumn(
                "is_rejected_return",
                col("return_status") == "REJECTED"
            )

            .withColumn(
                "requires_refund",
                col("return_status").isin(
                    "APPROVED",
                    "COMPLETED"
                )
            )
        )

        logger.info("Return Business Rules Applied Successfully.")

        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete return transformation pipeline.
        """

        logger.info("Starting Return Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Return Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records              : {initial_count}")
        logger.info(f"After Cleaning               : {cleaned_count}")
        logger.info(f"Final Records                : {final_count}")
        logger.info(f"Duplicates Removed           : {initial_count-cleaned_count}")

        logger.info(f"Invalid Order IDs            : {df.filter(~col('is_valid_order_id')).count()}")
        logger.info(f"Invalid Product IDs          : {df.filter(~col('is_valid_product_id')).count()}")
        logger.info(f"Invalid Return Dates         : {df.filter(~col('is_valid_return_date')).count()}")
        logger.info(f"Invalid Return Reasons       : {df.filter(~col('is_valid_return_reason')).count()}")
        logger.info(f"Invalid Refund Amounts       : {df.filter(~col('is_valid_refund_amount')).count()}")
        logger.info(f"Invalid Return Status        : {df.filter(~col('is_valid_return_status')).count()}")

        logger.info("Return Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df