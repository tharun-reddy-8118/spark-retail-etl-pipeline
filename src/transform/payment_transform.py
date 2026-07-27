from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger=get_logger(__name__)

class PaymentTransform:
    
    def clean_data(self,df:DataFrame)->DataFrame:
        """
        Cleans payment data.
        """
        logger.info("Cleaning payment data...")

        # Remove duplicate payment IDs
        df = df.dropDuplicates(["payment_id"])

        # Remove records with null primary key
        df = df.filter(col("payment_id").isNotNull())

        # Remove records with null order ID
        df = df.filter(col("order_id").isNotNull())

        # Trim string columns
        df = (
            df
            .withColumn("payment_method", trim(col("payment_method")))
            .withColumn("payment_status", trim(col("payment_status")))
            .withColumn("transaction_id", trim(col("transaction_id")))
        )

        # Standardize values
        df = (
            df
            .withColumn("payment_method", upper(col("payment_method")))
            .withColumn("payment_status", upper(col("payment_status")))
        )

        logger.info("Payment Cleaning Completed.")

        return df

    def validate_data(self,df:DataFrame)->DataFrame:
        """
        Validates Payment Data
        """

        logger.info("Validating the payments data")
        txn_window = Window.partitionBy("transaction_id")

        valid_methods = [
            "CASH",
            "CARD",
            "UPI",
            "NET BANKING",
            "WALLET"
        ]

        valid_status =[
            "SUCCESS",
            "FAILED",
            "PENDING",
            "REFUNDED"
        ]

        df=(
            df

            #orderID
            .withColumn(
                "is_valid_order_id",
                col("order_id").isNotNull() &
                (col("order_id") > 0)
            )

            # Amount
            .withColumn(
                "is_valid_amount_paid",
                col("amount_paid") >= 0
            )

            # Transaction ID
            .withColumn(
                "is_valid_transaction_id",
                col("transaction_id").isNotNull()
            )

            # Unique Transaction ID
            .withColumn(
                "txn_count",
                count("*").over(txn_window)
            )
            .withColumn(
                "is_unique_transaction_id",
                col("txn_count") == 1
            )

            # Payment Method
            .withColumn(
                "is_valid_payment_method",
                col("payment_method").isin(valid_methods)
            )

            # Payment Status
            .withColumn(
                "is_valid_payment_status",
                col("payment_status").isin(valid_status)
            )

            # Payment Date
            .withColumn(
                "is_valid_payment_date",
                col("payment_date") <= current_timestamp()
            )

            .drop("txn_count")
        )

        logger.info("Payment Validation Completed.")    

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to payment data.
        """

        logger.info("Applying Payment Business Rules...")

        df = (
            df

            # Successful Payment
            .withColumn(
                "is_successful_payment",
                col("payment_status") == "SUCCESS"
            )

            # Failed Payment
            .withColumn(
                "is_failed_payment",
                col("payment_status") == "FAILED"
            )

            # Pending Payment
            .withColumn(
                "is_pending_payment",
                col("payment_status") == "PENDING"
            )

            # Refunded Payment
            .withColumn(
                "is_refunded_payment",
                col("payment_status") == "REFUNDED"
            )

            # Payment Category
            .withColumn(
                "payment_amount_category",
                when(col("amount_paid") < 1000, "LOW")
                .when(col("amount_paid") < 5000, "MEDIUM")
                .when(col("amount_paid") < 10000, "HIGH")
                .otherwise("PREMIUM")
            )

            # Digital Payment
            .withColumn(
                "is_digital_payment",
                col("payment_method").isin(
                    "UPI",
                    "CARD",
                    "NET BANKING",
                    "WALLET"
                )
            )
        )

        logger.info("Payment Business Rules Applied Successfully.")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete payment transformation pipeline.
        """

        logger.info("Starting Payment Transformation...")

        initial_count = df.count()

        # Step 1: Clean Data
        df = self.clean_data(df)
        cleaned_count = df.count()

        # Step 2: Validate Data
        df = self.validate_data(df)

        # Step 3: Apply Business Rules
        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Payment Transformation Summary")
        logger.info("=" * 60)
        logger.info(f"Initial Records              : {initial_count}")
        logger.info(f"After Cleaning               : {cleaned_count}")
        logger.info(f"Final Records                : {final_count}")
        logger.info(f"Duplicates Removed           : {initial_count - cleaned_count}")

        logger.info(f"Invalid Order IDs            : {df.filter(~col('is_valid_order_id')).count()}")
        logger.info(f"Invalid Amount Paid          : {df.filter(~col('is_valid_amount_paid')).count()}")
        logger.info(f"Missing Transaction IDs      : {df.filter(~col('is_valid_transaction_id')).count()}")
        logger.info(f"Duplicate Transaction IDs    : {df.filter(~col('is_unique_transaction_id')).count()}")
        logger.info(f"Invalid Payment Methods      : {df.filter(~col('is_valid_payment_method')).count()}")
        logger.info(f"Invalid Payment Status       : {df.filter(~col('is_valid_payment_status')).count()}")
        logger.info(f"Invalid Payment Dates        : {df.filter(~col('is_valid_payment_date')).count()}")

        logger.info("Payment Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df





        