from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger=get_logger(__name__)

class SupplierTransform:
    """
    Handles all Supplier data transformations.
    """

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans supplier data.
        """

        logger.info("Cleaning Supplier Data...")

        df = df.dropDuplicates(["supplier_id"])

        df = df.filter(col("supplier_id").isNotNull())

        df = (
            df
            .withColumn("supplier_name", trim(col("supplier_name")))
            .withColumn("contact_person", initcap(trim(col("contact_person"))))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("address", trim(col("address")))
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("state", initcap(trim(col("state"))))
            .withColumn("country", initcap(trim(col("country"))))
            .withColumn("gst_number", upper(trim(col("gst_number"))))
            .withColumn("business_category", initcap(trim(col("business_category"))))
            .withColumn("payment_terms", trim(col("payment_terms")))
            .withColumn("status", upper(trim(col("status"))))
        )

        logger.info("Supplier Cleaning Completed.")

        return df
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates supplier data.
        """

        logger.info("Validating Supplier Data...")

        gst_window = Window.partitionBy("gst_number")

        valid_status = [
            "ACTIVE",
            "INACTIVE"
        ]

        df = (
            df

            .withColumn(
                "is_valid_supplier_name",
                col("supplier_name").isNotNull()
            )

            .withColumn(
                "is_valid_contact_person",
                col("contact_person").isNotNull()
            )

            .withColumn(
                "is_valid_email",
                col("email").rlike(
                    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
            )

            .withColumn(
                "is_valid_phone",
                regexp_extract(
                    col("phone"),
                    r'^\d{10}$',
                    0
                ) != ""
            )

            .withColumn(
                "gst_count",
                count("*").over(gst_window)
            )

            .withColumn(
                "is_unique_gst",
                col("gst_count") == 1
            )

            .withColumn(
                "is_valid_gst",
                col("gst_number").rlike(
                    r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$'
                )
            )

            .withColumn(
                "is_valid_rating",
                col("rating").between(1,5)
            )

            .withColumn(
                "is_valid_lead_time",
                col("lead_time_days") > 0
            )

            .withColumn(
                "is_valid_contract_date",
                col("contract_start_date") <= current_date()
            )

            .withColumn(
                "is_valid_status",
                col("status").isin(valid_status)
            )

            .drop("gst_count")
        )

        logger.info("Supplier Validation Completed.")

        return df

    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to supplier data.
        """

        logger.info("Applying Supplier Business Rules...")

        df = (
            df

            .withColumn(
                "contract_age_years",
                datediff(
                    current_date(),
                    col("contract_start_date")
                ) / 365
            )

            .withColumn(
                "supplier_rating_category",
                when(col("rating") >= 5, "EXCELLENT")
                .when(col("rating") >= 4, "GOOD")
                .when(col("rating") >= 3, "AVERAGE")
                .otherwise("POOR")
            )

            .withColumn(
                "lead_time_category",
                when(col("lead_time_days") <= 7, "FAST")
                .when(col("lead_time_days") <= 15, "NORMAL")
                .otherwise("SLOW")
            )

            .withColumn(
                "is_active_supplier",
                col("status") == "ACTIVE"
            )

            .withColumn(
                "payment_term_category",
                when(col("payment_terms").contains("30"), "SHORT TERM")
                .when(col("payment_terms").contains("60"), "MEDIUM TERM")
                .otherwise("LONG TERM")
            )
        )

        logger.info("Supplier Business Rules Applied Successfully.")

        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete supplier transformation pipeline.
        """

        logger.info("Starting Supplier Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Supplier Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records              : {initial_count}")
        logger.info(f"After Cleaning               : {cleaned_count}")
        logger.info(f"Final Records                : {final_count}")
        logger.info(f"Duplicates Removed           : {initial_count-cleaned_count}")

        logger.info(f"Invalid Supplier Name        : {df.filter(~col('is_valid_supplier_name')).count()}")
        logger.info(f"Invalid Contact Person       : {df.filter(~col('is_valid_contact_person')).count()}")
        logger.info(f"Invalid Email               : {df.filter(~col('is_valid_email')).count()}")
        logger.info(f"Invalid Phone               : {df.filter(~col('is_valid_phone')).count()}")
        logger.info(f"Duplicate GST Numbers       : {df.filter(~col('is_unique_gst')).count()}")
        logger.info(f"Invalid GST Numbers         : {df.filter(~col('is_valid_gst')).count()}")
        logger.info(f"Invalid Ratings            : {df.filter(~col('is_valid_rating')).count()}")
        logger.info(f"Invalid Lead Time          : {df.filter(~col('is_valid_lead_time')).count()}")
        logger.info(f"Invalid Contract Dates     : {df.filter(~col('is_valid_contract_date')).count()}")
        logger.info(f"Invalid Status             : {df.filter(~col('is_valid_status')).count()}")

        logger.info("Supplier Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df

        

        