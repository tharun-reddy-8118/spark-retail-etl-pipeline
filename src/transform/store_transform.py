from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger = get_logger(__name__)

class StoreTransform:
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans store data.
        """

        logger.info("Cleaning Store Data...")

        df = df.dropDuplicates(["store_id"])

        df = df.filter(col("store_id").isNotNull())

        df = (
            df
            .withColumn("store_name", trim(col("store_name")))
            .withColumn("store_type", trim(col("store_type")))
            .withColumn("status", upper(trim(col("status"))))
            .withColumn("address", trim(col("address")))
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("state", initcap(trim(col("state"))))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("manager_name", initcap(trim(col("manager_name"))))
        )

        logger.info("Store Cleaning Completed.")

        return df
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates store data.
        """

        logger.info("Validating Store Data...")

        valid_store_types = [
            "EXPRESS STORE",
            "SUPERMARKET",
            "HYPERMARKET",
            "WHOLESALE",
            "FLAGSHIP STORE"
        ]

        valid_status = [
            "ACTIVE",
            "INACTIVE",
            "UNDER MAINTENANCE"
        ]

        df = (
            df

            .withColumn(
                "is_valid_store_name",
                col("store_name").isNotNull()
            )

            .withColumn(
                "is_valid_store_type",
                upper(col("store_type")).isin(valid_store_types)
            )

            .withColumn(
                "is_valid_status",
                col("status").isin(valid_status)
            )

            .withColumn(
                "is_valid_phone",
                regexp_extract(
                    col("phone_number"),
                    r'^\+91\d{10}$',
                    0
                ) != ""
            )

            .withColumn(
                "is_valid_email",
                col("email").rlike(
                    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                )
            )

            .withColumn(
                "is_valid_pincode",
                length(col("pincode").cast("string")) == 6
            )

            .withColumn(
                "is_valid_opening_date",
                col("opening_date") <= current_date()
            )

            .withColumn(
                "is_valid_store_size",
                col("store_size_sqft") > 0
            )
        )

        logger.info("Store Validation Completed.")

        return df
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to store data.
        """

        logger.info("Applying Store Business Rules...")

        df = (
            df

            .withColumn(
                "store_age_years",
                datediff(
                    current_date(),
                    col("opening_date")
                ) / 365
            )

            .withColumn(
                "store_size_category",
                when(col("store_size_sqft") < 10000, "SMALL")
                .when(col("store_size_sqft") < 30000, "MEDIUM")
                .when(col("store_size_sqft") < 60000, "LARGE")
                .otherwise("MEGA")
            )

            .withColumn(
                "is_active_store",
                col("status") == "ACTIVE"
            )

            .withColumn(
                "store_type_group",
                when(
                    upper(col("store_type")).contains("EXPRESS"),
                    "SMALL FORMAT"
                )
                .when(
                    upper(col("store_type")).contains("SUPER"),
                    "RETAIL"
                )
                .when(
                    upper(col("store_type")).contains("HYPER"),
                    "LARGE FORMAT"
                )
                .otherwise("OTHER")
            )
        )

        logger.info("Store Business Rules Applied Successfully.")

        return df
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete store transformation pipeline.
        """

        logger.info("Starting Store Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Store Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records            : {initial_count}")
        logger.info(f"After Cleaning             : {cleaned_count}")
        logger.info(f"Final Records              : {final_count}")
        logger.info(f"Duplicates Removed         : {initial_count-cleaned_count}")

        logger.info(f"Invalid Store Name         : {df.filter(~col('is_valid_store_name')).count()}")
        logger.info(f"Invalid Store Type         : {df.filter(~col('is_valid_store_type')).count()}")
        logger.info(f"Invalid Status             : {df.filter(~col('is_valid_status')).count()}")
        logger.info(f"Invalid Phone              : {df.filter(~col('is_valid_phone')).count()}")
        logger.info(f"Invalid Email              : {df.filter(~col('is_valid_email')).count()}")
        logger.info(f"Invalid Pincode            : {df.filter(~col('is_valid_pincode')).count()}")
        logger.info(f"Invalid Opening Date       : {df.filter(~col('is_valid_opening_date')).count()}")
        logger.info(f"Invalid Store Size         : {df.filter(~col('is_valid_store_size')).count()}")

        logger.info("Store Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df
                