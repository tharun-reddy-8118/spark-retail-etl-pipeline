from pyspark.sql.functions import *
from src.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

logger = get_logger(__name__)

class EmployeeTransform:
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans employee data.
        """

        logger.info("Cleaning Employee Data...")

        df = df.dropDuplicates(["employee_id"])

        df = df.filter(col("employee_id").isNotNull())

        df = (
            df
            .withColumn("first_name", initcap(trim(col("first_name"))))
            .withColumn("last_name", initcap(trim(col("last_name"))))
            .withColumn("gender", initcap(trim(col("gender"))))
            .withColumn("department", initcap(trim(col("department"))))
            .withColumn("designation", initcap(trim(col("designation"))))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("phone_number", trim(col("phone_number")))
            .withColumn("status", upper(trim(col("status"))))
        )

        logger.info("Employee Cleaning Completed.")

        return df

    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validates employee data.
        """

        logger.info("Validating Employee Data...")

        valid_gender = [
            "Male",
            "Female",
            "Other"
        ]

        valid_status = [
            "ACTIVE",
            "INACTIVE",
            "RESIGNED",
            "TERMINATED"
        ]

        email_window = Window.partitionBy("email")

        df = (
            df

            .withColumn(
                "email_count",
                count("*").over(email_window)
            )

            .withColumn(
                "is_valid_first_name",
                col("first_name").isNotNull()
            )

            .withColumn(
                "is_valid_phone",
                col("phone_number").rlike(r"^\+91\d{10}$")
            )

            .withColumn(
                "is_valid_email",
                col("email").rlike(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                )
            )

            .withColumn(
                "is_unique_email",
                col("email_count") == 1
            )

            .withColumn(
                "is_valid_salary",
                col("salary") > 0
            )

            .withColumn(
                "is_valid_store_id",
                col("store_id").isNotNull() &
                (col("store_id") > 0)
            )

            .withColumn(
                "is_valid_hire_date",
                col("hire_date") <= current_date()
            )

            .withColumn(
                "employee_age",
                (datediff(current_date(), col("date_of_birth")) / 365).cast("int")
            )

            .withColumn(
                "is_valid_age",
                col("employee_age") >= 18
            )

            .withColumn(
                "is_valid_gender",
                col("gender").isin(valid_gender)
            )

            .withColumn(
                "is_valid_status",
                col("status").isin(valid_status)
            )
        )

        logger.info("Employee Validation Completed.")

        return df

    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules to employee data.
        """

        logger.info("Applying Employee Business Rules...")

        df = (
            df

            .withColumn(
                "employee_tenure_years",
                (datediff(current_date(), col("hire_date")) / 365).cast("int")
            )

            .withColumn(
                "salary_band",
                when(col("salary") < 30000, "ENTRY")
                .when(col("salary") < 60000, "MID")
                .when(col("salary") < 100000, "SENIOR")
                .otherwise("EXECUTIVE")
            )

            .withColumn(
                "experience_level",
                when(col("employee_tenure_years") < 2, "JUNIOR")
                .when(col("employee_tenure_years") < 5, "MID")
                .when(col("employee_tenure_years") < 10, "SENIOR")
                .otherwise("EXPERT")
            )

            .withColumn(
                "is_active_employee",
                col("status") == "ACTIVE"
            )

            .withColumn(
                "is_management",
                upper(col("designation")).contains("MANAGER")
            )

            .withColumn(
                "department_group",
                when(
                    upper(col("department")).isin(
                        "MANAGEMENT",
                        "ADMINISTRATION"
                    ),
                    "ADMIN"
                )
                .when(
                    upper(col("department")).isin(
                        "SALES",
                        "MARKETING"
                    ),
                    "BUSINESS"
                )
                .otherwise("OPERATIONS")
            )
        )

        logger.info("Employee Business Rules Applied Successfully.")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete employee transformation pipeline.
        """

        logger.info("Starting Employee Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 60)
        logger.info("Employee Transformation Summary")
        logger.info("=" * 60)

        logger.info(f"Initial Records              : {initial_count}")
        logger.info(f"After Cleaning               : {cleaned_count}")
        logger.info(f"Final Records                : {final_count}")
        logger.info(f"Duplicates Removed           : {initial_count-cleaned_count}")

        logger.info(f"Invalid First Name           : {df.filter(~col('is_valid_first_name')).count()}")
        logger.info(f"Invalid Phone                : {df.filter(~col('is_valid_phone')).count()}")
        logger.info(f"Invalid Email                : {df.filter(~col('is_valid_email')).count()}")
        logger.info(f"Duplicate Email              : {df.filter(~col('is_unique_email')).count()}")
        logger.info(f"Invalid Salary               : {df.filter(~col('is_valid_salary')).count()}")
        logger.info(f"Invalid Store ID             : {df.filter(~col('is_valid_store_id')).count()}")
        logger.info(f"Invalid Hire Date            : {df.filter(~col('is_valid_hire_date')).count()}")
        logger.info(f"Underage Employees           : {df.filter(~col('is_valid_age')).count()}")
        logger.info(f"Invalid Gender               : {df.filter(~col('is_valid_gender')).count()}")
        logger.info(f"Invalid Status               : {df.filter(~col('is_valid_status')).count()}")

        logger.info("Employee Transformation Completed Successfully.")
        logger.info("=" * 60)

        return df