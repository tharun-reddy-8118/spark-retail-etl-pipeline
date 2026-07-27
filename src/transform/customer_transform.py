from pyspark.sql import DataFrame
from src.utils.logger import get_logger
from pyspark.sql.functions import *
logger = get_logger(__name__)

class CustomerTransform:
    """
    Handles all customer data transformations.
    """
    def clean_data(self,df:DataFrame)->DataFrame:
        """
        Cleans customer data by:
        1. Removing duplicate customer IDs
        2. Removing records with null customer IDs
        3. Trimming whitespace
        4. Standardizing text columns
        """
        logger.info("Cleaning customer data...")

        # Remove duplicate customers
        df=df.drop_duplicates(["customer_id"])

        # Remove records with null customer_id
        df=df.filter(df["customer_id"].isNotNull())

        # String columns to trim
        string_columns = [
            "first_name",
            "last_name",
            "gender",
            "email",
            "phone",
            "address",
            "city",
            "state",
            "country",
            "segment",
            "preferred_payment",
            "occupation",
            "marital_status",
            "status",
        ]
        for column in string_columns:
            df=df.withColumn(column,trim(col(column)))
        
        # Standardize text
        df=(
            df.withColumn("first_name",initcap(col("first_name")))\
                .withColumn('last_name',initcap(col('last_name')))\
                .withColumn('city',initcap(col('city')))\
                .withColumn("state", upper(col("state")))\
                .withColumn('country',upper(col('country')))\
                .withColumn('email',lower(col('email')))
        )
        logger.info("Customer data Cleaned")
        
        return df
    
    def validate_data(self,df:DataFrame)->DataFrame:
        """
        Performs customer data validation by adding
        data quality flag columns.
        """
        logger.info("Validating Customer Data...")

        df=(
            df
            .withColumn(
                'is_valid_email',
                col('email').rlike(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                )
            )

            .withColumn(
                'is_valid_phone',
                col('phone').rlike(r"^\+?\d{10,15}$")
            )

            .withColumn(
                'is_valid_age',
                col('age').between(18,100)
            )

            .withColumn(
                'is_valid_income',
                col('annual_income')>=0
            )

            .withColumn(
                "is_valid_pincode",
                col("pincode").cast("string").rlike(r"^[0-9]{6}$")
            )

            .withColumn(
                "is_valid_registration_date",
                col("registration_date") <= current_date()
            )
            

        )
        logger.info("Customer data validation completed.")
    
        return df
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Applies business rules and enriches the customer data.
        """
        logger.info("Applying business rules to customer data...")
        
        df=(
            df

            .withColumn(
                'full_name',
                concat_ws(" ",col("first_name"),col("last_name"))
            )

            #Customer Tenure (Years)
            .withColumn(
                'customer_tenure_years',
                when(
                    col('is_valid_registration_date')==True,
                    floor(
                        date_diff(
                            current_date(),col('registration_date')
                        )/365
                    )
                ).otherwise(None)
            )
            # Age Group
            .withColumn(
                'age_group',
                when(col('age')<=25,"Young Adult")
                .when(col('age')<40,"Adult")
                .when(col('age')<60,"Middle Age")
                .otherwise("Senior Citizen")
            )

            #Income Group
            .withColumn(
                'income_group',
                when(col('annual_income')< 300000,'Low Income')
                .when(col('annual_income')<700000,'Middle Income')
                .otherwise('High Income')
            )

            #Loyalty Tier
            .withColumn(
                'loyalty_tier',
                when(col('loyalty_points')<1000,'Bronze')
                .when(col('loyalty_points')<5000,'Silver')
                .when(col('loyalty_points')<10000,'Gold')
                .otherwise('Platinum')
            )

            # Verified Customer
            .withColumn(
                "is_verified_customer",
                col("email_verified") & col("phone_verified")
            )

            .withColumn(
                "is_active",
                when(col("status") == "Active", True)
                .otherwise(False)
            )
        )
        logger.info("Business rules applied successfully.")

        return df
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete customer transformation pipeline.
        """
        logger.info("Starting Customer Transformation...")


        initial_count = df.count()

        #clean the data
        df=self.clean_data(df)

        cleaned_count = df.count()

        #Validate the data
        df=self.validate_data(df)
        
        # Apply Business Rules
        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 50)
        logger.info("Customer Transformation Summary")
        logger.info("=" * 50)
        logger.info(f"Initial Records           : {initial_count}")
        logger.info(f"After Cleaning            : {cleaned_count}")
        logger.info(f"Final Records             : {final_count}")
        logger.info(f"Duplicates Removed        : {initial_count - cleaned_count}")
        logger.info("Customer Transformation Completed Successfully")
        logger.info("=" * 50)

        return df