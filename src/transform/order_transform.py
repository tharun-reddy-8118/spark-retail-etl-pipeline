from pyspark.sql import DataFrame
from src.utils.logger import get_logger
from pyspark.sql.functions import *
logger = get_logger(__name__)

class OrderTransform:
    """
    Handles all Order data transformations.
    """
    def clean_data(self,df:DataFrame)->DataFrame:
        """
        Cleans order data.
        """

        logger.info("Cleaning Order Data...")

        # Remove duplicate orders
        df = df.dropDuplicates(['order_id'])
        

        # Remove records with null order_id
        df = df.filter(col('order_id').isNotNull())

        # Trim string columns
        string_columns = [
            "order_status"
        ]
        for column in string_columns:
            df = df.withColumn(column, trim(col(column)))
        
        # standardize status
        df=df.withColumn(
            'order_status',
            upper(col('order_status'))
        )

        logger.info('Order Data Cleaned Successfully')

        return df

    def validate_data(self,df:DataFrame)->DataFrame:
        """
        Validates order data and adds data quality flags.
        """

        logger.info("Validating Order Data...")

        df=(
            df

            # Customer ID Validation
            .withColumn(
                'is_valid_customer_id',
                (col('customer_id').isNotNull()) &
                (col('customer_id')>0)
            )

            # Store ID Validation
            .withColumn(
                'is_valid_store_id',
                (col('store_id').isNotNull()) &
                (col('store_id')>0)
            )

            #Employee ID Validation
            .withColumn(
                'is_valid_employee_id',
                (col('employee_id').isNotNull()) &
                (col('employee_id')>0)
            )

            # Promotion ID Validation (Optional Promotion)
            .withColumn(
                'is_valid_promotion_id',
                (col('promotion_id').isNull()) |
                (col("promotion_id") > 0)
            )

            # Order Date Validation
            .withColumn(
                'is_valid_order_date',
                col('order_date')<= current_date()
            )

            # Subtotal Validation
            .withColumn(
                'isvalid_subtotal',
                col('subtotal')>0
            )

            # Discount Validation
            .withColumn(
                'is_valid_discount',
                (col('discount')>=0) &
                (col('discount')<=col('subtotal'))
            )

            #Tax Validation
            .withColumn(
                'is_valid_tax',
                (col('tax')>=0) &
                (col('tax')<=col('subtotal'))
            )

            # Total Validation
            .withColumn(
                'is_valid_total_amount',
                round(col("total_amount"), 2) ==
                round(
                    col("subtotal") -
                    col("discount") +
                    col("tax"),
                    2
                )
            )

            #Order Status Validation
            .withColumn(
                'is_valid_order_status',
                col("order_status").isin(
                    "PENDING",
                    "COMPLETED",
                    "CANCELLED"
                )
            )
        )
        logger.info("Order Data Validation completed.")

        return df
    
    def apply_business_rules(self,df:DataFrame)->DataFrame:
        """
        Applies business rules to order data.
        """
        logger.info("Applying business Rules to Order Data...")

        df = (
            df

            # Order Age (Days)
            .withColumn(
                'order_age_days',
                when(
                    col('is_valid_order_date'),
                    datediff(current_date(),col('order_date'))
                )
            )

            # Order Month
            .withColumn(
                'order_month',
                date_format(col('order_date'),'MMMM')
            )

            # order Year
            .withColumn(
                'order_year',
                year(col('order_date'))
            )

            # Order Quarter
            .withColumn(
                'order_quarter',
                concat(lit('Q'),quarter(col('order_date')))
            )

            # Discount Percentage
            .withColumn(
                'discount_percentage',
                when(
                    col('subtotal')>0,
                    round((col("discount") / col("subtotal")) * 100,2)
                )
            )

            #Net Revenue
            .withColumn(
                'net_revenue',
                col('subtotal')-col('discount')+col('tax')
            )

            # Order Value Category
            .withColumn(
                'order_value_category',
                when(col('total_amount')<1000,'Low Value')
                .when(col('total_amount')<5000,'Medium Value')
                .otherwise('High Value')
            )

            #order status flags
            .withColumn(
                'is_completed_order',
                col('order_status')=='COMPLETED'
            )

            .withColumn(
                'is_pending_order',
                col('order_status')=='PENDING'
            )

            .withColumn(
                'is_cancelled_order',
                col('order_status')=='CANCELLED'
            )

            # Discount Applied
            .withColumn(
                'is_discount_applied',
                col('discount')>0
            )
        )
        logger.info('Business Rules applied successfully')
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete order transformation pipeline.
        """

        logger.info("Starting Order Transformation...")

        initial_count = df.count()

        # Clean Data
        df = self.clean_data(df)
        cleaned_count = df.count()

        # Validate Data
        df = self.validate_data(df)

        # Apply Business Rules
        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 50)
        logger.info("Order Transformation Summary")
        logger.info("=" * 50)
        logger.info(f"Initial Records        : {initial_count}")
        logger.info(f"After Cleaning         : {cleaned_count}")
        logger.info(f"Final Records          : {final_count}")
        logger.info(f"Duplicates Removed     : {initial_count - cleaned_count}")

        # Validation Summary
        logger.info(f"Invalid Customer IDs   : {df.filter(~col('is_valid_customer_id')).count()}")
        logger.info(f"Invalid Store IDs      : {df.filter(~col('is_valid_store_id')).count()}")
        logger.info(f"Invalid Employee IDs   : {df.filter(~col('is_valid_employee_id')).count()}")
        logger.info(f"Invalid Order Dates    : {df.filter(~col('is_valid_order_date')).count()}")
        logger.info(f"Invalid Discounts      : {df.filter(~col('is_valid_discount')).count()}")
        logger.info(f"Invalid Total Amounts  : {df.filter(~col('is_valid_total_amount')).count()}")

        logger.info("Order Transformation Completed Successfully")
        logger.info("=" * 50)

        return df
        
