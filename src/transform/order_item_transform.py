from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.utils.logger import get_logger

logger = get_logger(__name__)

class OrderItemTransform:
    """
    Handles all Order Item data transformations.
    """

    def clean_data(self,df:DataFrame)->DataFrame:
        """
        Cleans order item data.
        """
        logger.info('Cleaning OrderItem Data')

        # Remove Duplicates
        df= df.drop_duplicates(['order_item_id'])

        #remove Null Order Item Ids
        df=df.filter(df['order_item_id'].isNotNull())

        logger.info("Order Item Cleaning Completed...")

        return df

    def validate_data(self,df:DataFrame)->DataFrame:
        """
        Validates order item data.
        """
        logger.info("Validating order item data...")

        df= (
            df

            # Order ID
            .withColumn(
                'is_valid_order_id',
                (col('order_id').isNotNull()) &
                (col('order_id')>0)
            )

            #Product Id
            .withColumn(
                'is_valid_product_id',
                (col('product_id').isNotNull()) &
                (col('product_id')>0)
            )

            #Quantity
            .withColumn(
                'is_valid_Quantity',
                col('quantity')>0
            )

            #Unit Price Validation
            .withColumn(
                'is_valid_unit_price',
                (col('unit_price')>0)
            )

            #Discount Validation
            .withColumn(
                'is_valid_discount',
                (col('discount')>=0) &
                (col('discount')<=(col('quantity')*col('unit_price')))
            )

            #Line Total validation
            .withColumn(
                'is_valid_line_total',
                round(col("line_total"), 2) ==
                round(
                    (col("quantity") * col("unit_price")) -
                    col("discount"),
                    2
                )
            )
        )

        logger.info('Order Item Validation completed')
        return df

    def apply_business_rules(self,df:DataFrame)->DataFrame:
        """
        Applies business rules to order item data.
        """

        logger.info("Applying business rules...")
        df=(
            df

            # Gross Amount
            .withColumn(
                'gross_amount',
                col('quantity')*col('unit_price')
            )

            # Discount Percentage
            .withColumn(
                "discount_percentage",
                when(
                    (col("quantity") * col("unit_price")) > 0,
                    round(
                        (
                            col("discount") /
                            (col("quantity") * col("unit_price"))
                        ) * 100,
                        2
                    )
                )
            )

            #Net Amount
            .withColumn(
                'net_amount',
                (col("quantity") * col("unit_price")) -
                col('discount')
            )

            #Quantity Category
            .withColumn(
                'quantity_category',
                when(col('quantity')==1,'Single Item')
                .when(col('quantity')<=5,'Small Order')
                .when(col('quantity')<=10,'Bulk Order')
                .otherwise('Large Bulk Order')
            )

            #Bulk Order Flag
             .withColumn(
                "is_bulk_order",
                col("quantity") >= 5
            )

             # Line Value Category
            .withColumn(
                "line_value_category",
                when(col("line_total") < 1000, "Low Value")
                .when(col("line_total") < 5000, "Medium Value")
                .otherwise("High Value")
            )

            # Discount Applied
            .withColumn(
                "is_discount_applied",
                col("discount") > 0
            )
                
        )
        logger.info("Order Item Business Rules Applied Successfully.")

        return df
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Executes the complete order item transformation pipeline.
        """

        logger.info("Starting Order Item Transformation...")

        initial_count = df.count()

        df = self.clean_data(df)
        cleaned_count = df.count()

        df = self.validate_data(df)

        df = self.apply_business_rules(df)

        final_count = df.count()

        logger.info("=" * 50)
        logger.info("Order Item Transformation Summary")
        logger.info("=" * 50)
        logger.info(f"Initial Records       : {initial_count}")
        logger.info(f"After Cleaning        : {cleaned_count}")
        logger.info(f"Final Records         : {final_count}")
        logger.info(f"Duplicates Removed    : {initial_count - cleaned_count}")

        logger.info(f"Invalid Order IDs     : {df.filter(~col('is_valid_order_id')).count()}")
        logger.info(f"Invalid Product IDs   : {df.filter(~col('is_valid_product_id')).count()}")
        logger.info(f"Invalid Quantity      : {df.filter(~col('is_valid_quantity')).count()}")
        logger.info(f"Invalid Unit Price    : {df.filter(~col('is_valid_unit_price')).count()}")
        logger.info(f"Invalid Discounts     : {df.filter(~col('is_valid_discount')).count()}")
        logger.info(f"Invalid Line Totals   : {df.filter(~col('is_valid_line_total')).count()}")

        logger.info("Order Item Transformation Completed Successfully.")
        logger.info("=" * 50)

        return df
        