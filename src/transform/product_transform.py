from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from src.utils.logger import get_logger
logger = get_logger(__name__)
from pyspark.sql.window import Window

class ProductTransform:
    """
    Handles all product data transformations.
    """
    def clean_data(self,df:DataFrame)->DataFrame:
        """
        Cleans product data.
        """
        logger.info("Cleaning product data...")

        #remove duplicates
        df=df.drop_duplicates(['product_id'])

        # Remove null product IDs
        df= df.filter(df['product_id'].isNotNull())

        # Trim string columns
        string_columns = [
            "product_name",
            "sku",
            "barcode",
            "category",
            "sub_category",
            "brand",
            "stock_unit",
            "status"
        ]
        for column in string_columns:
            df=df.withColumn(column,trim(col(column)))
            
        # Standardize text columns

        df=(
            df
            .withColumn('product_name',initcap(col('product_name')))
            .withColumn('category',initcap(col('category')))
            .withColumn('sub_category',initcap(col('sub_category')))
            .withColumn('brand',initcap(col('brand')))
            .withColumn('stock_unit',upper(col('stock_unit')))
            .withColumn('status',upper(col('status')))
        )

        logger.info('Product cleaned Successfully')

        return df
        
    def validate_data(self,df:DataFrame)->DataFrame:
        """
        Validates product data and adds data quality flags.
        """

        logger.info("Validating product data...")
        sku_window=Window.partitionBy('sku')
        barcode_window=Window.partitionBy('barcode')

        df=(
            df
            
            .withColumn(
                'is_valid_product_name',
                col('product_name').isNotNull()
            )

            .withColumn(
                'is_unique_sku',
                count('*').over(sku_window)==1
            )

            .withColumn(
                'is_unique_barcode',
                count('*').over(barcode_window)==1
            )

            .withColumn(
                'is_valid_supplier_id',
                col("supplier_id").rlike("^SUP\\d{5}$")
            )

            .withColumn(
                'is_valid_cost_price',
                col('cost_price')>0
            )

            .withColumn(
                'is_valid_selling_price',
                col('selling_price')>= col("cost_price")
            )

            .withColumn(
                'is_valid_gst',
                col('gst_percentage').between(0,100)
            )

            .withColumn(
                'is_valid_reorder_level',
                col('reorder_level')>= 0
            )

            .withColumn(
                'is_valid_manufacture_date',
                col('manufacture_date')<= current_date()
            )

            .withColumn(
                'is_valid_expiry_date',
                col('expiry_date')>col('manufacture_date')
            )

            .withColumn(
                'is_valid_product_status',
                ~(
                    (col('status')=="ACTIVE") &
                    (col("expiry_date") < current_date())
                )
            )

        )
        logger.info("Product validation Completed")

        return df

    def apply_business_rules(self,df:DataFrame)->DataFrame:
        """
        Applies business rules to product data.
        """
        logger.info("Applying Product Business Rules...")

        df= (
            df

            #Calculated Profit Margin (%)
            .withColumn(
                "calculated_profit_margin",
                when(
                    col('cost_price')>0,
                    round(
                        ((col('selling_price')-col('cost_price'))
                        /col('selling_price')
                        )*100,
                        2
                    )
                    
                )
            )

            #Price Category
            .withColumn(
                "price_category",
                when(col('selling_price')<500,'Budget')
                .when(col('selling_price')<2000,'Standard')
                .otherwise("Premium")
            )

            # GST Category
            .withColumn(
                'gst_category',
                when(col("gst_percentage") == 0, "GST Exempt")
                .when(col('gst_percentage')<=5,'Low GST')
                .when(col('gst_percentage')<=12,'Standard GST')
                .when(col('gst_percentage')<=18,'High GST')
                .otherwise("Luxury GST")
            )

            #Product Availability
            .withColumn(
                'is_available',
                col('status')=="ACTIVE"
            )

            # Product Expiry
            .withColumn(
                'is_expired',
                col('expiry_date')<current_date()
            )

            # Days Until Expiry
            .withColumn(
                'days_to_expiry',
                date_diff(col('expiry_date'),lit(current_date()))
            )

            # Needs reorder
            .withColumn(
                'needs_reorder',
                col("reorder_level") > 0
            ) 
        )

        logger.info("Product Business Rules Applied Successfully")

        return df
        
    def transform(self,df:DataFrame)->DataFrame:
        """
        Executes the complete product transformation pipeline.
        """

        logger.info("Starting Product Transformation...")

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
        logger.info("Product Transformation Summary")
        logger.info("=" * 50)
        logger.info(f"Initial Records           : {initial_count}")
        logger.info(f"After Cleaning            : {cleaned_count}")
        logger.info(f"Final Records             : {final_count}")
        logger.info(f"Duplicates Removed        : {initial_count - cleaned_count}")
        logger.info("Product Transformation Completed Successfully")
        logger.info("=" * 50)

        return df
        


        

        
