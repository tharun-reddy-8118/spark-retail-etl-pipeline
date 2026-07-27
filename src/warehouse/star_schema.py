from datetime import datetime
from pyspark.sql.functions import to_date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from src.utils.logger import get_logger

logger = get_logger(__name__)

class StarSchemaBuilder:

    def __init__(self, spark: SparkSession):
        self.spark = spark

        logger.info("Star Schema Builder Initialized.")
    def build_dim_date(self,start_date="2023-01-01",end_date="2030-12-31") -> DataFrame:
        """
        Builds Date Dimension.
        """

        logger.info("Creating Date Dimension...")

        df = (
            self.spark.range(1)
            .select(
                explode(
                    sequence(
                        to_date(lit(start_date)),
                        to_date(lit(end_date))
                    )
                ).alias("date")
            )
        )

        df = (
            df

            .withColumn(
                "date_key",
                date_format(
                    col("date"),
                    "yyyyMMdd"
                ).cast("int")
            )

            .withColumn(
                "year",
                year(col("date"))
            )

            .withColumn(
                "quarter",
                quarter(col("date"))
            )

            .withColumn(
                "month",
                month(col("date"))
            )

            .withColumn(
                "month_name",
                date_format(
                    col("date"),
                    "MMMM"
                )
            )

            .withColumn(
                "week",
                weekofyear(col("date"))
            )

            .withColumn(
                "day",
                dayofmonth(col("date"))
            )

            .withColumn(
                "day_name",
                date_format(
                    col("date"),
                    "EEEE"
                )
            )

            .withColumn(
                "day_of_week",
                dayofweek(col("date"))
            )

            .withColumn(
                "is_weekend",
                col("day_of_week").isin(1, 7)
            )

            .withColumn(
                "quarter_name",
                when(col("quarter") == 1, "Q1")
                .when(col("quarter") == 2, "Q2")
                .when(col("quarter") == 3, "Q3")
                .otherwise("Q4")
            )
        )

        logger.info(
            f"Date Dimension Created ({df.count()} rows)"
        )

        return df

    def build_dim_customer(self,customers_df: DataFrame) -> DataFrame:
        """
        Builds Customer Dimension.
        """

        logger.info("Creating Customer Dimension...")

        window = Window.orderBy("customer_id")

        dim_customer = (
            customers_df

            .select(
                col("customer_id"),
                col("first_name"),
                col("last_name"),
                col("full_name"),
                col("gender"),
                col("date_of_birth"),
                col("age_group"),
                col("email"),
                col("phone").alias("phone_number"),
                col("city"),
                col("state"),
                col("country"),
                col("pincode").alias("postal_code"),
                col("registration_date"),
                col("customer_tenure_years"),
                col("annual_income").alias("income"),
                col("income_group"),
                col("loyalty_points"),
                col("loyalty_tier"),
                col("is_active")
            )

            .withColumn(
                "customer_key",
                row_number().over(window)
            )

            .select(
                "customer_key",
                "customer_id",
                "first_name",
                "last_name",
                "full_name",
                "gender",
                "date_of_birth",
                "age_group",
                "email",
                "phone_number",
                "city",
                "state",
                "country",
                "postal_code",
                "registration_date",
                "customer_tenure_years",
                "income",
                "income_group",
                "loyalty_points",
                "loyalty_tier",
                "is_active"
            )
        )

        logger.info(
            f"Customer Dimension Created ({dim_customer.count()} rows)"
        )

        return dim_customer

    def build_dim_product(self,products_df: DataFrame) -> DataFrame:
        """
        Builds Product Dimension.
        """

        logger.info("Creating Product Dimension...")

        window = Window.orderBy("product_id")

        dim_product = (
            products_df

            .select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("sub_category").alias("subcategory"),
                col("brand"),
                col("supplier_id"),
                col("sku"),
                col("barcode"),
                col("stock_unit"),
                col("cost_price"),
                col("selling_price"),
                col("gst_percentage"),
                col("calculated_profit_margin"),
                col("price_category"),
                col("gst_category"),
                col("reorder_level"),
                col("needs_reorder"),
                col("status"),
                col("is_available"),
                col("is_expired")
            )

            .withColumn(
                "product_key",
                row_number().over(window)
            )

            .select(
                "product_key",
                "product_id",
                "product_name",
                "category",
                "subcategory",
                "brand",
                "supplier_id",
                "sku",
                "barcode",
                "stock_unit",
                "cost_price",
                "selling_price",
                "gst_percentage",
                "calculated_profit_margin",
                "price_category",
                "gst_category",
                "reorder_level",
                "needs_reorder",
                "status",
                "is_available",
                "is_expired"
            )
        )

        logger.info(
            f"Product Dimension Created ({dim_product.count()} rows)"
        )

        return dim_product
    
    def build_dim_store(self,stores_df: DataFrame) -> DataFrame:
        """
        Builds Store Dimension.
        """

        logger.info("Creating Store Dimension...")

        window = Window.orderBy("store_id")

        dim_store = (
            stores_df

            .select(
                "store_id",
                "store_name",
                "store_type",
                "status",
                "address",
                "city",
                "state",
                "pincode",
                "phone_number",
                "email",
                "manager_name",
                "opening_date",
                "store_size_sqft",
                "store_age_years",
                "store_size_category",
                "store_type_group",
                "is_active_store"
            )

            .withColumn(
                "store_key",
                row_number().over(window)
            )

            .select(
                "store_key",
                "store_id",
                "store_name",
                "store_type",
                "store_type_group",
                "status",
                "is_active_store",
                "address",
                "city",
                "state",
                "pincode",
                "phone_number",
                "email",
                "manager_name",
                "opening_date",
                "store_age_years",
                "store_size_sqft",
                "store_size_category"
            )
        )

        logger.info(
            f"Store Dimension Created ({dim_store.count()} rows)"
        )

        return dim_store
    
    def build_dim_supplier(self,suppliers_df: DataFrame) -> DataFrame:
        """
        Builds Supplier Dimension.
        """

        logger.info("Creating Supplier Dimension...")

        window = Window.orderBy("supplier_id")

        dim_supplier = (
            suppliers_df

            .select(
                "supplier_id",
                "supplier_name",
                "contact_person",
                "email",
                "phone",
                "address",
                "city",
                "state",
                "country",
                "gst_number",
                "business_category",
                "rating",
                "payment_terms",
                "payment_term_category",
                "lead_time_days",
                "lead_time_category",
                "contract_start_date",
                "contract_age_years",
                "status",
                "is_active_supplier"
            )

            .withColumn(
                "supplier_key",
                row_number().over(window)
            )

            .select(
                "supplier_key",
                "supplier_id",
                "supplier_name",
                "contact_person",
                "email",
                "phone",
                "address",
                "city",
                "state",
                "country",
                "gst_number",
                "business_category",
                "rating",
                "payment_terms",
                "payment_term_category",
                "lead_time_days",
                "lead_time_category",
                "contract_start_date",
                "contract_age_years",
                "status",
                "is_active_supplier"
            )
        )

        logger.info(
            f"Supplier Dimension Created ({dim_supplier.count()} rows)"
        )

        return dim_supplier

    def build_dim_employee(self,employees_df: DataFrame) -> DataFrame:
        """
        Builds Employee Dimension.
        """

        logger.info("Creating Employee Dimension...")

        window = Window.orderBy("employee_id")

        dim_employee = (
            employees_df

            .select(
                "employee_id",
                "first_name",
                "last_name",
                "gender",
                "date_of_birth",
                "employee_age",
                "phone_number",
                "email",
                "department",
                "department_group",
                "designation",
                "salary",
                "salary_band",
                "store_id",
                "hire_date",
                "employee_tenure_years",
                "experience_level",
                "status",
                "is_active_employee",
                "is_management"
            )

            .withColumn(
                "employee_key",
                row_number().over(window)
            )

            .select(
                "employee_key",
                "employee_id",
                "first_name",
                "last_name",
                "gender",
                "date_of_birth",
                "employee_age",
                "phone_number",
                "email",
                "department",
                "department_group",
                "designation",
                "salary",
                "salary_band",
                "store_id",
                "hire_date",
                "employee_tenure_years",
                "experience_level",
                "status",
                "is_active_employee",
                "is_management"
            )
        )

        logger.info(
            f"Employee Dimension Created ({dim_employee.count()} rows)"
        )

        return dim_employee

    def build_dim_promotion(self,promotions_df: DataFrame) -> DataFrame:
        """
        Builds Promotion Dimension.
        """

        logger.info("Creating Promotion Dimension...")

        window = Window.orderBy("promotion_id")

        dim_promotion = (
            promotions_df

            .select(
                "promotion_id",
                "promotion_name",
                "promotion_type",
                "discount_value",
                "discount_category",
                "start_date",
                "end_date",
                "promotion_duration_days",
                "applicable_category",
                "minimum_order_amount",
                "coupon_code",
                "status",
                "promotion_lifecycle",
                "is_percentage_offer",
                "is_flat_offer",
                "is_bogo_offer"
            )

            .withColumn(
                "promotion_key",
                row_number().over(window)
            )

            .select(
                "promotion_key",
                "promotion_id",
                "promotion_name",
                "promotion_type",
                "discount_value",
                "discount_category",
                "start_date",
                "end_date",
                "promotion_duration_days",
                "applicable_category",
                "minimum_order_amount",
                "coupon_code",
                "status",
                "promotion_lifecycle",
                "is_percentage_offer",
                "is_flat_offer",
                "is_bogo_offer"
            )
        )

        logger.info(
            f"Promotion Dimension Created ({dim_promotion.count()} rows)"
        )

        return dim_promotion

    def build_fact_orders(self,orders_df: DataFrame,dim_customer: DataFrame,dim_store: DataFrame,dim_employee: DataFrame,dim_promotion: DataFrame,dim_date: DataFrame)-> DataFrame:
        customer_dim = dim_customer.select(
            "customer_id",
            "customer_key"
        )

        store_dim = dim_store.select(
            "store_id",
            "store_key"
        )

        employee_dim = dim_employee.select(
            "employee_id",
            "employee_key"
        )

        promotion_dim = dim_promotion.select(
            "promotion_id",
            "promotion_key"
        )

        date_dim = dim_date.select(
            "date",
            "date_key"
        )

        fact = (
            orders_df.alias("o")

            .join(
                customer_dim.alias("c"),
                col("o.customer_id") == col("c.customer_id"),
                "left"
            )
        )
        fact = fact.join(
            store_dim.alias("s"),
            col("o.store_id") == col("s.store_id"),
            "left"
        )
        fact = fact.join(
            employee_dim.alias("e"),
            col("o.employee_id") == col("e.employee_id"),
            "left"
        )
        fact = fact.join(
            promotion_dim.alias("p"),
            col("o.promotion_id") == col("p.promotion_id"),
            "left"
        )
        from pyspark.sql.functions import to_date

        fact = fact.join(
            date_dim.alias("d"),
            to_date(col("o.order_date")) == col("d.date"),
            "left"
        )
        fact_orders = (
            fact.select(

                col("o.order_id"),

                col("date_key"),

                col("customer_key"),

                col("store_key"),

                col("employee_key"),

                col("promotion_key"),

                col("o.order_date"),

                col("o.subtotal"),

                col("o.discount"),

                col("o.tax"),

                col("o.total_amount"),

                col("o.discount_percentage"),

                col("o.net_revenue"),

                col("o.order_value_category"),

                col("o.order_status"),

                col("o.is_discount_applied")
            ).withColumn("order_key", row_number().over(Window.orderBy("order_id")))
        )

        fact_orders = fact_orders.select(
            "order_key",
            "order_id",
            "date_key",
            "customer_key",
            "store_key",
            "employee_key",
            "promotion_key",
            "order_date",
            "subtotal",
            "discount",
            "tax",
            "total_amount",
            "discount_percentage",
            "net_revenue",
            "order_value_category",
            "order_status",
            "is_discount_applied"
        )

        logger.info(
            f"Fact Orders Created ({fact_orders.count()} rows)"
        )

        return fact_orders

    def build_fact_order_items(self,order_items_df: DataFrame,fact_orders: DataFrame,dim_product: DataFrame) -> DataFrame:
        product_dim = dim_product.select(
            "product_id",
            "product_key"
        )

        orders_fact = fact_orders.select(
            "order_id",
            "order_key"
        )
        fact = (
            order_items_df.alias("oi")

            .join(
                orders_fact.alias("fo"),
                col("oi.order_id") == col("fo.order_id"),
                "left"
            )
        )
        fact = fact.join(
            product_dim.alias("p"),
            col("oi.product_id") == col("p.product_id"),
            "left"
        )

        fact_order_items = (

            fact.select(

                col("oi.order_item_id"),

                col("order_key"),

                col("product_key"),

                col("oi.quantity"),

                col("oi.unit_price"),

                col("oi.discount"),

                col("oi.line_total"),

                col("oi.gross_amount"),

                col("oi.net_amount"),

                col("oi.discount_percentage"),

                col("oi.quantity_category"),

                col("oi.line_value_category"),

                col("oi.is_bulk_order"),

                col("oi.is_discount_applied")

            )

        )
        window = Window.orderBy("order_item_id")

        fact_order_items = (

            fact_order_items

            .withColumn(

                "order_item_key",

                row_number().over(window)

            )

        )
        fact_order_items = fact_order_items.select(

            "order_item_key",

            "order_item_id",

            "order_key",

            "product_key",

            "quantity",

            "unit_price",

            "discount",

            "line_total",

            "gross_amount",

            "net_amount",

            "discount_percentage",

            "quantity_category",

            "line_value_category",

            "is_bulk_order",

            "is_discount_applied"

        )
        logger.info(
            f"Fact Order Items Created ({fact_order_items.count()} rows)"
        )

        return fact_order_items

    def build_fact_payments(self,payments_df: DataFrame,fact_orders: DataFrame,dim_date: DataFrame) -> DataFrame:
        """
        Builds Payment Fact Table.
        """
        orders_fact = fact_orders.select(
            "order_id",
            "order_key"
        )

        date_dim = dim_date.select(
            "date",
            "date_key"
        )
        fact = (
            payments_df.alias("p")

            .join(
                orders_fact.alias("o"),
                col("p.order_id") == col("o.order_id"),
                "left"
            )
        )


        fact = fact.join(
            date_dim.alias("d"),
            to_date(col("p.payment_date")) == col("d.date"),
            "left"
        )
        fact_payments = (

            fact.select(

                col("p.payment_id"),

                col("order_key"),

                col("date_key"),

                col("p.payment_date"),

                col("p.payment_method"),

                col("p.payment_status"),

                col("p.amount_paid"),

                col("p.transaction_id"),

                col("p.payment_amount_category"),

                col("p.is_successful_payment"),

                col("p.is_failed_payment"),

                col("p.is_pending_payment"),

                col("p.is_refunded_payment"),

                col("p.is_digital_payment")

            )

        )
        
        window = Window.orderBy("payment_id")

        fact_payments = (

            fact_payments

                    .withColumn(
                        "payment_key",
                        row_number().over(window)
                    )

        )
        fact_payments = fact_payments.select(

            "payment_key",

            "payment_id",

            "order_key",

            "date_key",

            "payment_date",

            "payment_method",

            "payment_status",

            "amount_paid",

            "transaction_id",

            "payment_amount_category",

            "is_successful_payment",

            "is_failed_payment",

            "is_pending_payment",

            "is_refunded_payment",

            "is_digital_payment"

        )
        logger.info(
            f"Fact Payments Created ({fact_payments.count()} rows)"
        )
        return fact_payments

    def build_fact_shipments(self,shipments_df: DataFrame,fact_orders: DataFrame,dim_store: DataFrame,dim_date: DataFrame) -> DataFrame:
        """
        Builds Shipment Fact Table.
        """
        orders_fact = fact_orders.select(
            "order_id",
            "order_key"
        )

        store_dim = dim_store.select(
            "store_id",
            "store_key"
        )

        date_dim = dim_date.select(
            "date",
            "date_key"
        )
        fact = (
            shipments_df.alias("s")

            .join(
                orders_fact.alias("o"),
                col("s.order_id") == col("o.order_id"),
                "left"
            )
        )
        fact = fact.join(
            store_dim.alias("st"),
            col("s.store_id") == col("st.store_id"),
            "left"
        )


        fact = fact.join(
            date_dim.alias("sd"),
            to_date(col("s.shipment_date")) == col("sd.date"),
            "left"
        )

        fact = fact.withColumnRenamed(
            "date_key",
            "shipment_date_key"
        )

        delivery_date_dim = (
            dim_date
            .select("date", "date_key")
            .withColumnRenamed("date", "delivery_date")
            .withColumnRenamed("date_key", "delivery_date_key")
        )

        fact = fact.join(
            delivery_date_dim.alias("dd"),
            to_date(col("s.delivery_date")) == col("dd.delivery_date"),
            "left"
        )
        fact_shipments = (

            fact.select(

                col("s.shipment_id"),

                col("order_key"),

                col("store_key"),

                col("shipment_date_key"),

                col("delivery_date_key"),

                col("s.shipment_date"),

                col("s.delivery_date"),

                col("s.shipping_status"),

                col("s.courier_name"),

                col("s.tracking_number"),

                col("s.delivery_days"),

                col("s.delivery_speed"),

                col("s.shipment_age_days"),

                col("s.is_delivered"),

                col("s.is_in_transit"),

                col("s.is_pending"),

                col("s.is_returned"),

                col("s.is_cancelled")

            )

        )
        window = Window.orderBy("shipment_id")

        fact_shipments = (

            fact_shipments

            .withColumn(

                "shipment_key",

                row_number().over(window)

            )

        )
        fact_shipments = fact_shipments.select(

            "shipment_key",

            "shipment_id",

            "order_key",

            "store_key",

            "shipment_date_key",

            "delivery_date_key",

            "shipment_date",

            "delivery_date",

            "shipping_status",

            "courier_name",

            "tracking_number",

            "delivery_days",

            "delivery_speed",

            "shipment_age_days",

            "is_delivered",

            "is_in_transit",

            "is_pending",

            "is_returned",

            "is_cancelled"

        )
        logger.info(
            f"Fact Shipments Created ({fact_shipments.count()} rows)"
        )

        return fact_shipments
    def build_fact_returns(self,returns_df: DataFrame,fact_orders: DataFrame,dim_product: DataFrame,dim_date: DataFrame) -> DataFrame:
        """
        Builds Return Fact Table.
        """
        orders_fact = fact_orders.select(
            "order_id",
            "order_key"
        )

        product_dim = dim_product.select(
            "product_id",
            "product_key"
        )

        date_dim = (
            dim_date
            .select("date", "date_key")
            .withColumnRenamed("date", "return_date")
            .withColumnRenamed("date_key", "return_date_key")
        )
        fact = (
            returns_df.alias("r")
            .join(
                orders_fact.alias("o"),
                col("r.order_id") == col("o.order_id"),
                "left"
            )
        )

        fact = fact.join(
            product_dim.alias("p"),
            col("r.product_id") == col("p.product_id"),
            "left"
        )

        fact = fact.join(
            date_dim.alias("rd"),
            to_date(col("r.return_date")) == col("rd.return_date"),
            "left"
        )

        fact_returns = (

            fact.select(

                col("r.return_id"),

                col("order_key"),

                col("product_key"),

                col("return_date_key"),

                col("r.return_date"),

                col("r.return_reason"),

                col("r.return_status"),

                col("r.refund_amount"),

                col("r.return_age_days"),

                col("r.refund_category"),

                col("r.requires_refund"),

                col("r.is_approved_return").alias("is_approved"),

                col("r.is_completed_return").alias("is_completed"),

                col("r.is_rejected_return").alias("is_rejected")

            )

        )

        window = Window.orderBy("return_id")

        fact_returns = (

            fact_returns

            .withColumn(

                "return_key",

                row_number().over(window)

            )

        )

        fact_returns = fact_returns.select(

            "return_key",

            "return_id",

            "order_key",

            "product_key",

            "return_date_key",

            "return_date",

            "return_reason",

            "return_status",

            "refund_amount",

            "return_age_days",

            "refund_category",

            "requires_refund",

            "is_approved",

            "is_completed",

            "is_rejected"

        )
        logger.info(
            f"Fact Returns Created ({fact_returns.count()} rows)"
        )

        return fact_returns

    def build_fact_inventory(self,inventory_df: DataFrame,dim_product: DataFrame,dim_store: DataFrame,dim_supplier: DataFrame,dim_date: DataFrame) -> DataFrame:
        """
        Builds Inventory Fact Table.
        """

        product_dim = dim_product.select(
            "product_id",
            "product_key",
            "supplier_id"
        )

        store_dim = dim_store.select(
            "store_id",
            "store_key"
        )

        supplier_dim = dim_supplier.select(
            "supplier_id",
            "supplier_key"
        )

        date_dim = (
            dim_date
            .select("date", "date_key")
            .withColumnRenamed("date", "inventory_date")
            .withColumnRenamed("date_key", "inventory_date_key")
        )

        fact = (
            inventory_df.alias("i")
            .join(
                product_dim.alias("p"),
                col("i.product_id") == col("p.product_id"),
                "left"
            )
        )

        fact = fact.join(
            store_dim.alias("s"),
            col("i.store_id") == col("s.store_id"),
            "left"
        )

        fact = fact.join(
            supplier_dim.alias("sp"),
            col("p.supplier_id") == col("sp.supplier_id"),
            "left"
        )
        fact = fact.join(
            date_dim.alias("id"),
            to_date(col("i.last_updated")) == col("id.inventory_date"),
            "left"
        )

        fact_inventory = (

            fact.select(

                col("i.inventory_id"),

                col("product_key"),

                col("store_key"),

                col("supplier_key"),

                col("inventory_date_key"),

                col("i.last_updated").alias("last_stock_update"),

                col("i.quantity_on_hand").alias("current_stock"),

                col("i.reorder_level"),

                col("i.stock_difference"),

                col("i.inventory_age_days"),

                col("i.stock_category"),

                col("i.inventory_health"),

                col("i.needs_restock")

            )

        )

        window = Window.orderBy("inventory_id")

        fact_inventory = (

            fact_inventory

            .withColumn(
                "inventory_key",
                row_number().over(window)
            )

        )

        fact_inventory = fact_inventory.select(

            "inventory_key",

            "inventory_id",

            "product_key",

            "store_key",

            "supplier_key",

            "inventory_date_key",

            "last_stock_update",

            "current_stock",

            "reorder_level",

            "stock_difference",

            "inventory_age_days",

            "stock_category",

            "inventory_health",

            "needs_restock"

        )

        logger.info(
            f"Fact Inventory Created ({fact_inventory.count()} rows)"
        )

        return fact_inventory