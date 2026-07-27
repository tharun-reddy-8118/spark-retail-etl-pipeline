from src.utils.spark_session import SparkSessionBuilder
from src.utils.logger import get_logger

from src.load.postgres_loader import PostgresLoader

from src.extract.extract import Extract
from src.load.load import Load
from src.warehouse.star_schema import StarSchemaBuilder
from src.transform.customer_transform import CustomerTransform
from src.transform.product_transform import ProductTransform
from src.transform.order_transform import OrderTransform
from src.transform.order_item_transform import OrderItemTransform
from src.transform.payment_transform import PaymentTransform
from src.transform.shipment_transform import ShipmentTransform
from src.transform.inventory_transform import InventoryTransform
from src.transform.store_transform import StoreTransform
from src.transform.supplier_transform import SupplierTransform
from src.transform.promotion_transform import PromotionTransform
from src.transform.return_transform import ReturnTransform
from src.transform.employee_transform import EmployeeTransform

logger = get_logger(__name__)

def main():

    logger.info("=" * 80)
    logger.info("Retail ETL Pipeline Started")
    logger.info("=" * 80)

    spark = SparkSessionBuilder.get_spark_session()
    try:
        logger.info("Starting Extraction Layer...")

        extractor = Extract(spark)

        raw_data = extractor.extract_all()
        transformed = {}
        transformed["customers"] = CustomerTransform().transform(
            raw_data["customers"]
        )
        transformed["products"] = ProductTransform().transform(
            raw_data["products"]
        )
        transformed["stores"] = StoreTransform().transform(
            raw_data["stores"]
        )
        transformed["suppliers"] = SupplierTransform().transform(
            raw_data["suppliers"]
        )
        transformed["employees"] = EmployeeTransform().transform(
            raw_data["employees"]
        )
        transformed["promotions"] = PromotionTransform().transform(
            raw_data["promotions"]
        )
        transformed["orders"] = OrderTransform().transform(
            raw_data["orders"]
        )
        transformed["order_items"] = OrderItemTransform().transform(
            raw_data["order_items"]
        )
        transformed["payments"] = PaymentTransform().transform(
            raw_data["payments"]
        )
        transformed["shipments"] = ShipmentTransform().transform(
            raw_data["shipments"]
        )
        transformed["inventory"] = InventoryTransform().transform(
            raw_data["inventory"]
        )
        transformed["returns"] = ReturnTransform().transform(
            raw_data["returns"]
        )

        warehouse = StarSchemaBuilder(spark)

        logger.info("=" * 60)
        logger.info("BUILDING DIMENSIONS")
        logger.info("=" * 60)

        dim_date = warehouse.build_dim_date()

        dim_customer = warehouse.build_dim_customer(
            transformed["customers"]
        )

        dim_product = warehouse.build_dim_product(
            transformed["products"]
        )

        dim_store = warehouse.build_dim_store(
            transformed["stores"]
        )

        dim_supplier = warehouse.build_dim_supplier(
            transformed["suppliers"]
        )

        dim_employee = warehouse.build_dim_employee(
            transformed["employees"]
        )

        dim_promotion = warehouse.build_dim_promotion(
            transformed["promotions"]
        )

        logger.info("=" * 60)
        logger.info("BUILDING FACT TABLES")
        logger.info("=" * 60)

        fact_orders = warehouse.build_fact_orders(
            transformed["orders"],
            dim_customer,
            dim_store,
            dim_employee,
            dim_promotion,
            dim_date
        )

        fact_order_items = warehouse.build_fact_order_items(
            transformed["order_items"],
            fact_orders,
            dim_product
        )

        fact_payments = warehouse.build_fact_payments(
            transformed["payments"],
            fact_orders,
            dim_date
        )

        fact_shipments = warehouse.build_fact_shipments(
            transformed["shipments"],
            fact_orders,
            dim_store,
            dim_date
        )

        fact_returns = warehouse.build_fact_returns(
            transformed["returns"],
            fact_orders,
            dim_product,
            dim_date
        )

        fact_inventory = warehouse.build_fact_inventory(
            transformed["inventory"],
            dim_product,
            dim_store,
            dim_supplier,
            dim_date
        )


        logger.info("Starting Load Layer...")

        loader = Load()

        loader.load_all(transformed)




        logger.info("=" * 60)
        logger.info("LOADING DATA WAREHOUSE")
        logger.info("=" * 60)

        dimensions = {
            "dim_date": dim_date,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_store": dim_store,
            "dim_supplier": dim_supplier,
            "dim_employee": dim_employee,
            "dim_promotion": dim_promotion,
        }

        facts = {
            "fact_orders": fact_orders,
            "fact_order_items": fact_order_items,
            "fact_payments": fact_payments,
            "fact_shipments": fact_shipments,
            "fact_returns": fact_returns,
            "fact_inventory": fact_inventory,
        }

        loader.load_warehouse(dimensions, facts)

        postgres_loader = PostgresLoader()
        postgres_loader.load_warehouse(dimensions, facts)

        logger.info("=" * 80)
        logger.info("Retail ETL Pipeline Completed Successfully")
        logger.info("=" * 80)

    except Exception as e:

        logger.exception(
            f"Pipeline Failed : {str(e)}"
        )

        raise
    finally:

        logger.info("Stopping Spark Session.")

        spark.stop()

if __name__ == "__main__":
    main()