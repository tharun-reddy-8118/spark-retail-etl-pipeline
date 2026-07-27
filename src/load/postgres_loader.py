from pyspark.sql import DataFrame

from src.utils.logger import get_logger
from src.utils.postgres_config import (
    JDBC_URL,
    POSTGRES_CONFIG
)

logger = get_logger(__name__)


class PostgresLoader:

    def __init__(self):
        self.url = JDBC_URL

        self.properties = {
            "user": POSTGRES_CONFIG["user"],
            "password": POSTGRES_CONFIG["password"],
            "driver": "org.postgresql.Driver"
        }

        logger.info("PostgreSQL Loader Initialized.")

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite"
    ):
        """
        Write a Spark DataFrame to PostgreSQL.
        """

        logger.info(f"Loading table: {table_name}")

        (
            df.write
            .jdbc(
                url=self.url,
                table=table_name,
                mode=mode,
                properties=self.properties
            )
        )

        logger.info(f"{table_name} loaded successfully.")

    def load_warehouse(
        self,
        dimensions: dict,
        facts: dict
    ):
        """
        Load all dimensions and facts into PostgreSQL.
        """

        logger.info("=" * 60)
        logger.info("Loading Dimensions")

        for table_name, df in dimensions.items():
            self.write_table(df, table_name)

        logger.info("=" * 60)
        logger.info("Loading Facts")

        for table_name, df in facts.items():
            self.write_table(df, table_name)

        logger.info("=" * 60)
        logger.info("Warehouse Loaded Successfully")