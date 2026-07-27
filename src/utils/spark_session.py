import os
from pathlib import Path
from pyspark.sql import SparkSession
from src.utils.postgres_config import JDBC_DRIVER
# Ensure Windows Hadoop utilities (winutils.exe / hadoop.dll) are accessible
project_root = Path(__file__).resolve().parent.parent.parent
hadoop_home = project_root / "hadoop"
if hadoop_home.exists():
    hadoop_dir_str = str(hadoop_home)
    os.environ["HADOOP_HOME"] = hadoop_dir_str
    os.environ["hadoop.home.dir"] = hadoop_dir_str
    hadoop_bin = str(hadoop_home / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

class SparkSessionBuilder:
    """
    Creates and returns a configured SparkSession.
    """
    @staticmethod
    def get_spark_session(app_name: str = "Retail ETL Project") -> SparkSession:
        if "HADOOP_HOME" in os.environ:
            import py4j
            os.environ["_JAVA_OPTIONS"] = f"-Dhadoop.home.dir={os.environ['HADOOP_HOME']} {os.environ.get('_JAVA_OPTIONS', '')}".strip()

        spark=(
            SparkSession.builder
            .appName(app_name)

            # Run locally using all available CPU cores
            .master("local[*]")

            #shuffle Partitions
            .config("spark.sql.shuffle.partitions", "8")

            # Enable Adaptive Query Execution
            .config("spark.sql.adaptive.enabled", "true")

            # Enable dynamic partition coalescing
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

            # Broadcast join threshold (10 MB)
            .config("spark.sql.autoBroadcastJoinThreshold", 10485760)

            # Use Apache Arrow for faster Pandas conversion
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")

            # Ignore corrupt files
            .config("spark.sql.files.ignoreCorruptFiles", "true")

            .config("spark.jars", str(JDBC_DRIVER))
            .config("spark.driver.extraClassPath", str(JDBC_DRIVER))
            .config("spark.executor.extraClassPath", str(JDBC_DRIVER))

            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        return spark
