"""
config/spark_session.py

Centralized SparkSession builder for the project.
All bronze/silver/gold jobs should import get_spark_session()
from here instead of creating their own SparkSession, so that
config stays consistent across the whole pipeline.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "AdventureWorksDW") -> SparkSession:
    """
    Build (or reuse) a SparkSession configured for local development.

    Parameters
    ----------
    app_name : str
        Name shown in the Spark UI / logs for this job.

    Returns
    -------
    SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # use all local cores; change for cluster deployment
        .config("spark.sql.shuffle.partitions", "4")  # lower default for local/small data
        .config("spark.sql.session.timeZone", "UTC")
        # Uncomment the two lines below if/when the project adopts Delta Lake:
        # .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # quiet down noisy INFO logs

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop the given SparkSession."""
    spark.stop()


if __name__ == "__main__":
    # Quick sanity check: run this file directly to confirm Spark starts correctly.
    spark = get_spark_session()
    print(f"Spark session started. Version: {spark.version}")
    stop_spark_session(spark)
