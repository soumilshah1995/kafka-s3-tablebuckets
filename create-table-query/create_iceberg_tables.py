import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


def create_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("IcebergTableCreation")
    return builder.getOrCreate()


def setup_iceberg_table(spark: SparkSession, catalog: str, namespace: str, table: str) -> None:
    # Create the namespace if it doesn't exist
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{catalog}`.`{namespace}`")

    # Create the Iceberg table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.`{namespace}`.`{table}` (
        emp_id STRING,
        employee_name STRING,
        department STRING,
        state STRING,
        salary INT,
        age INT,
        bonus INT,
        ts BIGINT
    ) USING iceberg
    """)

    print(f"Iceberg table `{catalog}`.`{namespace}`.`{table}` created successfully.")


def main():
    # Configuration
    catalog = "ManagedIcebergCatalog"
    namespace = "default"
    table = "employees"

    spark = create_spark_session()

    try:
        setup_iceberg_table(spark, catalog, namespace, table)
    finally:
        spark.stop()





if __name__ == "__main__":
    main()
