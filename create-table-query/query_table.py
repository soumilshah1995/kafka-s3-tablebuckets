from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergS3TablesIntegration")
        .config("spark.jars.packages",
                "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ManagedIcebergCatalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ManagedIcebergCatalog.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config("spark.sql.catalog.ManagedIcebergCatalog.warehouse",
                "<>")
        .config("spark.sql.catalog.ManagedIcebergCatalog.client.region", "us-east-1")
        .getOrCreate()
    )


# Example usage
spark = create_spark_session()

spark.sql("SELECT * FROM ManagedIcebergCatalog.default.employees").show()
spark.sql("SELECT COUNT(*) FROM ManagedIcebergCatalog.default.employees").show()


