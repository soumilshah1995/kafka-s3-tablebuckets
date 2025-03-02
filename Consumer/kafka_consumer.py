from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
import json
import argparse
import time
import logging
import sys
from urllib.parse import urlparse
import boto3
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("KafkaStreamProcessor")

class AvroSchema:
    """
    Handles the conversion of Avro schema to Spark schema
    Supports both local and S3 schema files
    """
    def __init__(self, path, spark):
        logger.info(f"Initializing AvroSchema with schema path: {path}")
        self.path = path
        self.spark = spark
        self.spark_schema = self.read_schema()

    def read_schema(self):
        """Read and parse schema file from local or S3 storage"""
        parsed_url = urlparse(self.path)
        if parsed_url.scheme in ['s3', 's3a']:
            logger.info(f"Reading schema from S3: {self.path}")
            schema_json = self._read_s3_file_schema(parsed_url)
        else:
            logger.info(f"Reading schema from local file: {self.path}")
            with open(self.path, 'r') as f:
                schema_json = f.read()

        avro_schema = json.loads(schema_json)
        logger.info("Successfully parsed schema JSON")
        return self._avro_to_spark_schema(avro_schema)

    def _read_s3_file_schema(self, parsed_url):
        """Read schema file from S3"""
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to read schema from S3: {str(e)}")
            raise

    def _avro_to_spark_schema(self, avro_schema):
        """
        Convert Avro schema to Spark schema
        Handles complex types like records, maps, arrays, and logical types
        """
        def convert_type(avro_type):
            """Convert Avro type to Spark type"""
            type_mapping = {
                'string': StringType(),
                'int': IntegerType(),
                'long': LongType(),
                'float': FloatType(),
                'double': DoubleType(),
                'boolean': BooleanType(),
                'timestamp-micros': TimestampType(),
                'date': DateType()
            }

            # Handle complex types
            if isinstance(avro_type, dict):
                if avro_type.get('type') == 'record':
                    return self._avro_to_spark_schema(avro_type)
                elif avro_type.get('type') == 'map':
                    return MapType(StringType(), convert_type(avro_type['values']))
                elif avro_type.get('type') == 'array':
                    return ArrayType(convert_type(avro_type['items']))
                elif avro_type.get('logicalType') == 'timestamp-micros':
                    return TimestampType()
                elif avro_type.get('logicalType') == 'date':
                    return DateType()

            return type_mapping.get(avro_type, StringType())

        fields = []
        for field in avro_schema['fields']:
            field_type = field['type']

            if isinstance(field_type, dict):
                spark_type = convert_type(field_type)
                nullable = True
            elif isinstance(field_type, list):
                non_null_type = next((t for t in field_type if t != 'null'), None)
                if non_null_type:
                    spark_type = convert_type(non_null_type)
                    nullable = 'null' in field_type
                else:
                    spark_type = StringType()
                    nullable = True
            else:
                spark_type = convert_type(field_type)
                nullable = False

            fields.append(StructField(field['name'], spark_type, nullable))

        logger.info(f"Created Spark schema with {len(fields)} fields")
        return StructType(fields)


class MergeQuery:
    """
    Handles MERGE operations against Iceberg tables
    Reads merge query from local file or S3 and executes it against the target table
    """
    def __init__(self, args):
        self.args = args
        self.merge_query_path = args.mergeQuery
        self.catalog_name = args.catalogName
        self.database_name = args.dataBaseName
        self.table_name = args.tableName
        self.parsed_url = urlparse(self.merge_query_path)
        logger.info(f"Initialized MergeQuery for table {self.catalog_name}.{self.database_name}.{self.table_name}")

    def read_merge_query(self):
        """Read merge query from file (local or S3)"""
        try:
            if self.parsed_url.scheme in ['s3', 's3a']:
                logger.info(f"Reading merge query from S3: {self.merge_query_path}")
                return self._read_s3_file()
            else:
                logger.info(f"Reading merge query from local file: {self.merge_query_path}")
                return self._read_local_file()
        except Exception as e:
            logger.error(f"Failed to read merge query: {str(e)}")
            raise

    def _read_s3_file(self):
        """Read file from S3"""
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'))
        return response['Body'].read().decode('utf-8')

    def _read_local_file(self):
        """Read file from local filesystem"""
        with open(self.merge_query_path, 'r') as f:
            return f.read()

    def execute_merge(self, spark, df):
        """Execute the merge query against the target table"""
        if df.count() == 0:
            logger.info("Empty dataframe, skipping merge operation")
            return 0

        # Format the query with the provided parameters
        merge_query = self.read_merge_query().format(
            catalogName=self.catalog_name,
            dataBaseName=self.database_name,
            tableName=self.table_name
        )

        logger.info(f"Executing merge query against {self.catalog_name}.{self.database_name}.{self.table_name}")
        logger.debug(f"Merge query: {merge_query}")

        # Register the dataframe as a temp view for the query
        df.createOrReplaceGlobalTempView("source_data")

        # Track time taken for the merge operation
        start_time = time.time()
        try:
            # Execute the merge query
            result = spark.sql(merge_query)
            rows_affected = result.collect()[0][0] if result.count() > 0 else 0
            duration = time.time() - start_time

            logger.info(f"Merge operation completed successfully in {duration:.2f} seconds. Rows affected: {rows_affected}")
            return rows_affected
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Merge operation failed after {duration:.2f} seconds: {str(e)}")
            raise
        finally:
            # Clean up the temp view
            spark.catalog.dropGlobalTempView("source_data")


class KafkaStreamProcessor(MergeQuery):
    """
    Main processor class that handles:
    1. Setting up Spark and Kafka connection
    2. Reading and parsing data from Kafka
    3. Executing merge operations for each batch
    4. Monitoring performance and handling errors
    """
    def __init__(self, args, schema_handler):
        self.args = args
        self.schema_handler = schema_handler
        MergeQuery.__init__(self, args=self.args)

        logger.info("Initializing Spark session for Kafka Stream Processor")
        self.spark = SparkSession.builder \
            .appName("KafkaStreamProcessor") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()

        # Set log level for Spark
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")

    def create_kafka_read_stream(self):
        """
        Create and configure the Kafka read stream
        Returns a DataFrame representing the stream
        """
        logger.info(f"Setting up Kafka read stream for topic: {self.args.topic}")
        logger.info(f"Kafka bootstrap servers: {self.args.bootstrap_servers}")
        logger.info(f"Consumer group: {self.args.consumerGroup}")

        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.args.bootstrap_servers) \
            .option("subscribe", self.args.topic) \
            .option("maxOffsetsPerTrigger", self.args.maxOffsetsPerTrigger) \
            .option("startingOffsets", self.args.startingOffsets) \
            .option("kafka.group.id", self.args.consumerGroup) \
            .option("kafka.session.timeout.ms", "60000") \
            .option("kafka.request.timeout.ms", "70000") \
            .option("kafka.heartbeat.interval.ms", "20000") \
            .option("failOnDataLoss", "false") \
            .option("enable.auto.commit", "true") \
            .load()

    def process_batch(self, batch_df, batch_id):
        """
        Process each micro-batch from the Kafka stream
        Parses the data and executes merge into the target table
        """
        batch_start_time = time.time()
        logger.info(f"=== Processing batch {batch_id} ===")

        try:
            # Count records in the batch
            batch_count = batch_df.count()
            logger.info(f"Batch {batch_id} contains {batch_count} records")

            if batch_count > 0:
                # Parse the JSON data using the Avro schema
                parse_start_time = time.time()
                parsed_batch_df = batch_df.select(
                    from_json(col("value").cast("string"), self.schema_handler.spark_schema).alias("data")
                ).select("data.*")

                parse_duration = time.time() - parse_start_time
                logger.info(f"Parsed {batch_count} records in {parse_duration:.2f} seconds")

                # Log a sample of the data for debugging (limited to 5 records)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Sample of parsed data:")
                    sample_df = parsed_batch_df.limit(5)
                    for row in sample_df.collect():
                        logger.debug(str(row))

                # Execute the merge operation
                try:
                    merge_start_time = time.time()
                    rows_affected = self.execute_merge(spark=self.spark, df=parsed_batch_df)
                    merge_duration = time.time() - merge_start_time

                    logger.info(f"Batch {batch_id}: Merged {rows_affected} rows in {merge_duration:.2f} seconds")
                except Exception as e:
                    logger.error(f"Batch {batch_id}: Error during merge operation: {str(e)}")
                    logger.error("Terminating the streaming job due to merge failure")
                    raise
            else:
                logger.info(f"Batch {batch_id} is empty, skipping merge operation")

            # Calculate and log total batch processing time
            batch_duration = time.time() - batch_start_time
            logger.info(f"Batch {batch_id}: Total processing time: {batch_duration:.2f} seconds")
            logger.info(f"=== Completed batch {batch_id} ===")

        except Exception as e:
            logger.error(f"Batch {batch_id}: Unhandled exception during processing: {str(e)}")
            logger.error("Terminating the streaming job due to critical failure")
            raise

    def start_streaming(self):
        """
        Start the Kafka streaming job
        Sets up the streaming query and awaits termination
        """
        logger.info("Starting Kafka streaming job")
        try:
            # Create the Kafka read stream
            kafka_df = self.create_kafka_read_stream()

            # Configure the streaming job
            logger.info(f"Checkpoint location: {self.args.checkpoint}")
            logger.info(f"Trigger interval: {self.args.minSyncInterval}")

            # Start the streaming query
            query = kafka_df.writeStream \
                .foreachBatch(self.process_batch) \
                .outputMode("append") \
                .trigger(processingTime=self.args.minSyncInterval) \
                .option("checkpointLocation", self.args.checkpoint) \
                .start()

            logger.info("Kafka streaming job started successfully")
            logger.info("Waiting for termination...")

            # Wait for the query to terminate
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Failed to start Kafka streaming job: {str(e)}")
            raise


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Kafka Stream Processor for Iceberg Tables")

    # Avro schema configuration
    parser.add_argument("--schema-file", required=True, help="Path to Avro schema file (local or S3)")

    # Kafka configuration
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers (comma-separated list)")
    parser.add_argument("--topic", required=True, help="Kafka topic name to consume from")
    parser.add_argument("--startingOffsets", default="latest", help="Starting offsets for Kafka: 'earliest', 'latest', or JSON string")
    parser.add_argument("--consumerGroup", required=True, help="Kafka consumer group ID")
    parser.add_argument("--maxOffsetsPerTrigger", default="10000", type=int, help="Maximum number of offsets per trigger interval")

    # Streaming configuration
    parser.add_argument("--minSyncInterval", default="1 minute", help="Minimum sync interval for trigger (e.g., '1 minute', '30 seconds')")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint location for Kafka stream (local path or S3 URI)")

    # Iceberg table configuration
    parser.add_argument("--mergeQuery", required=True, help="Path to merge query SQL file (local or S3)")
    parser.add_argument("--catalogName", required=True, help="Iceberg catalog name")
    parser.add_argument("--dataBaseName", required=True, help="Iceberg database name")
    parser.add_argument("--tableName", required=True, help="Iceberg table name")

    # Logging configuration
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Log level")

    return parser.parse_args()


def main():
    """Main entry point for the application"""
    # Parse command line arguments
    args = parse_arguments()

    # Configure log level
    logger.setLevel(getattr(logging, args.log_level))

    logger.info("Starting Kafka to Iceberg Stream Processor")
    logger.info(f"Configuration: topic={args.topic}, table={args.catalogName}.{args.dataBaseName}.{args.tableName}")

    try:
        # Initialize Spark session for schema handling
        spark = SparkSession.builder.getOrCreate()

        # Initialize schema handler
        schema_handler = AvroSchema(args.schema_file, spark)

        # Initialize and start the Kafka processor
        processor = KafkaStreamProcessor(args, schema_handler)
        processor.start_streaming()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Critical error in main process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
