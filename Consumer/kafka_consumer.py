from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import json
import argparse
from urllib.parse import urlparse
import boto3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *


class AvroSchema:
    def __init__(self, path, spark):
        self.path = path
        self.spark = spark
        self.spark_schema = self.read_schema()

    def read_schema(self):
        parsed_url = urlparse(self.path)
        if parsed_url.scheme in ['s3', 's3a']:
            schema_json = self._read_s3_file_schema(parsed_url)
        else:
            with open(self.path, 'r') as f:
                schema_json = f.read()

        avro_schema = json.loads(schema_json)
        return self._avro_to_spark_schema(avro_schema)

    def _read_s3_file_schema(self, parsed_url):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip('/'))
        return response['Body'].read().decode('utf-8')

    def _avro_to_spark_schema(self, avro_schema):
        def convert_type(avro_type):
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
            if isinstance(avro_type, dict):
                if avro_type.get('type') == 'record':
                    return self._avro_to_spark_schema(avro_type)
                elif avro_type.get('type') == 'map':
                    return MapType(StringType(), convert_type(avro_type['values']))
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
                non_null_type = next(t for t in field_type if t != 'null')
                spark_type = convert_type(non_null_type)
                nullable = 'null' in field_type
            else:
                spark_type = convert_type(field_type)
                nullable = False
            fields.append(StructField(field['name'], spark_type, nullable))
        return StructType(fields)


# ==================================================================
# Merge Query Class
# ====================================================================
class MergeQuery:
    def __init__(self, merge_query_path):
        self.merge_query_path = merge_query_path
        self.parsed_url = urlparse(merge_query_path)

    def read_merge_query(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._read_s3_file()
        else:
            return self._read_local_file()

    def _read_s3_file(self):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=self.parsed_url.netloc, Key=self.parsed_url.path.lstrip('/'))

        return response['Body'].read().decode('utf-8')

    def _read_local_file(self):
        with open(self.merge_query_path, 'r') as f:
            return f.read()

    def execute_merge(self, spark, df):
        merge_query = self.read_merge_query()
        df.createOrReplaceGlobalTempView("source_data")
        spark.sql(merge_query).show()
        spark.catalog.dropGlobalTempView("source_data")


class KafkaStreamProcessor(MergeQuery):
    def __init__(self, args, schema_handler):
        self.args = args
        self.schema_handler = schema_handler
        MergeQuery.__init__(self, merge_query_path=self.args.mergeQuery)
        self.spark = SparkSession.builder.appName("KafkaStreamProcessor").getOrCreate()

    def create_kafka_read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.args.bootstrap_servers) \
            .option("subscribe", self.args.topic) \
            .option("maxOffsetsPerTrigger", self.args.maxOffsetsPerTrigger) \
            .option("startingOffsets", self.args.startingOffsets) \
            .option("kafka.group.id", self.args.consumerGroup) \
            .option("enable.auto.commit", "true") \
            .load()

    def process_batch(self, batch_df, batch_id):
        if batch_df.count() > 0:
            parsed_batch_df = batch_df.select(
                from_json(col("value").cast("string"), self.schema_handler.spark_schema).alias("data")
            ).select("data.*")

            print("===================================")
            try:
                self.execute_merge(spark=self.spark, df=parsed_batch_df)
                print(f" merging into Iceberg table Complete")
            except Exception as e:
                print(f"Error merging into Iceberg table: {str(e)}")
                raise
            print("===================================")

    def start_streaming(self):
        kafka_df = self.create_kafka_read_stream()
        query = kafka_df.writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("append") \
            .trigger(processingTime=self.args.minSyncInterval) \
            .option("checkpointLocation", self.args.checkpoint) \
            .start()
        query.awaitTermination()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka Stream Processor")
    parser.add_argument("--schema-file", required=True, help="Path to Avro schema file")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--startingOffsets", required=True, help="Starting offsets for Kafka")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--minSyncInterval", default="1 minute", help="Minimum sync interval for trigger")
    parser.add_argument("--maxOffsetsPerTrigger", default="1000", help="Maximum number of offsets per trigger interval")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint location for Kafka stream")
    parser.add_argument("--consumerGroup", required=True, help="Consumer Group Required")
    parser.add_argument("--mergeQuery", required=True, help="Consumer Group Required")
    return parser.parse_args()


def main():
    args = parse_arguments()
    schema_handler = AvroSchema(args.schema_file, SparkSession.builder.getOrCreate())
    processor = KafkaStreamProcessor(args, schema_handler)
    processor.start_streaming()


if __name__ == "__main__":
    main()
