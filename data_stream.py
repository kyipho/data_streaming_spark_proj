import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# DONE Create a schema for incoming resources
schema = StructType([
    StructField('address', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('city', StringType(), True),
    StructField('common_location', StringType(), True),
    StructField('crime_id', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('state', StringType(), True)
])

def run_spark_job(spark):

    # DONE Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port

    # suppress INFO messages to see output more easily
#     spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.sf.crime.stats") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # DONE extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    kafka_df = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # DONE select original_crime_type_name and disposition
#     distinct_table = selectExpr("original_crime_type_name", "disposition").distinct()

    # count the number of original crime type
    agg_df = kafka_df.groupBy("original_crime_type_name", "disposition").count()

    # DONE Q1. Submit a screen shot of a batch ingestion of the aggregation
    # DONE write output stream
    query = (
        agg_df
            .writeStream
            .format("console")  # memory = store in-memory table (for testing only in Spark 2.0)
            .queryName("counts")  # counts = name of the in-memory table
            .outputMode("complete")  # complete = all the counts should be in the table
            .start()
    )

    # DONE attach a ProgressReporter
    query.awaitTermination()

    # DONE get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(
        radio_code_json_filepath,
        # data will be corrupted if line below is missing
        multiLine=True,
    )

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # DONE rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # DONE join on disposition column. reorder output columns with .select()
    join_query = agg_df.join(other=radio_code_df, on="disposition", how='inner') \
        .select("original_crime_type_name", "disposition", "description", "count") \
        .writeStream \
        .format("console") \
        .queryName("counts_joined") \
        .outputMode("complete") \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # DONE Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", 3000) \
        .config("spark.streaming.kafka.maxRatePerPartition", 20) \
        .config("spark.streaming.receiver.maxRate", 0) \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
