import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
                     StructField("crime_id",StringType(),False),
                     StructField("original_crime_type_name",StringType(),True),
                     StructField("report_date",StringType(),True),
                     StructField("call_date",StringType(),True),
                     StructField("offense_date",StringType(),True),
                     StructField("call_time",StringType(),True),
                     StructField("call_date_time",StringType(),True),
                     StructField("disposition",StringType(),True),
                     StructField("address",StringType(),True),
                     StructField("city",StringType(),True),
                     StructField("state",StringType(),True),
                     StructField("agency_id",StringType(),True),
                     StructField("address_type",StringType(),True),
                     StructField("common_location",StringType(),True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka')\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe","org.sfo.crime.data")\
        .option("startingOffsets","earliest")\
        .option("maxRatePerPartition",10)\
        .option("maxOffsetPerTrigger",10)\
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    service_table.printSchema()
    distinct_table = service_table.select(psf.to_timestamp("call_date_time").alias("call_date_time"),"original_crime_type_name","disposition")
    
    #distinct_table.printSchema()
    
    #query = distinct_table \
    #        .writeStream \
    #        .format("console") \
    #        .start()
    
    #query.awaitTermination()

    # count the number of original crime type
    agg_df = distinct_table\
             .withWatermark("call_date_time",  "30 minutes")\
             .groupBy("original_crime_type_name",\
                     psf.window("call_date_time","10 minutes","5 minutes"))\
             .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()


    # TODO attach a ProgressReporter
    ##query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath,multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = distinct_table.join(radio_code_df,"disposition")
   
    
    radio_join_df =  join_query\
                     .writeStream\
                     .outputMode("append")\
                     .format("console")\
                     .start()

    radio_join_df.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port",3000)\
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

