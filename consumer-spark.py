from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, LongType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from time import sleep

# Initialize Spark session
spark = (SparkSession
         .builder
         .master('local[*]')
         .appName('wiki-events-consumer')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

# Define Kafka topic and server details
kafka_bootstrap_servers = 'kafka:9092'  # Replace with actual Kafka service name and port
kafka_topic = 'wikipedia-events'

# Define schema for event data
schema_wiki = StructType([
    StructField("$schema", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("id", StringType(), True),
    StructField("length", StructType([
        StructField("new", IntegerType(), True),
        StructField("old", IntegerType(), True)
    ]), True),
    StructField("meta", StructType([
        StructField("domain", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("id", StringType(), True),
        StructField("offset", LongType(), True),
        StructField("partition", LongType(), True),
        StructField("request_id", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("uri", StringType(), True)
    ]), True),
    StructField("minor", BooleanType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("revision", StructType([
        StructField("new", IntegerType(), True),
        StructField("old", IntegerType(), True)
    ]), True),
    StructField("server_name", StringType(), True),
    StructField("server_script_path", StringType(), True),
    StructField("server_url", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("user", StringType(), True),
    StructField("wiki", StringType(), True)
])

# Create a streaming DataFrame from Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic)
      .option("startingOffsets", "earliest")
      .load())

# Convert binary Kafka values to string
df1 = (df
       .withColumn("key", df["key"].cast(StringType()))
       .withColumn("value", df["value"].cast(StringType())))

# Parse JSON and define schema
df_wiki = (df1
           .withColumn("value", from_json("value", schema_wiki)))

# Format the DataFrame and perform necessary transformations
df_wiki_formatted = (df_wiki.select(
    col("key").alias("event_key"),
    col("topic").alias("event_topic"),
    col("value.$schema").alias("schema"),
    "value.bot",
    "value.comment",
    "value.id",
    col("value.length.new").alias("length_new"),
    col("value.length.old").alias("length_old"),
    "value.minor",
    "value.namespace",
    "value.parsedcomment",
    "value.patrolled",
    col("value.revision.new").alias("revision_new"),
    col("value.revision.old").alias("revision_old"),
    "value.server_name",
    "value.server_script_path",
    "value.server_url",
    to_timestamp(col("value.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("change_timestamp"),
    to_date(to_timestamp(col("value.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")).alias("change_timestamp_date"),
    "value.title",
    "value.type",
    "value.user",
    "value.wiki",
    col("value.meta.domain").alias("meta_domain"),
    col("value.meta.dt").alias("meta_dt"),
    col("value.meta.id").alias("meta_id"),
    col("value.meta.offset").alias("meta_offset"),
    col("value.meta.partition").alias("meta_partition"),
    col("value.meta.request_id").alias("meta_request_id"),
    col("value.meta.stream").alias("meta_stream"),
    col("value.meta.topic").alias("meta_topic"),
    col("value.meta.uri").alias("meta_uri")
))

# Define paths for writing data
raw_path = "/opt/bitnami/spark/wiki-changes"
checkpoint_path = "/opt/bitnami/spark/wiki-changes-checkpoint"

# Write stream to Parquet files
queryStream = (df_wiki_formatted
               .writeStream
               .format("parquet")
               .queryName("wiki_changes_ingestion")
               .option("checkpointLocation", checkpoint_path)
               .option("path", raw_path)
               .outputMode("append")
               .partitionBy("change_timestamp_date", "server_name")
               .start())

# Read parquet files as a stream to output the number of rows
df_wiki_changes = (spark
                   .readStream
                   .format("parquet")
                   .schema(df_wiki_formatted.schema)
                   .load(raw_path))

# Output to memory to count rows
queryStreamMem = (df_wiki_changes
                  .writeStream
                  .format("memory")
                  .queryName("wiki_changes_count")
                  .outputMode("update")
                  .start())

# Monitor and print row counts
try:
    i = 1
    while len(spark.streams.active) > 0:
        print("Run: {}".format(i))
        
        lst_queries = [s.name for s in spark.streams.active]

        if "wiki_changes_count" in lst_queries:
            spark.sql("SELECT COUNT(1) AS qty FROM wiki_changes_count").show()
        else:
            print("'wiki_changes_count' query not found.")

        sleep(5)
        i += 1

except KeyboardInterrupt:
    queryStreamMem.stop()
    print("Stream process interrupted")

# Check active streams
for s in spark.streams.active:
    print("ID:{} | NAME:{}".format(s.id, s.name))

# Stop ingestion
queryStream.stop()
