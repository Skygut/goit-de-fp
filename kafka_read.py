from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

spark = (
    SparkSession.builder.appName("KafkaLocalSparkProcessing")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "mysql:mysql-connector-java:8.0.32,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .getOrCreate()
)

json_schema = StructType(
    [
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("noc_country", StringType(), True),
        StructField("avg_height", StringType(), True),
        StructField("avg_weight", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

kafka_config = {"bootstrap_servers": ["localhost:9092"]}

data_from_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "oleh_aggregated_athlete_stats")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "50")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), json_schema).alias("data"))
    .select("data.*")
)

data_from_kafka.writeStream.trigger(availableNow=True).outputMode("append").format(
    "console"
).option("truncate", "false").start().awaitTermination()
