from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, to_json, struct

# Kafka configuration
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Athlete Data Processing")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "mysql:mysql-connector-java:8.0.32,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .getOrCreate()
)

# MySQL database connection properties
db_url = "jdbc:mysql://217.61.57.46:3306/neo_data"
db_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}

# 1. Read athlete bio data from MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .option("url", db_url)
    .option("dbtable", "olympic_dataset.athlete_bio")
    .options(**db_properties)
    .load()
)

# 2. Filter invalid height and weight data
filtered_athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("float").isNotNull())
    & (col("weight").cast("float").isNotNull())
)

# 3. Read athlete event results and write to Kafka topic
athlete_event_results_df = (
    spark.read.format("jdbc")
    .option("url", db_url)
    .option("dbtable", "olympic_dataset.athlete_event_results")
    .options(**db_properties)
    .load()
)

athlete_event_results_df.select(
    to_json(struct("athlete_id", "event", "medal", "country_noc", "sport")).alias(
        "value"
    )
).write.format("kafka").option(
    "kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config",
    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
).option(
    "topic", "athlete_event_results"
).save()

# Read results data from Kafka topic
kafka_results_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
    )
    .option("subscribe", "athlete_event_results")
    .load()
)

results_df = (
    kafka_results_df.selectExpr("CAST(value AS STRING) as json")
    .selectExpr(
        "from_json(json, 'athlete_id STRING, event STRING, medal STRING, country_noc STRING, sport STRING') AS data"
    )
    .select("data.*")
)

# Add a timestamp column for watermarking
results_df = results_df.withColumn("timestamp", current_timestamp())

# Add watermark to streaming data
results_df_with_watermark = results_df.withWatermark("timestamp", "10 minutes")

# 4. Join bio data with results data
joined_df = results_df_with_watermark.join(
    filtered_athlete_bio_df, on="athlete_id", how="inner"
).select(
    results_df["athlete_id"],
    results_df["event"],
    results_df["medal"],
    results_df["sport"],
    results_df["country_noc"].alias("result_country_noc"),
    filtered_athlete_bio_df["height"],
    filtered_athlete_bio_df["weight"],
    filtered_athlete_bio_df["sex"].alias("gender"),
    filtered_athlete_bio_df["country_noc"].alias("bio_country_noc"),
)

# 5. Calculate averages
grouped_avg_df = joined_df.groupBy(
    "sport", "medal", "gender", "result_country_noc"
).agg(
    avg(col("height")).alias("avg_height"),
    avg(col("weight")).alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)


# 6a. Stream data to output Kafka topic
def write_to_kafka(batch_df, batch_id):
    batch_df.select(
        to_json(
            struct(
                "sport",
                "medal",
                "gender",
                "result_country_noc",
                "avg_height",
                "avg_weight",
                "timestamp",
            )
        ).alias("value")
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"])
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
    ).option(
        "topic", "athlete_summary"
    ).save()


grouped_avg_df.writeStream.foreachBatch(write_to_kafka).start()


# 6b. Stream data to database
def write_to_mysql(batch_df, batch_id):
    batch_df.write.format("jdbc").option("url", db_url).option(
        "dbtable", "vchub_athlete_summary"
    ).options(**db_properties).mode("append").save()


grouped_avg_df.writeStream.foreachBatch(write_to_mysql).start().awaitTermination()
