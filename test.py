from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StructField, StructType
from configs import kafka_config

import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)
# Встановлення змінних середовища для PySpark
# os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Adm\\AppData\\Roaming\\Python\\Python311\\Scripts'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Adm\\AppData\\Roaming\\Python\\Python311\\Scripts'


# # Налаштування конфігурації SQL бази даних
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# # Створення Spark сесії
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("JDBCToKafka")
    .master("local[*]")
    .getOrCreate()
)
# Перевірка версій бібліотек
print("Spark version:", spark.version)

# Читання даних з SQL бази даних
jdbc_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
        partitionColumn="result_id",  # Колонка для розділення даних на партиції
        lowerBound=1,  # Нижня межа значень колонки
        upperBound=1000000,  # Верхня межа значень колонки
        numPartitions="10",  # Кількість партицій
    )
    .load()
)


# Відправка даних до Kafka
jdbc_df.selectExpr(
    "CAST(result_id AS STRING) AS key", "to_json(struct(*)) AS value"
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
).option(
    "kafka.security.protocol", "SASL_PLAINTEXT"
).option(
    "kafka.sasl.mechanism", "PLAIN"
).option(
    "kafka.sasl.jaas.config",
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
).option(
    "topic", "artur_home_athlete_event_results"
).save()

# Визначення схеми для JSON-даних
schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Читання даних з Kafka у стрімінговий DataFrame
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "vchub_athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.athlete_id", "data.sport", "data.medal")
)

# Виведення отриманих даних на екран
# kafka_streaming_df.writeStream \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start() \
#     .awaitTermination()

# Читання даних з SQL бази даних
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="athlete_bio",
        user=jdbc_user,
        password=jdbc_password,
        partitionColumn="athlete_id",  # Колонка для розділення даних на партиції
        lowerBound=1,  # Нижня межа значень колонки
        upperBound=1000000,  # Верхня межа значень колонки
        numPartitions="10",  # Кількість партицій
    )
    .load()
)

# Відфільтрування даних, де показники зросту та ваги є порожніми або не є числами
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)

# Об'єднання даних з результатами змагань з біологічними даними
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

# Обчислення середнього зросту і ваги атлетів
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)


# Функція для запису даних у Kafka та базу даних
def foreach_batch_function(df, epoch_id):
    # Запис даних до Kafka
    df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
    ).option(
        "kafka.security.protocol", "SASL_PLAINTEXT"
    ).option(
        "kafka.sasl.mechanism", "PLAIN"
    ).option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    ).option(
        "topic", "vchub_athlete_aggregated"
    ).save()

    # Запис даних до бази даних
    df.write.format("jdbc").options(
        url="jdbc:mysql://217.61.57.46:3306/neo_data",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="vchub_athlete_aggregated",
        user=jdbc_user,
        password=jdbc_password,
    ).mode("append").save()


# Запуск стрімінгу
aggregated_df.writeStream.outputMode("complete").foreachBatch(
    foreach_batch_function
).option("checkpointLocation", "/path/to/checkpoint/dir").start().awaitTermination()


# Ініціалізація Kafka Producer


# ----------------------------------------------
# Функція для відправки кожного рядка DataFrame до Kafka
# def send_partition_to_kafka(partition):
#     producer = KafkaProducer(
#         bootstrap_servers=kafka_config['bootstrap_servers'],
#         security_protocol=kafka_config['security_protocol'],
#         sasl_mechanism=kafka_config['sasl_mechanism'],
#         sasl_plain_username=kafka_config['username'],
#         sasl_plain_password=kafka_config['password'],
#         value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#         key_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )
#     for row in partition:
#         try:
#             data = row.asDict()
#             producer.send("artur_home_athlete_event_results", key=str(data['athlete_id']), value=data)
#             print(f"Sent data: {data} to topic successfully.")
#         except Exception as e:
#             print(f"An error occurred: {e}")
#     producer.flush()
#     producer.close()

# # Відправка кожної партиції DataFrame до Kafka
# kafka_df.foreachPartition(send_partition_to_kafka)
