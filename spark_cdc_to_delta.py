# cdc_preprocess.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    get_json_object, col, expr, lower, trim, regexp_replace, regexp_extract,
    split, when, length, initcap, current_timestamp, lit
)

# --- Spark session (Kafka + Delta) ---
spark = (SparkSession.builder.appName("CDC->CleanDelta")
 .config("spark.jars.packages",
         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0")
 .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
 .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
 .getOrCreate())

# --- Source: Debezium topic inside Docker network ---
raw = (
  spark.readStream.format("kafka") # streaming source
 .option("kafka.bootstrap.servers","kafka:9092") # how to reach the broker
 .option("subscribe","dbserver1.cog.customers") # topic name to listen to
 .option("startingOffsets","latest") # start point ("earliest" or "latest")
 .load() # actually build the streaming source
 .selectExpr("CAST(value AS STRING) AS json", "topic", "partition", "offset"))


# --- Minimal extraction from Debezium payload ---
base = (raw
 .withColumn("op", get_json_object("json","$.payload.op"))
 .withColumn("id", expr("CAST(get_json_object(json,'$.payload.after.id') AS INT)"))
 .withColumn("name", get_json_object("json","$.payload.after.name"))
 .withColumn("email", get_json_object("json","$.payload.after.email"))
 .withColumn("updated_at", get_json_object("json","$.payload.after.updated_at"))
 .withColumn("processed_at", current_timestamp())
 .withColumn("src_topic", col("topic"))
 .withColumn("src_partition", col("partition"))
 .withColumn("src_offset", col("offset")))

# --- Keep deletes separately for downstream MERGE ---
deletes = raw.where(get_json_object("json","$.payload.op") == "d") \
    .selectExpr(
        "CAST(get_json_object(json,'$.payload.before.id') AS INT) AS id",
        "get_json_object(json,'$.payload.before.updated_at') AS updated_at",
        "'d' AS op",
        "current_timestamp() AS processed_at",
        "topic AS src_topic", "partition AS src_partition", "offset AS src_offset"
    )

# --- EMAIL preprocessing ---
EMAIL_RE = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}$'
email_clean = (base.where(col("op").isin("c","u"))
  .withColumn("email", lower(trim(col("email"))))
  .withColumn("email", regexp_replace("email", r"\s+", ""))  # collapse spaces
  .withColumn("email_domain", split(col("email"), "@").getItem(1))
  .withColumn("email_local",  split(col("email"), "@").getItem(0))
  # canonicalize plus-addressing for common providers
  .withColumn("email_local",
      when(col("email_domain").isin("gmail.com","googlemail.com","outlook.com","hotmail.com"),
           regexp_replace(col("email_local"), r"\+.*$", ""))
      .otherwise(col("email_local")))
  .withColumn("email_domain",
      when(col("email_domain")=="googlemail.com","gmail.com").otherwise(col("email_domain")))
  .withColumn("email", expr("concat(email_local, '@', email_domain)"))
  .withColumn("is_valid_email", regexp_extract(col("email"), EMAIL_RE, 0) != "")
)

# --- NAME preprocessing ---
NAME_ALLOWED_EDGE = r"[^A-Za-zÀ-ÖØ-öø-ÿ'’ -]"  # keep letters, space, hyphen, apostrophe
name_clean = (email_clean
  .withColumn("name", regexp_replace("name", r"[\u0000-\u001F\u007F]", ""))  # control chars
  .withColumn("name", regexp_replace("name", r"\s+", " "))
  .withColumn("name", trim(col("name")))
  .withColumn("name", regexp_replace("name", NAME_ALLOWED_EDGE, ""))
  .withColumn("name", initcap(lower(col("name"))))
  .withColumn("is_valid_name", (length(col("name")) >= 2) & expr("name RLIKE '[A-Za-zÀ-ÖØ-öø-ÿ]'"))
  .withColumn("first_name", split(col("name"), " ").getItem(0))
  .withColumn("last_name",
      when(length(col("name")) - length(regexp_replace(col("name"), " ", "")) > 0,
           split(col("name"), " ").getItem(1)).otherwise(lit(None)))
)

# --- Routing: good vs quarantine ---
clean_good = name_clean.where(col("is_valid_email") & col("is_valid_name"))
quarantine = name_clean.where(~col("is_valid_email") | ~col("is_valid_name") | col("id").isNull())

# --- Sinks (local Delta) ---
q1 = (clean_good.writeStream.format("delta")
  .option("checkpointLocation","./chk/customers_clean")
  .outputMode("append")
  .start("./delta/customers_cdc_clean"))

q2 = (quarantine.writeStream.format("delta")
  .option("checkpointLocation","./chk/customers_quarantine")
  .outputMode("append")
  .start("./delta/customers_cdc_quarantine"))

q3 = (deletes.writeStream.format("delta")
  .option("checkpointLocation","./chk/customers_deletes")
  .outputMode("append")
  .start("./delta/customers_cdc_deletes"))

q1.awaitTermination()