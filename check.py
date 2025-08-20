from pyspark.sql import SparkSession

spark=(SparkSession.builder.appName("CheckDelta")
 .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
 .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
 .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
 .getOrCreate())

path="/Users/vinushan/Desktop/lakehouse/delta/customers_cdc"  
df=spark.read.format("delta").load(path)

print("COUNT =", df.count())
df.printSchema()
df.orderBy("id","updated_at").show(truncate=False)
df.write.mode("overwrite").json("/tmp/check_output")