from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

row = spark.sql("SELECT current_user() AS user, current_catalog() AS catalog, current_timestamp() AS ts").first()
print(f"Hello, Album Club! Running as {row.user} in catalog '{row.catalog}' at {row.ts}")
