import dlt
from pyspark.sql.functions import col, row_number, when, split
from pyspark.sql.window import Window

@dlt.table
def silver_album_club_member():
    bronze_member_df = dlt.read("music.bronze_album_club_member")
    silver_data = bronze_member_df \
        .withColumnRenamed("Name", "full_name") \
        .withColumnRenamed("DateOfBirth", "date_of_birth")
    window_spec = Window.orderBy("date_of_birth")
    silver_data = silver_data.withColumn("id", row_number().over(window_spec).cast("long"))
    silver_data = silver_data.withColumn("short_name", 
        when(col("full_name").startswith("Bill"), col("full_name")).otherwise(split(col("full_name"), " ")[0]))
    silver_data = silver_data.select("id", "full_name", "date_of_birth", "short_name")
    return silver_data