import dlt
from pyspark.sql.functions import col, row_number, when, year
from pyspark.sql.window import Window

@dlt.table()
def silver_artist():
    bronze_album_df = dlt.read("music.bronze_album") # Read as batch, not stream
    silver_data = (
        bronze_album_df
        .select("artist")
        .distinct()
        .withColumnRenamed("artist", "name")
    )
    window_spec = Window.orderBy("name")
    silver_artist_df = silver_data.withColumn("id", 
        row_number().over(window_spec).cast("long"))
    silver_artist_df = silver_artist_df.select("id", "name")
    return silver_artist_df
