import dlt
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

@dlt.table()
def silver_album():
    bronze_album_df = dlt.read("music.bronze_album") # read as batch, not stream
    silver_artist_df = dlt.readStream("silver_artist")

    window_spec = Window.partitionBy("artist", "album_name").orderBy("release_date")
    distinct_album_df = (
        bronze_album_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
    )

    silver_album_df = (
        distinct_album_df
        .join(
            silver_artist_df,
            distinct_album_df["artist"] == silver_artist_df["name"]
        )
        .select(
            distinct_album_df["id"],
            silver_artist_df["id"].alias("artist_id"),
            distinct_album_df["album_name"].alias("name"),
            distinct_album_df["release_year"],
            distinct_album_df["release_date"],
        )
    )

    return silver_album_df