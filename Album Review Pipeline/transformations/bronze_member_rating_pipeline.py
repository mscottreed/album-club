import dlt
from pyspark.sql.functions import col, input_file_name, current_timestamp
from pyspark.sql.types import StringType, IntegerType

@dlt.table
def bronze_member_rating():
    """
    Reads the raw CSV data as a streaming source.
    """
    path = "/Volumes/workspace/music/album-club-ratings"

    schema = """
        `Week_Number` INT,
        `Album_Number` INT,
        `Artist` STRING,
        `Album` STRING,
        `Album_Year` INT,
        `Meeting_Date` STRING,
        `PageId` INT,
        `FileName` STRING,
        `Member` STRING,
        `Rating` FLOAT,
        `FavoriteSongs` STRING,
        `RowIndex` INT
    """

    return (
        spark.readStream.schema(schema)
            .format("csv")
            .option("header", "true")
            .load(path)
            .select(
                col("Week_Number").alias("week_number"),
                col("Album_Number").alias("album_number"),
                col("Artist").alias("artist"),
                col("Album").alias("album_name"),
                col("Album_Year").alias("release_year"),
                col("Meeting_Date").alias("meeting_date"),
                col("Member").alias("member"),
                col("Rating").alias("rating"),
                col("FavoriteSongs").alias("favorite_songs"),
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingestion_time")
            )
    )
