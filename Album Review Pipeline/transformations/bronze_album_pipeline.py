import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, input_file_name, current_timestamp
from pyspark.sql.types import StringType, IntegerType

@dlt.table
def bronze_album():
    """
    Reads the raw CSV data as a streaming source.
    """
    path = "/Volumes/workspace/music/albums-1001-before-you-die"

    schema = """
        `Album #` INT, 
        `Artist` STRING,
        `Album Name` STRING, 
        `Release Year` INT
    """

    return (
        spark.readStream.schema(schema)
            .format("csv")
            .option("header", "true")
            .load(path)
            .select(
                col("Album #").alias("id"),
                col("Artist").alias("artist"),
                col("Album Name").alias("album_name"),
                col("Release Year").alias("release_year"),
                lit(None).cast(StringType()).alias("release_date"),
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingestion_time")
            )
    )

@dlt.append_flow(target="bronze_album")
def volume_2_flow():

    path = "/Volumes/workspace/music/albums-best-selling"

    schema = """
        `Rank` INT, 
        `Artist` STRING, 
        `Album` STRING,
        `Release Date` STRING
    """

    return (
        spark.readStream.schema(schema)
            .format("csv")
            .option("header", "true")
            .load(path)
            .select(
                (F.col("Rank") + 2000).alias("id"),
                col("Artist").alias("artist"),
                col("Album").alias("album_name"),
                lit(None).cast(IntegerType()).alias("release_year"),
                col("Release Date").alias("release_date"),
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingestion_time")
            )
    )

@dlt.append_flow(target="bronze_album")
def volume_3_flow():

    path = "/Volumes/workspace/music/albums-supplemental"

    schema = """
        `Album Num` INT, 
        `Artist` STRING, 
        `Album Name` STRING, 
        `Release Year` INT
    """

    return (
        spark.readStream.schema(schema)
            .format("csv")
            .option("header", "true")
            .load(path)
            .select(
                (F.col("Album Num") + 3000).alias("id"),
                col("Artist").alias("artist"),
                col("Album Name").alias("album_name"),
                col("Release Year").alias("release_year"),
                lit(None).cast(StringType()).alias("release_date"),
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingestion_time")
            )
    )
