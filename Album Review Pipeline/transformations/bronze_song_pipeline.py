import dlt
from pyspark.sql.functions import col, input_file_name, current_timestamp

@dlt.table
def bronze_song():
    path = "/Volumes/workspace/music/albums-1001-tracks"

    schema = """
        `album_num` INT,
        `track_number` INT,
        `name` STRING,
        `duration_ms` INT
    """

    return (
        spark.readStream.schema(schema)
            .format("csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(path)
            .select(
                col("album_num").alias("album_number"),
                col("track_number"),
                col("name"),
                col("duration_ms"),
                col("_metadata.file_path").alias("source_file"),
                current_timestamp().alias("ingestion_time")
            )
    )