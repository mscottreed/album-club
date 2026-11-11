from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Raw Spotify and MusicBrainz responses, one row per request, from the fetch_enrichment job task.")
def bronze_api_responses():
    responses_path = spark.conf.get("album_club.api_responses_path")  # noqa: F821 (spark is provided by the pipeline)
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl")
        .schema("source string, request string, status int, body string, fetched_at timestamp")
        .load(responses_path)
        .select("*", F.col("_metadata.file_path").alias("source_file"), F.current_timestamp().alias("ingested_at"))
    )
