from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from album_club.onenote_export import split_mht

PAGES = T.ArrayType(
    T.StructType(
        [
            T.StructField("page_number", T.IntegerType()),
            T.StructField("title", T.StringType()),
            T.StructField("created", T.StringType()),
            T.StructField("html", T.StringType()),
        ]
    )
)

split_mht_udf = F.udf(split_mht, PAGES)


@dp.table(comment="One row per OneNote page, split out of the .mht section exports as-is.")
def bronze_onenote_pages():
    exports_path = spark.conf.get("album_club.onenote_exports_path")  # noqa: F821 (spark is provided by the pipeline)
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.mht")
        .load(exports_path)
        .select(
            F.col("_metadata.file_path").alias("source_file"),
            F.col("modificationTime").alias("source_file_modified_at"),
            F.explode(split_mht_udf("content")).alias("page"),
        )
        .select("source_file", "source_file_modified_at", "page.*", F.current_timestamp().alias("ingested_at"))
    )
