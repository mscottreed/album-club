"""Silver tables: the bronze OneNote pages parsed into meetings, listens, ratings and favorites.

The parsing is plain Python in album_club (tested locally); this file wires it into the pipeline.
The rules are in docs/data-decisions.md.
"""

import pandas as pd
from pyspark import cloudpickle
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

import album_club
from album_club import seeds
from album_club.meeting_dates import assign_meeting_dates
from album_club.meeting_page import parse_meeting_page, parse_pasted_table

TRACK = "struct<disc:int, track:int>"
RESPONSE = (
    "struct<name_as_written:string, person:string, listen:string, rating:double, rating_raw:string, "
    f"favorites_raw:string, favorites_overridden:boolean, ranked:array<{TRACK}>, likes_rest:boolean, "
    f"excluded:array<{TRACK}>, review_reasons:array<string>>"
)
MEETING_PAGE = (
    "struct<week:int, list_number:int, year:int, artist:string, album:string, created_at:timestamp, "
    f"body_date:date, tracks_played:array<int>, spotify_track_ids:array<string>, responses:array<{RESPONSE}>, review_reasons:array<string>>"
)
# The workers that run UDFs can't import album_club, so ship its code with each function instead.
cloudpickle.register_pickle_by_value(album_club)

# Seeds are read here, on the driver, and travel to the workers inside the functions below.
PEOPLE = seeds.people()
FAVORITES_OVERRIDES = seeds.favorites_overrides()
MEETING_DATE_OVERRIDES = seeds.meeting_date_overrides()
LISTEN_OVERRIDES = seeds.listen_overrides()

parse_meeting_page_udf = F.udf(
    lambda title, created, html: parse_meeting_page(title, created, html, PEOPLE, FAVORITES_OVERRIDES), MEETING_PAGE
)
# Spark can call a UDF on every row, even inside F.when, so weeks without a pasted table get None.
parse_pasted_table_udf = F.udf(
    lambda week, text: None if text is None else parse_pasted_table(week, text, PEOPLE, FAVORITES_OVERRIDES),
    f"array<{RESPONSE}>",
)

SELECTION_METHOD = (
    F.when(F.col("week") <= 38, "1001 Albums list")
    .when(F.col("week") <= 69, "Democratic")
    .when(F.col("week") <= 85, "Democratic to Autocratic")
    .otherwise("Autocratic")
)


ENTERED_MEETING = "week int, meeting_date date, artist string, album string, year int, spotify_album_id string"
# A meeting entered in the app has no OneNote page; its page_number is this plus the week.
APP_PAGE_NUMBERS = 100_000


@dp.temporary_view(comment="Meeting pages from the newest export, parsed, and meetings entered in the app.")
def meeting_pages():
    bronze = spark.read.table("bronze_onenote_pages")  # noqa: F821
    newest = bronze.agg(F.max("source_file_modified_at").alias("source_file_modified_at"))
    overrides = spark.createDataFrame(  # noqa: F821
        [(int(r["week"]), r["album"], r["artist"]) for r in LISTEN_OVERRIDES],
        "week int, album string, artist_override string",
    )
    pages = (
        bronze.join(newest, "source_file_modified_at")
        .select("page_number", parse_meeting_page_udf("title", "created", "html").alias("page"))
        .where("page IS NOT NULL")
        .select("page_number", "page.*")
        .join(overrides, ["week", "album"], "left")
        .withColumn("artist", F.coalesce("artist_override", "artist"))
        .withColumn(  # an override supplies the missing artist
            "review_reasons",
            F.when(
                F.col("artist_override").isNotNull(),
                F.filter("review_reasons", lambda r: r != "no artist in title"),
            ).otherwise(F.col("review_reasons")),
        )
        .drop("artist_override")
        .withColumn("app_date", F.lit(None).cast("date"))
        .withColumn("spotify_album_id", F.lit(None).cast("string"))
    )
    entered = _entered_meetings().join(pages.select("week"), "week", "left_anti")  # a page wins
    weeks_with_files = spark.read.table("rating_rows").select("week").distinct().withColumn("has_file", F.lit(True))  # noqa: F821
    return (
        pages.unionByName(entered)
        .join(weeks_with_files, "week", "left")
        .withColumn(  # the ratings came in a file instead
            "review_reasons",
            F.when(F.col("has_file"), F.filter("review_reasons", lambda r: r != "no ratings table")).otherwise(
                F.col("review_reasons")
            ),
        )
        .drop("has_file")
    )


def _entered_meetings():
    """Meetings entered in the app, shaped like a parsed page. The newest file for a week wins."""
    newest = Window.partitionBy("week").orderBy(F.desc("source_file_modified_at"))
    return (
        spark.read.table("bronze_entered_meetings")  # noqa: F821
        .select("source_file_modified_at", F.from_json("text", ENTERED_MEETING).alias("m"))
        .select("source_file_modified_at", "m.*")
        .withColumn("n", F.row_number().over(newest))
        .where("n = 1")
        .select(
            (F.lit(APP_PAGE_NUMBERS) + F.col("week")).alias("page_number"),
            "week",
            F.lit(None).cast("int").alias("list_number"),
            "year",
            "artist",
            "album",
            F.col("meeting_date").cast("timestamp").alias("created_at"),
            F.lit(None).cast("date").alias("body_date"),
            F.array().cast("array<int>").alias("tracks_played"),
            F.array().cast("array<string>").alias("spotify_track_ids"),
            F.array().cast(f"array<{RESPONSE}>").alias("responses"),
            F.array().cast("array<string>").alias("review_reasons"),
            F.col("meeting_date").alias("app_date"),
            "spotify_album_id",
        )
    )


@dp.temporary_view(comment="Rows from ratings tables saved as files, pasted from OneNote or written by the app.")
def rating_rows():
    """For each week, person and listen, the row from the newest file wins. A file's week comes
    from its name: ratings_tables/W153.tsv, or W160-20261115T190503120000.tsv from the app."""
    rows = (
        spark.read.table("bronze_pasted_ratings")  # noqa: F821
        .withColumn("week", F.regexp_extract("source_file", r"/W(\d{3})[^/]*\.tsv$", 1).cast("int"))
        .where("week IS NOT NULL")
        .select("week", "source_file_modified_at", F.explode(parse_pasted_table_udf("week", "text")).alias("r"))
    )
    newest = Window.partitionBy("week", _response_key(), "r.listen").orderBy(F.desc("source_file_modified_at"))
    return rows.withColumn("n", F.row_number().over(newest)).where("n = 1").select("week", "r")


def _response_key():
    """Who a response is from: the person, or the name as written if it matches no one."""
    return F.coalesce("r.person", "r.name_as_written")


@dp.temporary_view(comment="One row per person per listen, from every ratings table.")
def responses():
    """The page's rows, except where a file has a row for the same person and listen."""
    pages = spark.read.table("meeting_pages")  # noqa: F821
    from_files = spark.read.table("rating_rows")  # noqa: F821
    replaced = from_files.select("week", _response_key().alias("key"), F.col("r.listen").alias("listen"))
    from_pages = (
        pages.select("week", "page_number", F.explode("responses").alias("r"))
        .withColumn("key", _response_key())
        .withColumn("listen", F.col("r.listen"))
        .join(replaced, ["week", "key", "listen"], "left_anti")
        .select("week", "page_number", "r")
    )
    return from_pages.unionByName(from_files.join(pages.select("week", "page_number"), "week"))


@dp.materialized_view(comment="Album club members and guests, from the people seed in git.")
def silver_people():
    columns = ["person", "first_name", "last_initial", "role"]
    rows = [tuple(r[c] for c in columns) for r in PEOPLE]
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in columns))  # noqa: F821


@dp.materialized_view(comment="One row per meeting, with the meeting date and where it came from.")
@dp.expect("has_meeting_date", "meeting_date IS NOT NULL")
@dp.expect("dates_in_order", "review_reason IS NULL")
def silver_meetings():
    per_week = (
        spark.read.table("meeting_pages")  # noqa: F821
        .groupBy("week")
        .agg(
            F.to_date(F.min("created_at")).alias("created"),
            F.min("body_date").alias("body_date"),
            F.min("app_date").alias("app_date"),
            F.count("*").alias("page_count"),
        )
    )
    return (
        per_week.withColumn("all_weeks", F.lit(1))
        .groupBy("all_weeks")
        .applyInPandas(
            _assign_dates,
            "week int, created date, body_date date, app_date date, page_count long, meeting_date date, "
            "date_source string, review_reason string",
        )
    )


def _assign_dates(_key, weeks: pd.DataFrame) -> pd.DataFrame:
    """Meeting dates are assigned in week order across all meetings, so this runs as one group."""
    records = [
        {**r, **{k: None if pd.isna(r[k]) else r[k] for k in ("body_date", "app_date")}}
        for r in weeks.to_dict("records")
    ]
    dated = assign_meeting_dates(records, MEETING_DATE_OVERRIDES)
    return pd.DataFrame(dated)[
        ["week", "created", "body_date", "app_date", "page_count", "meeting_date", "date_source", "review_reason"]
    ]


@dp.materialized_view(comment="One row per thing listened to at a meeting: an album, or a playlist.")
def silver_listens():
    pages = spark.read.table("meeting_pages")  # noqa: F821
    albums = pages.select(
        "week",
        "page_number",
        F.lit("album").alias("listen"),
        "artist",
        "album",
        "year",
        F.col("list_number").alias("list_number_in_title"),
        "tracks_played",
        "spotify_track_ids",
        "spotify_album_id",
    )
    playlists = (
        spark.read.table("responses")  # noqa: F821
        .where("r.listen = 'playlist'")
        .groupBy("week")
        .agg(F.min("page_number").alias("page_number"))
        .select(
            "week",
            "page_number",
            F.lit("playlist").alias("listen"),
            F.lit(None).cast("string").alias("artist"),
            F.lit(None).cast("string").alias("album"),
            F.lit(None).cast("int").alias("year"),
            F.lit(None).cast("int").alias("list_number_in_title"),
            F.array().cast("array<int>").alias("tracks_played"),
            F.array().cast("array<string>").alias("spotify_track_ids"),
            F.lit(None).cast("string").alias("spotify_album_id"),
        )
    )
    order = Window.partitionBy("week").orderBy(F.col("listen"), "page_number")  # albums first, then playlist
    return (
        albums.unionByName(playlists)
        .withColumn("listen_number", F.row_number().over(order))
        .withColumn("listen_id", F.format_string("W%03d-%d", "week", "listen_number"))
        .withColumn("selection_method", SELECTION_METHOD)
    )


def _with_listen_id(df):
    """Attach the listen_id. A playlist listen's page_number is the page its responses are on."""
    listens = spark.read.table("silver_listens").select("week", "page_number", "listen", "listen_id")  # noqa: F821
    return df.join(listens, ["week", "page_number", "listen"])


@dp.materialized_view(comment="Who was at each meeting. A blank rating means the person wasn't there.")
def silver_attendance():
    return (
        spark.read.table("responses")  # noqa: F821
        .groupBy("week", F.col("r.person").alias("person"))
        .agg(F.max(F.col("r.rating").isNotNull()).alias("attended"))
    )


@dp.materialized_view(comment="One row per person per listen they rated or picked favorites for.")
@dp.expect("rating_in_range", "rating IS NULL OR rating BETWEEN 0 AND 10")
@dp.expect("person_matched", "person IS NOT NULL")
def silver_ratings():
    responses = spark.read.table("responses").select("week", "page_number", "r.*")  # noqa: F821
    return (
        _with_listen_id(responses)
        .where("rating IS NOT NULL OR size(ranked) > 0 OR likes_rest OR size(excluded) > 0")
        .select(
            "listen_id",
            "week",
            "person",
            "rating",
            "rating_raw",
            "favorites_raw",
            "favorites_overridden",
            "likes_rest",
            "excluded",
        )
    )


@dp.materialized_view(comment="Ranked favorite tracks: rank 1 is the person's top favorite.")
def silver_favorites():
    responses = spark.read.table("responses").select("week", "page_number", "r.*")  # noqa: F821
    return (
        _with_listen_id(responses)
        .select("listen_id", "week", "person", F.posexplode("ranked").alias("rank0", "fav"))
        .select("listen_id", "week", "person", (F.col("rank0") + 1).alias("rank"), "fav.disc", "fav.track")
    )


@dp.materialized_view(comment="Everything the parser flagged for a person to check.")
def silver_review():
    pages = (
        spark.read.table("meeting_pages")  # noqa: F821
        .select("week", F.lit(None).cast("string").alias("person"), F.explode("review_reasons").alias("reason"))
        # a meeting_dates override settles a question about the date written on the page
        .where(~(F.col("reason").startswith("page says") & F.col("week").isin(list(MEETING_DATE_OVERRIDES))))
    )
    responses = spark.read.table("responses").select(  # noqa: F821
        "week", F.col("r.name_as_written").alias("person"), F.explode("r.review_reasons").alias("reason")
    )
    dates = (
        spark.read.table("silver_meetings")  # noqa: F821
        .where("review_reason IS NOT NULL")
        .select("week", F.lit(None).cast("string").alias("person"), F.col("review_reason").alias("reason"))
    )
    matches = spark.read.table("silver_album_matches").select(  # noqa: F821
        "week", F.lit(None).cast("string").alias("person"), F.explode("review_reasons").alias("reason")
    )
    # Track numbers the chosen edition doesn't have (a listen with no edition is flagged once, above).
    listens_with_tracks = spark.read.table("silver_tracks").select("listen_id").distinct()  # noqa: F821
    missing_tracks = (
        spark.read.table("silver_track_picks")  # noqa: F821
        .where("position IS NULL")
        .join(listens_with_tracks, "listen_id", "left_semi")
        .groupBy("week", "person")
        .agg(F.array_join(F.collect_list(F.concat_ws("-", "disc", "track")), ", ").alias("tracks"))
        .select("week", "person", F.format_string("not on the Spotify edition: track %s", "tracks").alias("reason"))
    )
    return pages.unionByName(responses).unionByName(dates).unionByName(matches).unionByName(missing_tracks)
