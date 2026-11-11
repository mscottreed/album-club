"""Enrichment: each listen matched to Spotify, MusicBrainz and the 1001 Albums list, using the API
responses in bronze_api_responses (written by the fetch_enrichment job task).

The matching is plain Python in album_club.enrichment (tested locally). The rules are in
docs/data-decisions.md.
"""

import json

import pandas as pd
from pyspark import cloudpickle
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import album_club
from album_club import seeds
from album_club.enrichment import crawl, match_listen

TRACK = (
    "struct<position:int, disc:int, track:int, title:string, artists:array<string>, duration_ms:long, "
    "spotify_track_id:string, on_original_album:boolean>"
)
MATCH = (
    "struct<spotify_album_id:string, spotify_album:string, spotify_artists:array<string>, "
    "spotify_release_date:string, spotify_match:string, musicbrainz_release_group_id:string, "
    "original_title:string, original_artist:string, original_year:int, original_track_count:int, "
    f"genres:array<string>, list_number:int, tracks:array<{TRACK}>, review_reasons:array<string>>"
)
# The workers that run UDFs can't import album_club, so ship its code with each function instead.
cloudpickle.register_pickle_by_value(album_club)

# Seeds are read here, on the driver, and travel to the workers inside the functions below.
ALBUM_OVERRIDES = seeds.album_overrides()
ALBUMS_1001 = seeds.albums_1001()
PLAYLIST_TRACKS = seeds.playlist_tracks()


@dp.temporary_view(comment="Each album listen matched to Spotify, MusicBrainz and the 1001 list, tracks included.")
def album_matches():
    listens = spark.read.table("silver_listens").where("listen = 'album'")  # noqa: F821
    # Every track number anyone wrote down, so the chosen edition can be checked against them.
    ratings = spark.read.table("silver_ratings")  # noqa: F821
    numbers = (
        spark.read.table("silver_favorites")  # noqa: F821
        .select("listen_id", "disc", "track")
        .unionByName(ratings.select("listen_id", F.explode("excluded").alias("t")).select("listen_id", "t.*"))
        .groupBy("listen_id")
        .agg(F.collect_set(F.struct("disc", "track")).alias("favorites"))
    )
    rows = listens.join(numbers, "listen_id", "left").select(
        F.lit("listen").alias("kind"),
        F.col("listen_id").alias("key"),
        F.to_json(F.struct("week", "artist", "album", "year", "spotify_track_ids", "favorites")).alias("value"),
    )
    responses = spark.read.table("bronze_api_responses").where("status = 200")  # noqa: F821
    rows = rows.unionByName(
        responses.select(
            F.lit("response").alias("kind"),
            F.concat_ws(" ", "source", "request").alias("key"),
            F.col("body").alias("value"),
        )
    )
    return (
        rows.withColumn("everything", F.lit(1))
        .groupBy("everything")
        .applyInPandas(_match, "listen_id string, match string")
        .select("listen_id", F.from_json("match", MATCH).alias("m"))
        .join(listens.select("listen_id", "week"), "listen_id")
        .select("listen_id", "week", "m.*")
    )


def _match(_key, rows: pd.DataFrame) -> pd.DataFrame:
    """Matching needs every cached response at hand, so it runs as one group."""
    bodies = dict(rows.loc[rows["kind"] == "response", ["key", "value"]].itertuples(index=False))
    parsed = {}

    def get(source, request):
        key = f"{source} {request}"
        if key in bodies and key not in parsed:
            parsed[key] = json.loads(bodies[key])
        return parsed.get(key)

    results = []
    for listen_id, value in rows.loc[rows["kind"] == "listen", ["key", "value"]].itertuples(index=False):
        listen = json.loads(value)
        favorites = [(f.get("disc"), f["track"]) for f in listen.get("favorites") or []]
        listen = {**listen, "spotify_track_ids": listen.get("spotify_track_ids") or []}
        override = ALBUM_OVERRIDES.get(listen["week"], {})
        match = match_listen(listen, crawl(listen, override, get), favorites, override, ALBUMS_1001)
        results.append((listen_id, json.dumps(match)))
    return pd.DataFrame(results, columns=["listen_id", "match"])


@dp.materialized_view(
    comment="One row per album listen: its Spotify edition, MusicBrainz album, genres and 1001 entry."
)
@dp.expect("has_spotify_edition", "spotify_album_id IS NOT NULL")
@dp.expect("has_musicbrainz_album", "musicbrainz_release_group_id IS NOT NULL")
def silver_album_matches():
    return spark.read.table("album_matches").drop("tracks")  # noqa: F821


@dp.materialized_view(comment="The tracks of each listen: the album's Spotify edition, or the playlist.")
def silver_tracks():
    albums = (
        spark.read.table("album_matches")  # noqa: F821
        .select("listen_id", F.explode("tracks").alias("t"))
        .select("listen_id", "t.*")
    )
    playlist_listens = spark.read.table("silver_listens").where("listen = 'playlist'").select("listen_id", "week")  # noqa: F821
    seed = spark.createDataFrame(  # noqa: F821
        [(int(r["week"]), int(r["position"]), r["title"], r["artist"], r["album"]) for r in PLAYLIST_TRACKS],
        "week int, position int, title string, artist string, from_album string",
    )
    playlists = playlist_listens.join(seed, "week").select(
        "listen_id",
        "position",
        F.lit(None).cast("int").alias("disc"),
        F.col("position").alias("track"),
        "title",
        F.array("artist").alias("artists"),
        F.lit(None).cast("long").alias("duration_ms"),
        F.lit(None).cast("string").alias("spotify_track_id"),
        F.lit(None).cast("boolean").alias("on_original_album"),
    )
    return albums.unionByName(playlists)


@dp.materialized_view(comment="Each person's opinion of each track they mentioned: ranked, liked, or not liked.")
@dp.expect("track_on_edition", "position IS NOT NULL")
def silver_track_picks():
    tracks = spark.read.table("silver_tracks")  # noqa: F821
    ratings = spark.read.table("silver_ratings")  # noqa: F821
    ranked = spark.read.table("silver_favorites").select(  # noqa: F821
        "listen_id", "week", "person", "disc", "track", "rank", F.lit(True).alias("liked")
    )
    excluded = ratings.select("listen_id", "week", "person", F.explode("excluded").alias("t")).select(
        "listen_id",
        "week",
        "person",
        "t.disc",
        "t.track",
        F.lit(None).cast("int").alias("rank"),
        F.lit(False).alias("liked"),
    )
    mentioned = _with_track(ranked.unionByName(excluded), tracks)
    # "likes the rest": every track on the listen that the person didn't rank or exclude
    rest = (
        ratings.where("likes_rest")
        .select("listen_id", "week", "person")
        .join(tracks.select("listen_id", "position", "disc", "track", "title", "on_original_album"), "listen_id")
        .join(mentioned.select("listen_id", "person", "position"), ["listen_id", "person", "position"], "left_anti")
        .select(
            "listen_id",
            "week",
            "person",
            "disc",
            "track",
            "position",
            "title",
            "on_original_album",
            F.lit(None).cast("int").alias("rank"),
            F.lit(True).alias("liked"),
        )
    )
    return mentioned.unionByName(rest)


def _with_track(picks, tracks):
    """Look up each track number as written. Without a disc, the number counts through the whole listen."""
    t = tracks.select("listen_id", "position", "disc", "track", "title", "on_original_album").alias("t")
    p = picks.alias("p")
    on_track = (F.col("p.listen_id") == F.col("t.listen_id")) & F.when(
        F.col("p.disc").isNull(), F.col("p.track") == F.col("t.position")
    ).otherwise((F.col("p.disc") == F.col("t.disc")) & (F.col("p.track") == F.col("t.track")))
    return p.join(t, on_track, "left").select(
        "p.listen_id",
        "p.week",
        "p.person",
        "p.disc",
        "p.track",
        "t.position",
        "t.title",
        "t.on_original_album",
        "p.rank",
        "p.liked",
    )
