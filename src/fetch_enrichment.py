"""Fetch the Spotify and MusicBrainz responses enrichment needs, as JSON-lines files in a volume.

This runs as a job task between two pipeline updates: the first builds silver_listens, the second
loads these files into bronze_api_responses with Auto Loader and matches each listen using only
those responses. Each response is fetched once, so later runs only call the APIs for listens they
haven't seen.
"""

import argparse
import datetime as dt
import json
import pathlib

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from album_club import apis, seeds
from album_club.enrichment import crawl

KEPT_STATUSES = (200, 404)  # anything else might work next time, so it isn't kept


def main():
    parser = argparse.ArgumentParser()
    for arg in ("--catalog", "--schema", "--responses-path", "--secret-scope"):
        parser.add_argument(arg, required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    responses = pathlib.Path(args.responses_path)
    cache = {}
    for file in sorted(responses.glob("*.jsonl")):
        for line in file.open(encoding="utf-8"):
            r = json.loads(line)
            cache[(r["source"], r["request"])] = (r["status"], r["body"])
    client = apis.Client(
        dbutils.secrets.get(args.secret_scope, "spotify-client-id"),
        dbutils.secrets.get(args.secret_scope, "spotify-client-secret"),
    )
    new = []

    def get(source: str, request: str) -> dict | None:
        if (source, request) not in cache:
            status, body = client.get(source, request)
            cache[(source, request)] = (status, body)
            if status in KEPT_STATUSES:
                fetched_at = dt.datetime.now(dt.UTC).isoformat()
                new.append(
                    {"source": source, "request": request, "status": status, "body": body, "fetched_at": fetched_at}
                )
        status, body = cache[(source, request)]
        return json.loads(body) if status == 200 else None

    def save():
        if new:
            name = f"responses-{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%S%f}.jsonl"
            with (responses / name).open("w", encoding="utf-8") as f:
                f.writelines(json.dumps(r) + "\n" for r in new)
            print(f"wrote {len(new)} responses to {name}")
            new.clear()

    overrides = seeds.album_overrides()
    listens = spark.table(f"{args.catalog}.{args.schema}.silver_listens").where("listen = 'album'").orderBy("week")
    for listen in listens.collect():
        crawl(listen.asDict(), overrides.get(listen.week, {}), get)
        if len(new) >= 100:
            save()
    save()


if __name__ == "__main__":
    main()
