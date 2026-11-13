"""The files the ratings app writes: a new meeting, and ratings tables in the same tab-separated
form as a table copied out of OneNote, so the pipeline reads both the same way.

The rules are in docs/data-decisions.md.
"""

import datetime as dt
import json
import re

from album_club.meeting_page import parse_pasted_table
from album_club.shorthand import parse_rating

SPOTIFY_ALBUM_LINK = re.compile(r"(?:open\.spotify\.com/(?:intl-[a-z-]+/)?album/|spotify:album:)([A-Za-z0-9]{22})")
HEADER = ("Name", "Album Rating", "Favorite songs")


def spotify_album_id(link: str) -> str | None:
    """The album ID in a Spotify album link, or None if it isn't one."""
    m = SPOTIFY_ALBUM_LINK.search(link)
    return m.group(1) if m else None


def meeting_file(week: int, meeting_date: dt.date, artist: str, album: str, year: int, album_id: str) -> str:
    """A meeting with no OneNote page: what its page title would have said, plus the date and edition."""
    return json.dumps(
        {
            "week": week,
            "meeting_date": meeting_date.isoformat(),
            "artist": artist,
            "album": album,
            "year": year,
            "spotify_album_id": album_id,
        }
    )


def ratings_file(rows: list[tuple[str, str, str]]) -> str:
    """Rows of (name, rating, favorites) as a tab-separated ratings table, header first."""
    return "".join("\t".join(cells) + "\n" for cells in [HEADER, *rows])


def file_name(folder: str, week: int, extension: str, at: dt.datetime) -> str:
    """A new file for each save, named so the newest sorts last: ratings_tables/W160-20261115T190503120000.tsv."""
    return f"{folder}/W{week:03d}-{at:%Y%m%dT%H%M%S%f}.{extension}"


def check_rows(week: int, rows: list[tuple[str, str, str]], people: list[dict]) -> dict[str, list[str]]:
    """Problems with each row, by name, as the pipeline would see them. An empty dict means the
    rows are fine to save. A rating the parser can't read would stop the pipeline, so it's an error here."""
    problems = {}
    for name, rating, favorites in rows:
        try:
            values = [value for value, _ in parse_rating(rating)]
        except ValueError:
            problems[name] = [f"can't read the rating {rating!r}"]
            continue
        if any(not 0 <= value <= 10 for value in values):
            problems[name] = ["the rating has to be between 0 and 10"]
            continue
        (response,) = parse_pasted_table(week, ratings_file([(name, rating, favorites)]), people, {})
        if response["review_reasons"]:
            problems[name] = response["review_reasons"]
    return problems
