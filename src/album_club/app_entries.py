"""The files the ratings app writes: a new meeting, and a change record for each save, with who saved
it, when, and each rating's value before and after. The pipeline reads a change the same way as a
ratings table copied out of OneNote.

The rules are in docs/data-decisions.md.
"""

import csv
import datetime as dt
import io
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


def clean(cell: str) -> str:
    """A cell on one line. A return or tab inside it, like one pasted along with a rating, would start a
    new row or cell in a ratings table, so any run of whitespace becomes one space."""
    return " ".join(cell.split())


def ratings_file(rows: list[tuple[str, str, str]]) -> str:
    """Rows of (name, rating, favorites) as a tab-separated ratings table, header first."""
    return "".join("\t".join(clean(cell) for cell in cells) + "\n" for cells in [HEADER, *rows])


def rating_changes(rows: list[tuple[str, str, str]], current: dict[str, tuple[str, str]]) -> list[dict]:
    """The rows that differ from what's recorded (name -> (rating, favorites)), each with its value
    before and after. A row that's the same as before isn't a change."""
    changes = []
    for name, rating, favorites in rows:
        before = tuple(clean(value) for value in current.get(name, ("", "")))
        after = (clean(rating), clean(favorites))
        if after != before:
            changes.append(
                {
                    "name": name,
                    "from": {"rating": before[0], "favorites": before[1]},
                    "to": {"rating": after[0], "favorites": after[1]},
                }
            )
    return changes


def change_file(week: int, saved_at: dt.datetime, saved_by: str, changes: list[dict]) -> str:
    """One save: the week, when (UTC), who saved it (a person, never an email) and the changes. The
    pipeline orders changes by saved_at, not by when the file was written, so a file copied from dev
    to prod later still lands in its place, and copying it again changes nothing."""
    return json.dumps({"week": week, "saved_at": saved_at.isoformat(), "saved_by": saved_by, "changes": changes})


def apply_changes(current: dict[str, tuple[str, str]], change_files: list[str]) -> dict[str, tuple[str, str]]:
    """What's recorded, with saved changes on top in the order they were saved. The app uses it so a
    save shows up before the refresh that puts it in silver, and so the next change starts from it."""
    current = dict(current)
    for record in sorted((json.loads(text) for text in change_files), key=lambda r: r["saved_at"]):
        for change in record["changes"]:
            current[change["name"]] = (change["to"]["rating"], change["to"]["favorites"])
    return current


def file_name(folder: str, week: int, extension: str, at: dt.datetime) -> str:
    """A new file for each save, named so the newest sorts last: rating_changes/W160-20261115T190503120000.json."""
    return f"{folder}/W{week:03d}-{at:%Y%m%dT%H%M%S%f}.{extension}"


def members_by_email(text: str, people: list[dict]) -> dict[str, str]:
    """Who each signed-in email is, from members.csv: email,person on each line, with or without that
    header. Emails match without case, and a line whose person isn't in the people seed (the header
    included) is left out, so it can't write a rating for anyone."""
    known = {p["person"] for p in people}
    return {
        row[0].strip().lower(): row[1].strip()
        for row in csv.reader(io.StringIO(text))
        if len(row) == 2 and row[1].strip() in known
    }


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
        responses = parse_pasted_table(week, ratings_file([(name, rating, favorites)]), people, {})
        if len(responses) > 1:  # "8.4 (A), 9.8 (P)" rates an album and a playlist
            problems[name] = ["the app is for one-album weeks, so leave out (A) and (P)"]
            continue
        if responses[0]["review_reasons"]:
            problems[name] = responses[0]["review_reasons"]
    return problems
