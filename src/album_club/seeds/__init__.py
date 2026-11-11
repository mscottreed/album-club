"""Small reference files that live in git and ship with the package: people, overrides, the 1001 list.

Read them on the driver and pass the data to the parsing functions. Workers running UDFs can't
import album_club, so nothing that runs there should read these files.
"""

import csv
import datetime as dt
from importlib.resources import files


def read(name: str) -> list[dict]:
    """Rows of a seed CSV as dicts."""
    with files(__package__).joinpath(name).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def people() -> list[dict]:
    return read("people.csv")


def favorites_overrides() -> dict[tuple[int, str, str], str]:
    """(week, person, listen) -> corrected favorites text. A blank listen means the album."""
    return {(int(r["week"]), r["person"], r["listen"]): r["favorites"] for r in read("favorites_overrides.csv")}


def meeting_date_overrides() -> dict[int, dt.date]:
    """week -> meeting date, for meetings the rules date wrongly."""
    return {int(r["week"]): dt.date.fromisoformat(r["meeting_date"]) for r in read("meeting_dates.csv")}


def listen_overrides() -> list[dict]:
    """Corrections to what a page title says about an album, keyed by week and album title."""
    return read("listen_overrides.csv")


def album_overrides() -> dict[int, dict]:
    """week -> album name to search for, Spotify album ID, MusicBrainz release group ID and/or
    original year, for albums the matching gets wrong."""
    return {int(r["week"]): {k: v for k, v in r.items() if v and k != "week"} for r in read("album_overrides.csv")}


def playlist_tracks() -> list[dict]:
    """Each playlist listen's tracks in order (Spotify doesn't return a playlist's tracks without a user)."""
    return read("playlist_tracks.csv")


def albums_1001() -> list[dict]:
    """The 1001 Albums You Must Hear Before You Die list (2021 edition)."""
    return read("1001_albums.csv")
