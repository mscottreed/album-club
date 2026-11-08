"""Parse one OneNote meeting page (title, created date, page HTML) into structured data.

The rules are in docs/data-decisions.md.
"""

import datetime as dt
import re

from bs4 import BeautifulSoup

from album_club.people import match_person
from album_club.shorthand import parse_favorites, parse_rating

# Early titles carry the album's 1001 Albums number and end with the year:
#   W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955)
# later titles lead with the year:
#   W159 - 1959 - Miles Davis - Kind of Blue
EARLY_TITLE = re.compile(r"W(\d{3}) - #(\d+)\s*-\s*(.+?) - (.+) \((\d{4})\)")
LATER_TITLE = re.compile(r"W(\d{3}) - (\d{4}) - (.+)")
WEEK_CORRECTIONS = {165: 156}  # W165 was created between W155 and W157


def parse_title(title: str) -> dict | None:
    """Return week, list_number, year, artist and album, or None if it isn't a meeting page."""
    if m := EARLY_TITLE.fullmatch(title):
        week, list_number, artist, album, year = m.groups()
        list_number = int(list_number)
    elif m := LATER_TITLE.fullmatch(title):
        week, year, rest = m.groups()
        list_number = None
        artist, _, album = rest.rpartition(" - ")  # a compilation title has no artist part
    else:
        return None
    week = WEEK_CORRECTIONS.get(int(week), int(week))
    return {"week": week, "list_number": list_number, "year": int(year), "artist": artist or None, "album": album}


def parse_created(created: str) -> dt.datetime:
    """OneNote's created text, e.g. "Sunday, November 21, 2021 11:08 AM"."""
    return dt.datetime.strptime(created, "%A, %B %d, %Y %I:%M %p")


def ratings_table(html: str) -> list[list[str]] | None:
    """The ratings table as rows of cell text, header first, or None if the page has none.

    It's the table whose header has a Name column and a column containing "Rating". On a few
    pages the export turned the content into images, and the table survives only in an image's
    alt text, one cell per line.
    """
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        rows = [[_text(td) for td in tr.find_all(["td", "th"], recursive=False)] for tr in table.find_all("tr")]
        if rows and _is_ratings_header(rows[0]):
            return rows
    for img in soup.find_all("img"):
        lines = [" ".join(line.split()) for line in (img.get("alt") or "").splitlines()]
        width = next((i for i, line in enumerate(lines) if not _is_header_cell(line)), 0)
        if width and _is_ratings_header(lines[:width]):
            rows = [lines[:width]]
            for i in range(width, len(lines) - width + 1, width):
                if not lines[i]:
                    break
                rows.append(lines[i : i + width])
            return rows
    return None


def tracks_played(html: str) -> list[int]:
    """Track numbers on the line right after the ratings table (W133 played only some tracks)."""
    soup = BeautifulSoup(html, "html.parser")
    table = next((t for t in soup.find_all("table") if _is_ratings_header(_first_row(t))), None)
    if table is None:
        return []
    inside = {id(s) for s in table.find_all(string=True)}
    after = (s.strip() for s in table.find_all_next(string=True) if id(s) not in inside)
    first = next((s for s in after if s), "")
    return [int(n) for n in first.split(",")] if re.fullmatch(r"\d+(\s*,\s*\d+)+", first) else []


# The meeting date, when it's written, sits right before the ratings table: "Sunday June 30th, 2024".
TABLE_DATE = re.compile(
    r"(?:(Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,? )?"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? (\d{1,2})(?:st|nd|rd|th)?(?:,? (\d{4}))?\s+Name\b"
)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def body_date(html: str, created: dt.datetime) -> tuple[dt.date | None, str | None]:
    """The date written right before the ratings table, if it's within 3 weeks after the page was
    created. Also returns a review reason when the written weekday doesn't match the date."""
    text = " ".join(BeautifulSoup(html, "html.parser").get_text(" ").split())
    m = TABLE_DATE.search(text)
    if not m:
        return None, None
    weekday, month, day, year = m.groups()
    start = created.date()
    for y in [int(year)] if year else [start.year, start.year + 1]:
        try:
            d = dt.date(y, MONTHS.index(month) + 1, int(day))
        except ValueError:
            continue
        if start <= d <= start + dt.timedelta(days=21):
            if weekday and WEEKDAYS.index(weekday) != d.weekday():
                return d, f"page says {m.group(0)[:-4].strip()!r}, but {d} is a {d:%A}"
            return d, None
    return None, None


def parse_meeting_page(
    title: str, created: str, html: str, people: list[dict], favorites_overrides: dict
) -> dict | None:
    """Everything post 4 needs from one meeting page, or None if it isn't a meeting page.

    people and favorites_overrides come from album_club.seeds.
    """
    meeting = parse_title(title)
    if meeting is None:
        return None
    created_at = parse_created(created)
    table = ratings_table(html)
    date, date_review = body_date(html, created_at)
    review = [] if meeting["artist"] else ["no artist in title"]
    if table is None:
        review.append("no ratings table")
    if date_review:
        review.append(date_review)
    return {
        **meeting,
        "created_at": created_at,
        "body_date": date,
        "tracks_played": tracks_played(html),
        "responses": _responses(meeting["week"], table, people, favorites_overrides) if table else [],
        "review_reasons": review,
    }


def _responses(week: int, table: list[list[str]], people: list[dict], overrides: dict) -> list[dict]:
    """One response per person per listen (album or playlist), including blank ones for attendance."""
    header = [h.lower() for h in table[0]]
    rating_cols = [i for i, h in enumerate(header) if "rating" in h]
    favorite_cols = [i for i, h in enumerate(header) if "favorite" in h]
    # W010's triple album has an unlabeled column per extra volume after "Favorite songs".
    disc_cols = [i for i in range(favorite_cols[-1] + 1, len(header)) if not header[i]] if favorite_cols else []
    has_playlist = any("playlist" in header[i] for i in rating_cols + favorite_cols) or any(
        "(p)" in row[i].lower() for row in table[1:] for i in rating_cols if i < len(row)
    )

    responses = []
    for row in table[1:]:
        row = row + [""] * (len(header) - len(row))
        name = row[0]
        if not name:
            continue
        ratings = {}
        for i in rating_cols:
            default = "playlist" if "playlist" in header[i] else "album"
            for value, listen in parse_rating(row[i]):
                ratings[listen or default] = (value, row[i])
        favorites = {}
        for i in favorite_cols:
            listen = "playlist" if "playlist" in header[i] else "album"
            text = row[i]
            if disc_cols and listen == "album":
                text = " ".join(f"Disc {n}: {row[c]}" for n, c in enumerate([i, *disc_cols], start=1))
            favorites[listen] = text
        if len(favorite_cols) == 1 and has_playlist and set(ratings) == {"playlist"}:
            favorites = {"playlist": favorites["album"]}  # W107: one favorites column, for what they rated

        person = match_person(name, people)
        for listen in ["album", "playlist"] if has_playlist else ["album"]:
            rating, rating_raw = ratings.get(listen, (None, ""))
            favorites_raw = favorites.get(listen, "")
            responses.append(_response(week, name, person, listen, rating, rating_raw, favorites_raw, overrides))
    return responses


def _response(week, name, person, listen, rating, rating_raw, favorites_raw, overrides) -> dict:
    override = overrides.get((week, person, listen), overrides.get((week, person, "")) if listen == "album" else None)
    fav = parse_favorites(override if override is not None else favorites_raw)
    review = list(fav.review_reasons)
    if person is None:
        review.append(f"name {name!r} matches no one, or more than one person")
    return {
        "name_as_written": name,
        "person": person,
        "listen": listen,
        "rating": rating,
        "rating_raw": rating_raw,
        "favorites_raw": favorites_raw,
        "favorites_overridden": override is not None,
        "ranked": [{"disc": d, "track": t} for d, t in fav.ranked],
        "likes_rest": fav.likes_rest,
        "excluded": [{"disc": d, "track": t} for d, t in fav.excluded],
        "review_reasons": review,
    }


def _is_header_cell(text: str) -> bool:
    t = text.lower()
    return t.startswith("name") or "rating" in t or "favorite" in t


def _is_ratings_header(cells: list[str]) -> bool:
    return bool(cells) and cells[0].lower().startswith("name") and any("rating" in c.lower() for c in cells)


def _first_row(table) -> list[str]:
    tr = table.find("tr")
    return [_text(td) for td in tr.find_all(["td", "th"], recursive=False)] if tr else []


def _text(element) -> str:
    return " ".join(element.get_text(" ").split())
