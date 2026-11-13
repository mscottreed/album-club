"""The ratings app: the host enters a meeting and everyone's ratings.

It saves files the pipeline reads like a ratings table copied from OneNote (album_club.app_entries),
then starts album_club_refresh to put them on the dashboard.
"""

import datetime as dt
import io
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # src/, for album_club

from album_club import seeds
from album_club.app_entries import check_rows, file_name, meeting_file, ratings_file, spotify_album_id

CATALOG, SCHEMA = os.environ["ALBUM_CLUB_CATALOG"], os.environ["ALBUM_CLUB_SCHEMA"]
TABLES = f"{CATALOG}.{SCHEMA}"
EXPORTS = f"/Volumes/{CATALOG}/{SCHEMA}/onenote_exports"
ADMIN = os.environ["ALBUM_CLUB_ADMIN"]
PEOPLE = seeds.people()
MEMBERS = [p["person"] for p in PEOPLE if p["role"] == "member"]

config = Config()


def query(statement: str, params: dict | None = None) -> list[dict]:
    """Run a query on the SQL warehouse as the app's service principal."""
    with (
        sql.connect(
            server_hostname=config.host,
            http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
            credentials_provider=lambda: config.authenticate,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(statement, params or {})
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@st.cache_data(ttl=60)
def recent_meetings() -> list[dict]:
    return query(f"SELECT week, artist, album, meeting_date FROM {TABLES}.dim_album ORDER BY week DESC LIMIT 8")


def recorded_ratings(week: int) -> dict[str, tuple[str, str]]:
    """What's already recorded for a week: person -> (rating, favorites) as written."""
    rows = query(
        f"SELECT person, rating_raw, favorites_raw FROM {TABLES}.silver_ratings WHERE week = :week", {"week": week}
    )
    return {r["person"]: (r["rating_raw"] or "", r["favorites_raw"] or "") for r in rows}


def save(path: str, text: str) -> None:
    WorkspaceClient().files.upload(f"{EXPORTS}/{path}", io.BytesIO(text.encode()), overwrite=False)


st.set_page_config(page_title="Album club ratings")
st.title("Album club ratings")

email = st.context.headers.get("X-Forwarded-Email", "")
if email.lower() != ADMIN.lower():
    st.info("Entering a meeting is for the host.")
    st.stop()

meetings = recent_meetings()
latest = meetings[0]["week"] if meetings else 0
choices = [f"New meeting (W{latest + 1:03d})"] + [f"W{m['week']:03d} {m['artist']} - {m['album']}" for m in meetings]
choice = st.radio("Meeting", choices)
new = choice == choices[0]

if new:
    week = st.number_input("Week", min_value=latest + 1, value=latest + 1, step=1)
    meeting_date = st.date_input("Date", dt.date.today())
    artist = st.text_input("Artist")
    album = st.text_input("Album")
    year = st.number_input("Year", min_value=1900, max_value=dt.date.today().year, value=None, step=1)
    link = st.text_input("Spotify album link", placeholder="https://open.spotify.com/album/...")
    recorded = {}
else:
    week = meetings[choices.index(choice) - 1]["week"]
    recorded = recorded_ratings(week)

st.subheader("Ratings")
st.caption(
    "Write them the way the OneNote pages do: favorites like `3, 1, 7` or `2, ... (x5)`. Leave a row blank if they weren't there."
)
names = MEMBERS + [p for p in recorded if p not in MEMBERS]  # guests already recorded for this week
table = st.data_editor(
    pd.DataFrame([(n, *recorded.get(n, ("", ""))) for n in names], columns=["Name", "Rating", "Favorite songs"]),
    num_rows="dynamic",  # a guest can be added at the bottom
    hide_index=True,
    use_container_width=True,
)

if st.button("Save", type="primary"):
    rows = [
        (str(name).strip(), str(rating or "").strip(), str(favorites or "").strip())
        for name, rating, favorites in table.fillna("").itertuples(index=False)
        if str(name).strip()
    ]
    problems = check_rows(week, rows, PEOPLE)
    if new:
        album_id = spotify_album_id(link)
        missing = [label for label, value in [("artist", artist), ("album", album), ("year", year)] if not value]
        if missing:
            problems["Meeting"] = [f"missing {', '.join(missing)}"]
        if album_id is None:
            problems.setdefault("Meeting", []).append("the Spotify link has to be an album link")
    if problems:
        for name, reasons in problems.items():
            st.error(f"{name}: {'; '.join(reasons)}")
    else:
        now = dt.datetime.now(dt.UTC)
        if new:
            save(
                file_name("meetings", week, "json", now),
                meeting_file(week, meeting_date, artist, album, year, album_id),
            )
        save(file_name("ratings_tables", week, "tsv", now), ratings_file(rows))
        WorkspaceClient().jobs.run_now(job_id=int(os.environ["ALBUM_CLUB_REFRESH_JOB_ID"]))
        st.success(
            f"Saved W{week:03d} and started album_club_refresh. It will be on the dashboard in about 10 minutes."
        )
