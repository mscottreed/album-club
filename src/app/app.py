"""The ratings app: each member fixes their own rating, and the host enters a meeting and everyone's ratings.

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
from databricks.sdk.errors import NotFound

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # src/, for album_club

from album_club import seeds
from album_club.app_entries import (
    check_rows,
    file_name,
    meeting_file,
    members_by_email,
    ratings_file,
    spotify_album_id,
)

CATALOG, SCHEMA = os.environ["ALBUM_CLUB_CATALOG"], os.environ["ALBUM_CLUB_SCHEMA"]
TABLES = f"{CATALOG}.{SCHEMA}"
EXPORTS = f"/Volumes/{CATALOG}/{SCHEMA}/onenote_exports"
ADMIN = os.environ["ALBUM_CLUB_ADMIN"]
PEOPLE = seeds.people()
# In people.csv order, the same as the OneNote ratings table, so a copied column pastes straight in.
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
def all_meetings() -> list[dict]:
    """Every meeting, newest first."""
    return query(f"SELECT week, artist, album, meeting_date FROM {TABLES}.dim_album ORDER BY week DESC")


@st.cache_data(ttl=60)
def members() -> dict[str, str]:
    """Signed-in email -> person, from members.csv next to the exports. The emails aren't in git."""
    try:
        text = WorkspaceClient().files.download(f"{EXPORTS}/members.csv").contents.read().decode()
    except NotFound:
        return {}
    return members_by_email(text, PEOPLE)


def recorded_ratings(week: int) -> dict[str, tuple[str, str]]:
    """What's already recorded for a week: person -> (rating, favorites) as written."""
    rows = query(
        f"SELECT person, rating_raw, favorites_raw FROM {TABLES}.silver_ratings WHERE week = :week", {"week": week}
    )
    return {r["person"]: (r["rating_raw"] or "", r["favorites_raw"] or "") for r in rows}


def listen_count(week: int) -> int:
    """How many albums and playlists a week had. Most weeks had one."""
    rows = query(
        f"SELECT count(DISTINCT listen_id) AS n FROM {TABLES}.silver_ratings WHERE week = :week", {"week": week}
    )
    return rows[0]["n"]


def save(path: str, text: str) -> None:
    WorkspaceClient().files.upload(f"{EXPORTS}/{path}", io.BytesIO(text.encode()), overwrite=False)


def save_ratings(week: int, rows: list[tuple[str, str, str]], now: dt.datetime) -> None:
    """Save a ratings file and start the refresh that puts it on the dashboard."""
    save(file_name("ratings_tables", week, "tsv", now), ratings_file(rows))
    WorkspaceClient().jobs.run_now(job_id=int(os.environ["ALBUM_CLUB_REFRESH_JOB_ID"]))
    st.success(f"Saved W{week:03d} and started album_club_refresh. It will be on the dashboard in about 10 minutes.")


def show_problems(problems: dict[str, list[str]]) -> None:
    for name, reasons in problems.items():
        st.error(f"{name}: {'; '.join(reasons)}")


def meeting_label(m: dict) -> str:
    return f"W{m['week']:03d} {m['artist']} - {m['album']}"


def my_ratings(person: str) -> None:
    """A member's own rating and favorite songs. The file has one row, theirs, so it replaces only their rating."""
    meetings = all_meetings()
    if not meetings:
        st.info("There are no meetings yet.")
        return
    labels = [meeting_label(m) for m in meetings]
    # Typing filters the list: "kind" finds W159 Miles Davis - Kind of Blue.
    choice = st.selectbox("Meeting", labels, key="my_meeting", filter_mode="contains")
    week = meetings[labels.index(choice)]["week"]
    if listen_count(week) > 1:
        st.info(f"W{week:03d} had more than one album or a playlist, and the app is for one-album weeks.")
        return
    rating, favorites = recorded_ratings(week).get(person, ("", ""))
    # In a form, Enter in either box saves, the same as the Save button.
    with st.form(f"my_form_{week}"):
        rating = st.text_input("Rating", rating)
        favorites = st.text_input("Favorite songs", favorites)
        st.caption("Favorites like `3, 1, 7` or `2, ... (x5)`, the way the OneNote pages do.")
        submitted = st.form_submit_button("Save", type="primary")
    if submitted:
        rows = [(person, rating.strip(), favorites.strip())]
        problems = check_rows(week, rows, PEOPLE)
        if problems:
            show_problems(problems)
        else:
            save_ratings(week, rows, dt.datetime.now(dt.UTC))


def enter_meeting() -> None:
    """The host's page: a new meeting, or everyone's ratings for a recent one."""
    meetings = all_meetings()[:8]
    latest = meetings[0]["week"] if meetings else 0
    choices = [f"New meeting (W{latest + 1:03d})"] + [meeting_label(m) for m in meetings]
    choice = st.radio("Meeting", choices, key="host_meeting")
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

    if st.button("Save", type="primary", key="host_save"):
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
            show_problems(problems)
        else:
            now = dt.datetime.now(dt.UTC)
            if new:
                save(
                    file_name("meetings", week, "json", now),
                    meeting_file(week, meeting_date, artist, album, year, album_id),
                )
            save_ratings(week, rows, now)


st.set_page_config(page_title="Album club ratings")
st.title("Album club ratings")

email = st.context.headers.get("X-Forwarded-Email", "").lower()
person = members().get(email)
host = email == ADMIN.lower()

if person and host:
    mine, meeting = st.tabs(["My ratings", "Enter a meeting"])
    with mine:
        my_ratings(person)
    with meeting:
        enter_meeting()
elif host:
    enter_meeting()
elif person:
    my_ratings(person)
else:
    st.info("The app doesn't know who you are yet. Ask the host to add your email.")
