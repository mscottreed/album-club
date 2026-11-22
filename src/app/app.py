"""The ratings app: each member fixes their own rating, and the host enters a meeting and everyone's ratings.

Each save is a change record with who saved it, when, and each rating before and after
(album_club.app_entries). Then it starts album_club_refresh to put the change on the dashboard.
"""

import datetime as dt
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.jobs import RunLifeCycleState

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # src/, for album_club

from album_club import seeds
from album_club.app_entries import (
    apply_changes,
    change_file,
    check_rows,
    file_name,
    meeting_file,
    members_by_email,
    rating_changes,
    spotify_album_id,
)

CATALOG, SCHEMA = os.environ["ALBUM_CLUB_CATALOG"], os.environ["ALBUM_CLUB_SCHEMA"]
EXPORTS = f"/Volumes/{CATALOG}/{SCHEMA}/onenote_exports"
ADMIN = os.environ["ALBUM_CLUB_ADMIN"]
PEOPLE = seeds.people()
# In people.csv order, the same as the OneNote ratings table, so a copied column pastes straight in.
MEMBERS = [p["person"] for p in PEOPLE if p["role"] == "member"]

config = Config()
log = logging.getLogger(__name__)


def query(statement: str, params: dict | None = None) -> list[dict]:
    """Run a query on the SQL warehouse as the app's service principal."""
    with (
        sql.connect(
            server_hostname=config.host,
            http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
            credentials_provider=lambda: config.authenticate,
            # Queries name tables without a catalog and schema, so the SQL is never built from strings.
            catalog=CATALOG,
            schema=SCHEMA,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(statement, params or {})
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@st.cache_data(ttl=60)
def all_meetings() -> list[dict]:
    """Every meeting, newest first."""
    return query("SELECT week, artist, album, meeting_date FROM dim_album ORDER BY week DESC")


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
    rows = query("SELECT person, rating_raw, favorites_raw FROM silver_ratings WHERE week = :week", {"week": week})
    return {r["person"]: (r["rating_raw"] or "", r["favorites_raw"] or "") for r in rows}


def saved_changes(week: int) -> list[str]:
    """The change records saved for a week, including any the pipeline hasn't picked up yet."""
    try:
        entries = WorkspaceClient().files.list_directory_contents(f"{EXPORTS}/rating_changes")
        paths = [e.path for e in entries if e.name.startswith(f"W{week:03d}-")]
    except NotFound:  # nothing saved yet
        return []
    return [WorkspaceClient().files.download(path).contents.read().decode() for path in paths]


def current_ratings(week: int) -> dict[str, tuple[str, str]]:
    """What's recorded for a week, with every saved change on top: what the next save changes from."""
    return apply_changes(recorded_ratings(week), saved_changes(week))


def listen_count(week: int) -> int:
    """How many albums and playlists a week had. Most weeks had one."""
    rows = query("SELECT count(DISTINCT listen_id) AS n FROM silver_ratings WHERE week = :week", {"week": week})
    return rows[0]["n"]


def save(path: str, text: str) -> None:
    WorkspaceClient().files.upload(f"{EXPORTS}/{path}", io.BytesIO(text.encode()), overwrite=False)


def start_refresh(week: int) -> None:
    """Start the refresh that puts a save on the dashboard. A run that's queued hasn't read the
    volume yet, so when one is waiting it will pick this save up, and another run would only repeat it."""
    jobs = WorkspaceClient().jobs
    job_id = int(os.environ["ALBUM_CLUB_REFRESH_JOB_ID"])
    try:
        runs = jobs.list_runs(job_id=job_id, active_only=True)
        if any(r.state.life_cycle_state == RunLifeCycleState.QUEUED for r in runs):
            st.success(f"Saved W{week:03d}. A refresh is already waiting to start, and it will pick this up.")
            return
        jobs.run_now(job_id=job_id)
    except DatabricksError:
        log.exception("Couldn't start album_club_refresh")  # in the app's logs, for the host
        st.warning(f"Saved W{week:03d}, but the refresh didn't start. It will be on the dashboard after the next one.")
        return
    st.success(f"Saved W{week:03d} and started album_club_refresh. It will be on the dashboard in about 10 minutes.")


def changed_rows(changes: list[dict]) -> list[tuple[str, str, str]]:
    """The changes as (name, rating, favorites), the rows to check before saving."""
    return [(c["name"], c["to"]["rating"], c["to"]["favorites"]) for c in changes]


def show_problems(problems: dict[str, list[str]]) -> None:
    for name, reasons in problems.items():
        st.error(f"{name}: {'; '.join(reasons)}")


def meeting_label(m: dict) -> str:
    return f"W{m['week']:03d} {m['artist']} - {m['album']}"


def my_ratings(person: str) -> None:
    """A member's own rating and favorite songs. The change record names only them, so it changes only their rating."""
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
    current = current_ratings(week)
    rating, favorites = current.get(person, ("", ""))
    # In a form, Enter in either box saves, the same as the Save button.
    with st.form(f"my_form_{week}"):
        rating = st.text_input("Rating", rating)
        favorites = st.text_input("Favorite songs", favorites)
        st.caption("Favorites like `3, 1, 7` or `2, ... (x5)`, the way the OneNote pages do.")
        submitted = st.form_submit_button("Save", type="primary")
    if submitted:
        changes = rating_changes([(person, rating, favorites)], current)
        problems = check_rows(week, changed_rows(changes), PEOPLE)
        if problems:
            show_problems(problems)
        elif not changes:
            st.info("That's what's already recorded, so there's nothing to save.")
        else:
            now = dt.datetime.now(dt.UTC)
            save(file_name("rating_changes", week, "json", now), change_file(week, now, person, changes))
            start_refresh(week)


def enter_meeting(saved_by: str) -> None:
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
    else:
        week = meetings[choices.index(choice) - 1]["week"]
    recorded = current_ratings(week)

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
        changes = rating_changes(rows, recorded)
        problems = check_rows(week, changed_rows(changes), PEOPLE)
        if new:
            album_id = spotify_album_id(link)
            missing = [label for label, value in [("artist", artist), ("album", album), ("year", year)] if not value]
            if missing:
                problems["Meeting"] = [f"missing {', '.join(missing)}"]
            if album_id is None:
                problems.setdefault("Meeting", []).append("the Spotify link has to be an album link")
        if problems:
            show_problems(problems)
        elif not new and not changes:
            st.info("Nothing changed, so there's nothing to save.")
        else:
            now = dt.datetime.now(dt.UTC)
            if new:
                save(
                    file_name("meetings", week, "json", now),
                    meeting_file(week, meeting_date, artist, album, year, album_id),
                )
            if changes:
                save(file_name("rating_changes", week, "json", now), change_file(week, now, saved_by, changes))
            start_refresh(week)


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
        enter_meeting(person)
elif host:
    enter_meeting("host")
elif person:
    my_ratings(person)
else:
    st.info("The app doesn't know who you are yet. Ask the host to add your email.")
