import datetime as dt
import json

import pytest

from album_club.app_entries import check_rows, file_name, meeting_file, ratings_file, spotify_album_id

PEOPLE = [
    {"person": "Alex B", "first_name": "Alex", "last_initial": "B"},
    {"person": "Sam C", "first_name": "Sam", "last_initial": "C"},
]


@pytest.mark.parametrize(
    "link",
    [
        "https://open.spotify.com/album/1J8QW9qsMLx3staWaHpQmU",
        "https://open.spotify.com/intl-de/album/1J8QW9qsMLx3staWaHpQmU?si=abc123",
        "spotify:album:1J8QW9qsMLx3staWaHpQmU",
    ],
)
def test_spotify_album_id(link):
    assert spotify_album_id(link) == "1J8QW9qsMLx3staWaHpQmU"


@pytest.mark.parametrize("link", ["", "https://open.spotify.com/track/1J8QW9qsMLx3staWaHpQmU", "not a link"])
def test_spotify_album_id_rejects_other_links(link):
    assert spotify_album_id(link) is None


def test_meeting_file():
    text = meeting_file(160, dt.date(2026, 11, 15), "Carole King", "Tapestry", 1971, "1J8QW9qsMLx3staWaHpQmU")
    assert json.loads(text) == {
        "week": 160,
        "meeting_date": "2026-11-15",
        "artist": "Carole King",
        "album": "Tapestry",
        "year": 1971,
        "spotify_album_id": "1J8QW9qsMLx3staWaHpQmU",
    }


def test_ratings_file_is_a_pasted_table():
    text = ratings_file([("Alex B", "7.5", "3, 1"), ("Sam C", "", "")])
    assert text == "Name\tAlbum Rating\tFavorite songs\nAlex B\t7.5\t3, 1\nSam C\t\t\n"


def test_file_name_sorts_by_time():
    at = dt.datetime(2026, 11, 15, 19, 5, 3, 120000, tzinfo=dt.UTC)
    assert file_name("ratings_tables", 160, "tsv", at) == "ratings_tables/W160-20261115T190503120000.tsv"


def test_check_rows_accepts_what_the_pipeline_reads():
    assert check_rows(160, [("Alex B", "7.5", "3, 1, ... (x5)"), ("Sam C", "", "")], PEOPLE) == {}


def test_check_rows_reports_each_problem_by_name():
    problems = check_rows(160, [("Alex B", "seven", "3"), ("Pat Q", "6", "")], PEOPLE)
    assert set(problems) == {"Alex B", "Pat Q"}
    assert "rating" in problems["Alex B"][0]
    assert "matches no one" in problems["Pat Q"][0]


def test_check_rows_rejects_ratings_out_of_range():
    assert "between 0 and 10" in check_rows(160, [("Alex B", "11", "")], PEOPLE)["Alex B"][0]
