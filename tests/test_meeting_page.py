import datetime as dt

from album_club.meeting_dates import assign_meeting_dates, next_sunday
from album_club.meeting_page import (
    body_date,
    parse_meeting_page,
    parse_pasted_table,
    parse_title,
    ratings_table,
    tracks_played,
)
from album_club.people import match_person

PEOPLE = [
    {"person": "Alex B", "first_name": "Alex", "last_initial": "B"},
    {"person": "Sam C", "first_name": "Sam", "last_initial": "C"},
    {"person": "Sam D", "first_name": "Sam", "last_initial": "D"},
]


def table(*rows: tuple[str, ...]) -> str:
    cells = "".join("<tr>" + "".join(f"<td><p>{c}</p></td>" for c in row) + "</tr>" for row in rows)
    return f"<table>{cells}</table>"


def page(body: str) -> str:
    return f'<div style="direction:ltr;border-width:100%"><div><div>Title</div><div>Date</div></div>{body}</div>'


# --- titles


def test_parse_early_title():
    assert parse_title("W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955)") == {
        "week": 1,
        "list_number": 1,
        "year": 1955,
        "artist": "Frank Sinatra",
        "album": "In the Wee Small Hours",
    }


def test_parse_early_title_with_missing_space():
    assert parse_title("W006 - #101- Velvet Underground - White Light/White Heat (1968)")["artist"] == (
        "Velvet Underground"
    )


def test_parse_later_title():
    assert parse_title("W159 - 1959 - Miles Davis - Kind of Blue") == {
        "week": 159,
        "list_number": None,
        "year": 1959,
        "artist": "Miles Davis",
        "album": "Kind of Blue",
    }


def test_compilation_title_has_no_artist():
    assert parse_title("W129 - 1963 - A Christmas Gift For You")["artist"] is None


def test_w165_is_w156():
    assert parse_title("W165 - 2001 - Some Artist - Some Album")["week"] == 156


def test_planning_pages_are_not_meetings():
    assert parse_title("W17 - 1987 (602)") is None
    assert parse_title("Autocratic Cycle 1") is None


# --- the ratings table


def test_ratings_table_is_the_one_with_name_and_rating_columns():
    html = page(table(("No.", "Title"), ("1", "Song")) + table(("Name", "Album Rating"), ("Alex", "7")))
    assert ratings_table(html) == [["Name", "Album Rating"], ["Alex", "7"]]


def test_ratings_table_from_image_alt_text():
    alt = "Name\r\nAlbum Rating\r\nFavorite songs\r\nAlex\r\n7\r\n2, 1\r\nSam C\r\n\r\n13\r\n\r\n\r\n\r\nLiner notes"
    html = page(f'<img src="x.png" alt="{alt}">')
    assert ratings_table(html) == [
        ["Name", "Album Rating", "Favorite songs"],
        ["Alex", "7", "2, 1"],
        ["Sam C", "", "13"],
    ]


def test_tracks_played_line_after_the_table():
    html = page(table(("Name", "Album Rating"), ("Alex", "7")) + "<p> </p><p>1, 4, 7, 12</p><p>1. Song</p>")
    assert tracks_played(html) == [1, 4, 7, 12]


def test_no_tracks_played_line():
    html = page(table(("Name", "Album Rating"), ("Alex", "7")) + "<p>1. Song</p>")
    assert tracks_played(html) == []


# --- dates


def test_body_date_right_before_the_table():
    created = dt.datetime(2024, 6, 28, 9, 0)
    html = page("<p>Sunday June 30th, 2024</p>" + table(("Name", "Album Rating"), ("Alex", "7")))
    assert body_date(html, created) == (dt.date(2024, 6, 30), None)


def test_body_date_ignores_dates_elsewhere_on_the_page():
    created = dt.datetime(2024, 6, 28, 9, 0)
    html = page(table(("Name", "Album Rating"), ("Alex", "7")) + "<p>Released: July 1, 2024</p>")
    assert body_date(html, created) == (None, None)


def test_body_date_flags_a_weekday_that_does_not_match():
    created = dt.datetime(2025, 7, 26, 9, 0)
    html = page("<p>Sunday August 2nd, 2025</p>" + table(("Name", "Album Rating"), ("Alex", "7")))
    date, review = body_date(html, created)
    assert date == dt.date(2025, 8, 2)
    assert "Saturday" in review


def test_next_sunday_is_strictly_after():
    assert next_sunday(dt.date(2024, 6, 28)) == dt.date(2024, 6, 30)  # Friday
    assert next_sunday(dt.date(2024, 6, 30)) == dt.date(2024, 7, 7)  # Sunday


def test_meeting_dates_follow_week_order():
    meetings = [
        {"week": 1, "created": dt.date(2022, 1, 30), "body_date": None},
        {"week": 2, "created": dt.date(2022, 1, 30), "body_date": None},  # created in the same batch
        {"week": 3, "created": dt.date(2022, 2, 1), "body_date": dt.date(2022, 2, 19)},
        {"week": 4, "created": dt.date(2022, 1, 30), "body_date": None},
    ]
    dated = assign_meeting_dates(meetings, overrides={})
    assert [(m["meeting_date"], m["date_source"]) for m in dated] == [
        (dt.date(2022, 2, 6), "sunday_rule"),
        (dt.date(2022, 2, 13), "sunday_rule"),
        (dt.date(2022, 2, 19), "page"),
        (dt.date(2022, 2, 20), "sunday_rule"),
    ]


def test_meeting_date_override_wins_and_out_of_order_dates_are_flagged():
    meetings = [
        {"week": 1, "created": dt.date(2022, 1, 30), "body_date": None},
        {"week": 2, "created": dt.date(2022, 1, 30), "body_date": None},
    ]
    dated = assign_meeting_dates(meetings, overrides={2: dt.date(2022, 2, 1)})
    assert dated[1]["date_source"] == "override"
    assert dated[1]["review_reason"] == "not after the previous meeting (2022-02-06)"


def test_a_date_entered_in_the_app_is_used_as_is():
    meetings = [{"week": 160, "created": dt.date(2026, 11, 18), "body_date": None, "app_date": dt.date(2026, 11, 18)}]
    (dated,) = assign_meeting_dates(meetings, overrides={})
    assert (dated["meeting_date"], dated["date_source"]) == (dt.date(2026, 11, 18), "app")


# --- people


def test_match_person():
    assert match_person("Alex", PEOPLE) == "Alex B"
    assert match_person("Alex Brown", PEOPLE) == "Alex B"
    assert match_person("Sam Cole", PEOPLE) == "Sam C"
    assert match_person("Sam", PEOPLE) is None  # two Sams
    assert match_person("Pat", PEOPLE) is None


# --- a whole page


def test_parse_meeting_page_with_album_and_playlist():
    html = page(
        table(
            ("Name", "Album Rating", "Favorite songs (Album)", "Favorite Songs (Playlist)"),
            ("Alex Brown", "8.4 (A), 9.8 (P)", "3, 1", "all"),
            ("Sam Cole", "6.5 (P)", "", "2, ..."),
            ("Sam Dunn", "", "", ""),
        )
    )
    parsed = parse_meeting_page(
        "W122 - 1994 - Some Band - Some Album",
        "Saturday, October 11, 2025 7:21 AM",
        html,
        PEOPLE,
        favorites_overrides={(122, "Sam C", "playlist"): "2, 4"},
    )
    by_key = {(r["person"], r["listen"]): r for r in parsed["responses"]}
    assert by_key[("Alex B", "album")]["rating"] == 8.4
    assert [f["track"] for f in by_key[("Alex B", "album")]["ranked"]] == [3, 1]
    assert by_key[("Alex B", "playlist")]["likes_rest"] is True
    assert by_key[("Sam C", "album")]["rating"] is None
    assert [f["track"] for f in by_key[("Sam C", "playlist")]["ranked"]] == [2, 4]
    assert by_key[("Sam C", "playlist")]["favorites_overridden"] is True
    assert by_key[("Sam D", "album")]["rating"] is None  # listed, but wasn't there


def test_one_favorites_column_goes_with_the_listen_they_rated():
    html = page(
        table(
            ("Name", "Album Rating", "Playlist Rating", "Favorite songs"),
            ("Alex", "7.8", "", "3, 4"),
            ("Sam Cole", "", "6.75", "9, 4"),
        )
    )
    parsed = parse_meeting_page("W107 - 1992 - A - B", "Sunday, May 4, 2025 1:05 PM", html, PEOPLE, {})
    by_key = {(r["person"], r["listen"]): r for r in parsed["responses"]}
    assert [f["track"] for f in by_key[("Alex B", "album")]["ranked"]] == [3, 4]
    assert [f["track"] for f in by_key[("Sam C", "playlist")]["ranked"]] == [9, 4]
    assert by_key[("Sam C", "album")]["ranked"] == []


def test_unlabeled_columns_after_favorites_are_extra_discs():
    html = page(table(("Name", "Album Rating", "Favorite songs", "", ""), ("Alex", "6.5", "1, 5", "7", "")))
    parsed = parse_meeting_page("W010 - #901 - A - B (1999)", "Sunday, January 30, 2022 1:21 PM", html, PEOPLE, {})
    assert parsed["responses"][0]["ranked"] == [
        {"disc": 1, "track": 1},
        {"disc": 1, "track": 5},
        {"disc": 2, "track": 7},
    ]


def test_a_pasted_table_is_read_like_the_page_table():
    pasted = "Name\tAlbum Rating\tFavorite songs\nAlex Brown\t6.7\t4, 9\nSam Cole\t\t\n"
    responses = parse_pasted_table(153, pasted, PEOPLE, {})
    assert [(r["person"], r["rating"], r["ranked"]) for r in responses] == [
        ("Alex B", 6.7, [{"disc": None, "track": 4}, {"disc": None, "track": 9}]),
        ("Sam C", None, []),
    ]
