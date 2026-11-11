from album_club import apis
from album_club.enrichment import (
    choose_edition,
    clean_title,
    crawl,
    edition_tracks,
    match_1001,
    match_listen,
    names_match,
    pick_original_release,
    pick_release_group,
    similar,
)
from album_club.meeting_page import spotify_track_ids

LISTEN = {"week": 1, "artist": "Some Band", "album": "Some Album", "year": 1977, "spotify_track_ids": []}


def album(album_id, name, titles, artist="Some Band"):
    return {
        "id": album_id,
        "name": name,
        "artists": [{"name": artist}],
        "release_date": "1977-02-04",
        "all_tracks": [
            {
                "disc_number": 1,
                "track_number": i + 1,
                "name": t,
                "id": f"{album_id}-{i}",
                "artists": [],
                "duration_ms": 1,
            }
            for i, t in enumerate(titles)
        ],
    }


def crawled(*albums, linked=(), original_titles=()):
    original = {"media": [{"position": 1, "tracks": [{"title": t} for t in original_titles]}]}
    return {
        "linked_album_ids": list(linked),
        "albums": {a["id"]: a for a in albums},
        "release_group": None,
        "original": original if original_titles else None,
    }


# --- names


def test_similar_ignores_case_the_and_edition_suffixes():
    assert similar("The Velvet Underground", "Velvet Underground") == 1.0
    assert similar("In the Wee Small Hours", "In The Wee Small Hours (Remastered 1998)") == 1.0
    assert similar("Simon & Garfunkel", "Simon and Garfunkel") == 1.0


def test_names_match_needs_album_and_artist():
    assert names_match("Some Band", "Some Album", ["Some Band"], "Some Album - Deluxe Edition")
    assert not names_match("Some Band", "Some Album", ["Other Band"], "Some Album")
    assert names_match(None, "A Christmas Gift for You", ["Various Artists"], "A Christmas Gift For You")


def test_names_match_when_one_name_contains_the_other():
    assert names_match("James Brown & The Famous Flames", "Live at the Apollo", ["James Brown"], "Live at the Apollo")
    assert names_match("REM", "Document", ["R.E.M."], "Document")
    assert names_match("The Who", "Sell Out", ["The Who"], "The Who Sell Out")
    assert not names_match("INXS", "Kick", ["MC5"], "Kick Out the Jams")
    assert not names_match("Some Band", "Live", ["Some Band"], "Live at Leeds")  # one-word albums must be close


def test_names_with_different_numbers_are_different_albums():
    assert not names_match("Van Halen", "Van Halen", ["Van Halen"], "Van Halen II (Remastered)")
    assert not names_match("Ray Charles", "Ray Charles", ["Ray Charles"], "The Ray Charles Story Volume 2")
    assert names_match("Led Zeppelin", "Led Zeppelin IV", ["Led Zeppelin"], "Led Zeppelin IV (Remaster)")
    assert names_match("Tito Puente", "Dance Mania (Vol 1)", ["Tito Puente"], "Dance Mania Vol.1")
    assert names_match("Tito Puente", "Dance Mania (Vol 1)", ["Tito Puente"], "Dance Mania")  # in the suffix only


def test_a_self_titled_album_has_to_be_close_not_contained():
    assert not names_match("Ray Charles", "Ray Charles", ["Ray Charles"], "The Genius Of Ray Charles")
    assert not names_match("Ray Charles", "Ray Charles (aka: Hallelujah)", ["Ray Charles"], "Genius of Ray Charles")
    assert names_match("Weezer", "Weezer (Blue Album)", ["Weezer"], "Weezer")


def test_clean_title_drops_remaster_notes_only():
    assert clean_title("Dreams - 2004 Remaster") == "dreams"
    assert clean_title("Go Your Own Way (Remastered)") == "go your own way"
    assert clean_title("Dreams - Live") == "dreams live"


# --- MusicBrainz


def test_pick_release_group_prefers_the_studio_album():
    search = {
        "release-groups": [
            {
                "id": "comp",
                "title": "Some Album",
                "primary-type": "Album",
                "secondary-types": ["Compilation"],
                "artist-credit": [{"name": "Some Band"}],
            },
            {"id": "studio", "title": "Some Album", "primary-type": "Album", "artist-credit": [{"name": "Some Band"}]},
            {"id": "other", "title": "Other Album", "primary-type": "Album", "artist-credit": [{"name": "Some Band"}]},
        ]
    }
    assert pick_release_group(search, "Some Band", "Some Album") == "studio"
    assert pick_release_group({"release-groups": []}, "Some Band", "Some Album") is None


def test_pick_release_group_prefers_the_earliest_of_equal_matches():
    credit = [{"name": "Some Band"}]
    search = {
        "release-groups": [
            {
                "id": "reissue",
                "title": "Some Album",
                "primary-type": "Album",
                "first-release-date": "2023-09-15",
                "artist-credit": credit,
            },
            {
                "id": "original",
                "title": "Some Album",
                "primary-type": "Album",
                "first-release-date": "1971-08-14",
                "artist-credit": credit,
            },
        ]
    }
    assert pick_release_group(search, "Some Band", "Some Album") == "original"


def test_pick_original_release_is_the_earliest():
    releases = [{"id": "reissue", "date": "2004-03-01"}, {"id": "undated"}, {"id": "first", "date": "1977-02-04"}]
    assert pick_original_release(releases) == "first"


def release(release_id, date, tracks):
    return {"id": release_id, "date": date, "media": [{"track-count": tracks}]}


def test_pick_original_release_has_the_usual_track_count():
    # Rubber Soul: a 12-track Canadian LP is dated earliest, but most releases have 14 tracks
    releases = [release("ca", "1965", 12), release("uk", "1965-12-03", 14), release("cd", "1987-04-30", 14)]
    assert pick_original_release(releases) == "uk"


def test_pick_original_release_prefers_a_full_date_in_the_same_year():
    releases = [release("mx", "1969", 9), release("us", "1969-11-28", 9)]
    assert pick_original_release(releases) == "us"


# --- choosing the Spotify edition


def test_override_wins():
    data = crawled(album("a", "Some Album", ["x"]), album("b", "Some Album", ["x"]))
    assert choose_edition(LISTEN, data, [], {"spotify_album_id": "b"}) == ("b", "override", None)


def test_track_link_on_the_page_wins_when_it_is_this_album():
    data = crawled(
        album("best-of", "Greatest Hits", ["x"]),
        album("search", "Some Album", ["x"]),
        album("linked", "Some Album (Remastered)", ["x"]),
        linked=["best-of", "linked"],
    )
    assert choose_edition(LISTEN, data, [], {}) == ("linked", "track_link", None)


def test_search_prefers_the_original_track_list_among_editions_that_fit():
    deluxe = album("deluxe", "Some Album (Deluxe)", ["One", "Two", "Three", "Bonus"])
    original = album("original", "Some Album", ["One - 2004 Remaster", "Two", "Three"])
    data = crawled(deluxe, original, original_titles=["One", "Two", "Three"])
    assert choose_edition(LISTEN, data, [(None, 3)], {}) == ("original", "search", None)
    # a favorite track 4 only exists on the deluxe edition
    assert choose_edition(LISTEN, data, [(None, 4)], {}) == ("deluxe", "search", None)


def test_search_prefers_the_track_list_most_like_the_original():
    titles = ["One", "Two", "Three"]
    deluxe = album("deluxe", "Some Album (Super Deluxe)", titles + ["One - Demo", "Two - Mono", "Three - Live"] * 3)
    remaster = album("remaster", "Some Album (Remastered)", ["One - Remastered", "Two", "Three", "Four"])
    data = crawled(deluxe, remaster, original_titles=titles)
    assert choose_edition(LISTEN, data, [(None, 3)], {}) == ("remaster", "search", None)


def test_search_falls_back_to_the_nearest_track_count():
    # no titles match exactly (Spotify adds "- Live At ..."), so the track count decides
    longer = album("longer", "Some Album (Vol. 2)", [f"Song {i} - Live" for i in range(19)])
    same_length = album("same", "Some Album (Expanded)", [f"Song {i} - Live" for i in range(3)])
    data = crawled(longer, same_length, original_titles=["Song 0", "Song 1", "Song 2"])
    assert choose_edition(LISTEN, data, [], {}) == ("same", "search", None)


def test_a_bad_track_number_still_gets_the_best_edition():
    short = album("short", "Some Album", ["One", "Two"])
    longer = album("longer", "Some Album (Deluxe)", ["One", "Two", "Three"])
    data = crawled(short, longer)
    # nothing has a track 0, but only the longer edition has track 3
    assert choose_edition(LISTEN, data, [(None, 0), (None, 3)], {}) == ("longer", "search", None)


def test_override_album_name_replaces_the_page_shorthand():
    data = crawled(album("a", "Some Band IV (Remaster)", ["x"]))
    listen = {**LISTEN, "album": "IV"}
    assert match_listen(listen, data, [], {}, [])["spotify_album_id"] is None
    assert match_listen(listen, data, [], {"album": "Some Band IV"}, [])["spotify_album_id"] == "a"


def test_override_year_replaces_musicbrainz_year():
    data = crawled(album("a", "Some Album", ["x"]))
    data["release_group"] = {"id": "g", "title": "Some Album", "first-release-date": "1947-06", "artist-credit": []}
    listen = {**LISTEN, "year": 1957}
    assert match_listen(listen, data, [], {}, [])["review_reasons"] == ["MusicBrainz says 1947, the page says 1957"]
    match = match_listen(listen, data, [], {"year": "1957"}, [])
    assert (match["original_year"], match["review_reasons"]) == (1957, [])


def test_no_match_is_flagged():
    assert choose_edition(LISTEN, crawled(), [], {}) == (None, None, "no Spotify match")


def test_edition_tracks_marks_bonus_tracks():
    tracks = edition_tracks(
        album("a", "Some Album", ["One - Remastered", "Two", "Two - Demo"]),
        {"media": [{"position": 1, "tracks": [{"title": "One"}, {"title": "Two"}]}]},
    )
    assert [(t["position"], t["on_original_album"]) for t in tracks] == [(1, True), (2, True), (3, False)]


def test_edition_tracks_match_titles_written_differently():
    tracks = edition_tracks(
        album("a", "Some Album", ["To Zion (feat. Carlos Santana)", "Love Minus Zero", "Night Train - Live 1962"]),
        {
            "media": [
                {
                    "position": 1,
                    "tracks": [{"title": t} for t in ["To Zion", "Love Minus Zero/No Limit", "Night Train"]],
                }
            ]
        },
    )
    assert [t["on_original_album"] for t in tracks] == [True, True, True]


def test_edition_tracks_match_one_word_titles_and_double_suffixes():
    tracks = edition_tracks(
        album("a", "Some Album", ["Steamroller Blues - 2019 Remaster", "Scenario (feat. Busta Rhymes) - LP Mix"]),
        {"media": [{"position": 1, "tracks": [{"title": "Steamroller"}, {"title": "Scenario"}]}]},
    )
    assert [t["on_original_album"] for t in tracks] == [True, True]


def test_edition_tracks_without_an_original_are_unknown():
    assert [t["on_original_album"] for t in edition_tracks(album("a", "Some Album", ["One"]), None)] == [None]


# --- the 1001 list


def test_match_1001_survives_spelling_differences():
    albums_1001 = [
        {"list_number": "1", "artist": "Alanis Morissette", "album": "Jagged Little Pill"},
        {"list_number": "2", "artist": "Red Hot Chili Peppers", "album": "Blood Sugar Sex Magik"},
    ]
    assert match_1001([("Alanis Morrisette", "Jagged Little Pill")], albums_1001)["list_number"] == "1"
    assert match_1001([("Red Hot Chilli Peppers", "Blood Sugar Sex Magik")], albums_1001)["list_number"] == "2"
    assert match_1001([("Some Band", "Some Album")], albums_1001) is None


def test_match_1001_accepts_a_name_inside_the_other():
    albums_1001 = [
        {"list_number": "36", "artist": "Phil Spector", "album": "A Christmas Gift for You"},
        {"list_number": "39", "artist": "James Brown", "album": "Live at the Apollo"},
    ]
    names = [("Phil Spector / Various Artists", "A Christmas Gift For You From Phil Spector")]
    assert match_1001(names, albums_1001)["list_number"] == "36"
    names = [("James Brown & The Famous Flames", "The James Brown Show: Live At The Apollo")]
    assert match_1001(names, albums_1001)["list_number"] == "39"


# --- crawling


def test_crawl_follows_track_links_and_long_track_lists():
    track_page = {"disc_number": 1, "track_number": 1, "name": "t", "id": "t", "artists": [], "duration_ms": 1}
    responses = {
        (apis.SPOTIFY, apis.spotify_track("T1")): {"album": {"id": "A1"}},
        (apis.SPOTIFY, apis.spotify_search("Some Band", "Some Album")): {"albums": {"items": []}},
        (apis.SPOTIFY, apis.spotify_album("A1")): {
            "id": "A1",
            "name": "Some Album",
            "artists": [{"name": "Some Band"}],
            "tracks": {"items": [track_page] * 50, "total": 52},
        },
        (apis.SPOTIFY, apis.spotify_album_tracks("A1", 50)): {"items": [track_page] * 2},
    }
    result = crawl({**LISTEN, "spotify_track_ids": ["T1"]}, {}, lambda s, r: responses.get((s, r)))
    assert result["linked_album_ids"] == ["A1"]
    assert len(result["albums"]["A1"]["all_tracks"]) == 52
    assert result["release_group"] is None


def test_spotify_track_ids_from_page_links():
    html = (
        '<a href="https://open.spotify.com/track/5l7KTwdgBLrx2PhFHnYoQ4?si=90da">x</a>'
        '<a href="https://open.spotify.com/intl-de/track/0000000000000000000002">y</a>'
        '<a href="https://open.spotify.com/track/5l7KTwdgBLrx2PhFHnYoQ4">again</a>'
    )
    assert spotify_track_ids(html) == ["5l7KTwdgBLrx2PhFHnYoQ4", "0000000000000000000002"]
