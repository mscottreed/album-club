import pytest

from album_club.shorthand import parse_favorites, parse_rating


@pytest.mark.parametrize(
    "text, expected",
    [
        ("", []),
        ("7", [(7.0, None)]),
        ("6.75", [(6.75, None)]),
        ("8?", [(8.0, None)]),
        ("?8.X?", [(8.0, None)]),
        ("3.x", [(3.0, None)]),
        ("4.5 (P)", [(4.5, "playlist")]),
        ("8.4 (A), 9.8 (P)", [(8.4, "album"), (9.8, "playlist")]),
    ],
)
def test_parse_rating(text, expected):
    assert parse_rating(text) == expected


def test_parse_rating_rejects_text_it_does_not_understand():
    with pytest.raises(ValueError):
        parse_rating("great")


def tracks(pairs):
    return [track for _, track in pairs]


@pytest.mark.parametrize("text", ["", "None", "none", "X", "no standouts"])
def test_no_favorites(text):
    fav = parse_favorites(text)
    assert (fav.ranked, fav.likes_rest, fav.excluded, fav.review_reasons) == ([], False, [], [])


@pytest.mark.parametrize(
    "text, ranked, likes_rest, excluded",
    [
        ("2, 3, 1", [2, 3, 1], False, []),
        ("10,1", [10, 1], False, []),
        ("9, 16, 3 10", [9, 16, 3, 10], False, []),
        ("12, 9,", [12, 9], False, []),
        # parentheses are ties and are ignored
        ("10, (2, 5, 8)", [10, 2, 5, 8], False, []),
        # "..." means every other track was liked, except the numbers after it
        ("8, 1, 2, 4, ...", [8, 1, 2, 4], True, []),
        ("1, 2, 9, 5, … 8", [1, 2, 9, 5], True, [8]),
        ("2, 3, 1, ... (x5, 9, 10)", [2, 3, 1], True, [5, 9, 10]),
        ("13, 12, 9, ... X8", [13, 12, 9], True, [8]),
        # x marks a track not liked; inside parentheses it applies to the rest of them
        ("9, x3", [9], False, [3]),
        ("7, 2, 1 (x9, 13, 14)", [7, 2, 1], False, [9, 13, 14]),
        ("8, 1, X12X", [8, 1], False, [12]),
        # written-out forms of "liked all except"
        ("all", [], True, []),
        ("6, all others", [6], True, []),
        ("all except 7, 4", [], True, [7, 4]),
        ("7, 2, 11, all but (10, 6)", [7, 2, 11], True, [10, 6]),
        ("all of them except 8", [], True, [8]),
        # ranges, pairs, and track numbers with extra marks
        ("5-8, 10", [5, 6, 7, 8, 10], False, []),
        ("6, 2&16", [6, 2, 16], False, []),
        ("3, 2/4, 9", [3, 2, 4, 9], False, []),
        ("4, 3, 5, 1ii-v", [4, 3, 5, 1], False, []),
        ("5, 10b", [5, 10], False, []),
        ("4, 2*, 9", [4, 2, 9], False, []),
        ("4-Dirt, 1", [4, 1], False, []),
    ],
)
def test_parse_favorites(text, ranked, likes_rest, excluded):
    fav = parse_favorites(text)
    assert tracks(fav.ranked) == ranked
    assert fav.likes_rest == likes_rest
    assert tracks(fav.excluded) == excluded
    assert fav.review_reasons == []


def test_disc_labels():
    fav = parse_favorites("Disc 1: 4, 7 Disc 2: 1, 3")
    assert fav.ranked == [(1, 4), (1, 7), (2, 1), (2, 3)]


@pytest.mark.parametrize(
    "text, reason",
    [
        ("4, 10, 9 (lyrics)", "note 'lyrics'"),
        ("? 2, 11 ?", "character '?'"),
        ("4, 16, 1 ... 16", "track both ranked and excluded"),
    ],
)
def test_flags_what_the_rules_do_not_cover_but_keeps_best_effort(text, reason):
    fav = parse_favorites(text)
    assert reason in fav.review_reasons
    assert fav.ranked  # the numbers are still read
