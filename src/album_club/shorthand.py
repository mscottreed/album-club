"""Parse the shorthand in a ratings table's rating and favorite-songs cells.

The rules are in docs/data-decisions.md. Anything the rules don't cover is kept as best-effort
output with a review flag, never silently dropped.
"""

import re
from dataclasses import dataclass, field

# A rating, optionally marked (A) for the album or (P) for a playlist. Question marks are
# dropped and ".x" counts as ".0": "8?" -> 8, "?8.X?" -> 8, "3.x" -> 3.
RATING = re.compile(r"\?*\s*(\d+)(?:\.(\d+|x))?\s*\?*\s*(?:\((A|P)\))?", re.IGNORECASE)
LISTEN_MARK = {"a": "album", "p": "playlist"}


def parse_rating(text: str) -> list[tuple[float, str | None]]:
    """Return (rating, listen) pairs, where listen is "album", "playlist" or None if unmarked."""
    text = text.strip()
    if not text:
        return []
    ratings = []
    for part in text.split(","):
        m = RATING.fullmatch(part.strip())
        if not m:
            raise ValueError(f"unrecognized rating: {text!r}")
        whole, decimals, mark = m.groups()
        value = float(f"{whole}.{decimals}" if decimals and decimals.lower() != "x" else whole)
        ratings.append((value, LISTEN_MARK.get(mark.lower()) if mark else None))
    return ratings


@dataclass
class Favorites:
    """Ranked favorites as (disc, track) pairs; disc is None unless the cell labels discs."""

    ranked: list[tuple[int | None, int]] = field(default_factory=list)
    likes_rest: bool = False
    excluded: list[tuple[int | None, int]] = field(default_factory=list)
    review_reasons: list[str] = field(default_factory=list)


NO_FAVORITES = {"", "x", "none", "no standouts"}

TOKEN = re.compile(
    r"""
    (?P<disc>disc\s*(?P<disc_no>\d+)\s*:)
  | (?P<ellipsis>\.{2,})
  | (?P<range>(?P<lo>\d+)\s*-\s*(?P<hi>\d+)(?![a-z]))
  # a track: "5", "x5" (not liked), with a movement ("1ii-v"), a letter ("10b"), a star ("2*"),
  # or its name ("1-Down on the Street")
  | (?P<track>(?P<x>x\s*)?(?P<no>\d+)(?:[ivx]+(?:-[ivx]+)?\b|[a-z](?![a-z])|\*)?(?:-[a-z][^,]*)?)
  | (?P<word>[a-z]+)
  | (?P<open>\()
  | (?P<close>\))
  | (?P<sep>[,&/.:;\s])
  | (?P<other>.)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Words that are part of the shorthand. Any other word is a note the rules can't interpret.
ALL_WORDS = {"all", "others", "of", "them", "the", "rest"}
EXCEPT_WORDS = {"except", "but"}


def parse_favorites(text: str) -> Favorites:
    """Parse a favorite-songs cell. See docs/data-decisions.md for the rules."""
    fav = Favorites()
    text = text.replace("…", "...").strip()
    if text.lower() in NO_FAVORITES:
        return fav

    disc = None
    excluding = False  # after "..." or "all except", numbers are tracks not liked
    in_parens = False
    excluding_in_parens = False  # "(x5, 9, 10)": an x applies to the rest of the parentheses
    for m in TOKEN.finditer(text):
        if m.group("disc"):
            disc, excluding = int(m.group("disc_no")), False
        elif m.group("ellipsis"):
            fav.likes_rest, excluding = True, True
        elif m.group("range"):
            lo, hi = int(m.group("lo")), int(m.group("hi"))
            if lo >= hi:
                fav.review_reasons.append(f"unclear range {m.group('range')!r}")
                continue
            target = fav.excluded if excluding or excluding_in_parens else fav.ranked
            target.extend((disc, t) for t in range(lo, hi + 1))
        elif m.group("track"):
            if m.group("x") and in_parens:
                excluding_in_parens = True
            target = fav.excluded if (m.group("x") or excluding or excluding_in_parens) else fav.ranked
            target.append((disc, int(m.group("no"))))
        elif m.group("word"):
            word = m.group("word").lower()
            if word in ALL_WORDS:
                fav.likes_rest = fav.likes_rest or word == "all"
            elif word in EXCEPT_WORDS and fav.likes_rest:
                excluding = True
            else:
                fav.review_reasons.append(f"note {word!r}")
        elif m.group("open"):
            in_parens = True
        elif m.group("close"):
            in_parens = excluding_in_parens = False
        elif m.group("other"):
            fav.review_reasons.append(f"character {m.group('other')!r}")

    fav.ranked = _dedupe(fav.ranked)
    fav.excluded = _dedupe(fav.excluded)
    if set(fav.ranked) & set(fav.excluded):
        fav.review_reasons.append("track both ranked and excluded")
    fav.review_reasons = _dedupe(fav.review_reasons)
    return fav


def _dedupe(items: list) -> list:
    return list(dict.fromkeys(items))
